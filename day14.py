"""Run the job under several configurations and compare them.

Each configuration is a fresh day9.py subprocess rather than a loop inside one
session: a session keeps cached blocks, a warm JIT and conf values from the
previous experiment, so a loop measures the order things ran in. The OS page
cache is still shared, so --warm reads each slice once before the matrix
starts and every configuration sees the same warm cache.

The configurations are defined in CONFIGS below as day9.py flags.

    uv run python day14.py
    uv run python day14.py --slices b0003,b0000,b0004 --warm
    uv run python day14.py --report
"""
import argparse
import json
import os
import pathlib
import signal
import subprocess
import sys
import time

import bench
import curate
import job
from bench import banner, gb

HERE = pathlib.Path(__file__).parent
RUNS = HERE / "data" / "reports" / "runs.jsonl"
OUT = HERE / "data" / "scratch" / "day14"

# Each value is a list of day9.py flags. Three configurations rather than two
# so the middle one isolates the cache change from the rest.
CONFIGS = {
    # The original: cache the fact table, and compute every aggregate once to
    # time it and again to write it.
    "before":  ["--cache", "memory_and_disk",
                "--no-write-in-place", "--no-cache-results"],
    # Drop the cache, change nothing else.
    "nocache": ["--cache", "none",
                "--no-write-in-place", "--no-cache-results"],
    # No fact-table cache, each aggregate written by the stage that built it,
    # and the small intermediate persisted. These are job.py's defaults.
    "after":   [],
}


def run_once(slice_name, config_name, timeout_s):
    """One day9.py run. Returns a dict even on failure: a configuration that
    cannot finish is a result, and dropping it would leave a table comparing
    only the runs that survived.

    The subprocess gets its own process group, killed afterwards whether the
    run succeeded or not: when day9.py dies of an OutOfMemoryError the python
    process exits but the JVM it started does not, and it keeps its heap and
    the UI port. The next run then starts short of memory and fails during
    startup for an unrelated-looking reason.
    """
    tag = f"{config_name}-{slice_name}"
    cmd = [sys.executable, "-u", str(HERE / "day9.py"),
           "--batches", slice_name, "--tag", tag,
           "--out", str(OUT / tag)] + CONFIGS[config_name]
    before = RUNS.stat().st_size if RUNS.exists() else 0
    t0 = time.perf_counter()
    proc = subprocess.Popen(cmd, cwd=HERE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True,
                            start_new_session=True)
    try:
        _out, err = proc.communicate(timeout=timeout_s)
        rc = proc.returncode
    except subprocess.TimeoutExpired:
        rc, err = -1, f"timed out after {timeout_s}s"
    finally:
        reap(proc.pid)
    wall = time.perf_counter() - t0

    row = {"slice": slice_name, "config": config_name, "wall_s": wall,
           "ok": rc == 0, "note": ""}
    if rc == 0 and RUNS.exists() and RUNS.stat().st_size > before:
        with RUNS.open() as f:
            rec = json.loads(f.readlines()[-1])
        row.update(
            rows=rec["fact_rows"], job_s=rec["wall_seconds"],
            read=sum(s["input_bytes"] for s in rec["stages"]),
            shuffle=sum(s["shuffle_write_bytes"] for s in rec["stages"]),
            spill=sum(s["disk_spill_bytes"] for s in rec["stages"]),
            task_s=sum(s["task_ms"] for s in rec["stages"]) / 1000,
            verified=rec["verified"], stages=rec["stages"],
            usable=rec["headline"]["usable"],
        )
    else:
        row["note"] = classify_failure(err)
    return row


def reap(pid):
    """Kill the process group and wait for the JVM to actually be gone.

    SIGKILL, not SIGTERM: a JVM out of heap may never reach its shutdown
    hooks.
    """
    try:
        os.killpg(os.getpgid(pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    # Let the OS release the memory and the UI port before the next run.
    for _ in range(40):
        if not java_running():
            return
        time.sleep(0.5)


def java_running():
    out = subprocess.run(["pgrep", "-f", "org.apache.spark.deploy"],
                         capture_output=True, text=True)
    return bool(out.stdout.strip())


def classify_failure(stderr):
    """Short reason for a failure, from the stderr already captured."""
    if "OutOfMemoryError" in stderr:
        return "OOM (java heap)"
    if "SparkEnv" in stderr and "NullPointerException" in stderr:
        # A previous run's JVM was still holding memory and the UI port.
        # reap() should prevent this.
        return "startup clash - a previous JVM outlived its run"
    if "timed out" in stderr:
        return stderr
    if "SparkOutOfMemoryError" in stderr:
        return "OOM (spark memory)"
    for line in reversed(stderr.strip().splitlines()):
        if line.strip() and not line.startswith((" ", "\t")):
            return line.strip()[:60]
    return "failed"


def print_matrix(rows):
    banner("before and after")
    print(f"  {'slice':<9}{'config':<9}{'rows':>14}{'job s':>9}{'read':>11}"
          f"{'shuffle':>10}{'spill':>10}{'task s':>9}  result")
    for r in rows:
        if r["ok"]:
            print(f"  {r['slice']:<9}{r['config']:<9}{r['rows']:>14,}"
                  f"{r['job_s']:>9.1f}{gb(r['read']):>11}{gb(r['shuffle']):>10}"
                  f"{gb(r['spill']):>10}{r['task_s']:>9.0f}"
                  f"  {'verified' if r['verified'] else 'WRONG ROW COUNT'}")
        else:
            print(f"  {r['slice']:<9}{r['config']:<9}{'-':>14}"
                  f"{r['wall_s']:>9.1f}{'-':>11}{'-':>10}{'-':>10}{'-':>9}"
                  f"  FAILED: {r['note']}")

    banner("the delta, per slice")
    by_slice = {}
    for r in rows:
        by_slice.setdefault(r["slice"], {})[r["config"]] = r
    for sl, pair in by_slice.items():
        b, a = pair.get("before"), pair.get("after")
        if not (b and a):
            continue
        print(f"\n  {sl}  ({gb(curate.table_bytes(batches=[sl]))} on disk)")
        mid = pair.get("nocache")
        if mid and mid["ok"] and b["ok"]:
            print(f"    (removing the cache alone: {b['job_s']:.1f}s -> "
                  f"{mid['job_s']:.1f}s, {gb(b['read'])} -> {gb(mid['read'])} read)")
        if not b["ok"] and a["ok"]:
            print(f"    before : FAILED - {b['note']}")
            print(f"    after  : {a['job_s']:.1f}s, verified")
            print("    -> not a speedup: the job went from not completing "
                  "to completing.")
            continue
        if not (b["ok"] and a["ok"]):
            print(f"    both configurations did not complete - nothing to compare")
            continue
        for name, key, fmt in [("wall time", "job_s", lambda v: f"{v:.1f}s"),
                               ("bytes read", "read", gb),
                               ("bytes shuffled", "shuffle", gb),
                               ("bytes spilled", "spill", gb),
                               ("executor task time", "task_s",
                                lambda v: f"{v:.0f}s")]:
            bv, av = b[key], a[key]
            delta = (av - bv) / bv * 100 if bv else 0
            arrow = "faster" if key == "job_s" and av < bv else ""
            print(f"    {name:<20}{fmt(bv):>12} -> {fmt(av):>12}"
                  f"   {delta:+6.0f}%  {arrow}")
        same = abs(b["usable"] - a["usable"]) < 1e-9
        print(f"    {'answer unchanged':<20}{'usable_for_mapping ':>12}"
              f"{100 * a['usable']:>7.2f}%   {'yes' if same else 'NO - BUG'}")


def print_stage_delta(rows):
    """Which stage the difference came from."""
    pairs = {}
    for r in rows:
        if r["ok"]:
            pairs.setdefault(r["slice"], {})[r["config"]] = r
    for sl, pair in pairs.items():
        # Baseline is whichever earlier configuration completed. Where
        # `before` OOMs there is nothing to subtract, so `nocache` is used.
        base_name = next((c for c in CONFIGS if c in pair and c != "after"), None)
        if base_name is None or "after" not in pair:
            continue
        banner(f"stage by stage, {sl}   ({base_name} -> after)")
        b = {s["label"]: s for s in pair[base_name]["stages"]}
        a = {s["label"]: s for s in pair["after"]["stages"]}
        print(f"  {'stage':<30}{base_name:>10}{'after':>10}{'delta':>10}")
        for label in a:
            bs = b.get(label, {}).get("seconds", 0)
            as_ = a[label]["seconds"]
            print(f"  {label:<30}{bs:>9.1f}s{as_:>9.1f}s{as_ - bs:>+9.1f}s")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--slices", default="b0003,b0000",
                   help="ingest batches to run the matrix over, smallest first")
    p.add_argument("--timeout", type=int, default=3600,
                   help="seconds before a single run is called a failure")
    p.add_argument("--warm", action="store_true",
                   help="read each slice once first, so no config pays for a "
                        "cold page cache")
    p.add_argument("--report", action="store_true",
                   help="re-print from a previous day14 result file")
    args = p.parse_args()

    result_file = HERE / "data" / "reports" / "day14_before_after.json"
    if args.report:
        if not result_file.exists():
            return print("no day14 results yet")
        rows = json.loads(result_file.read_text())
        print_matrix(rows)
        return print_stage_delta(rows)

    slices = [s for s in args.slices.split(",") if s]
    banner("day 14 - before and after")
    print(f"  slices  : {', '.join(slices)}")
    for s in slices:
        print(f"            {s}  {gb(curate.table_bytes(batches=[s]))}")
    print(f"  configs : ")
    for name, flags in CONFIGS.items():
        print(f"            {name:<8} day9.py {' '.join(flags)}")
    print(f"  each run is a fresh JVM; {len(slices) * len(CONFIGS)} runs total")

    if args.warm:
        print("\n  warming the page cache...")
        for s in slices:
            for f in (HERE / "data" / "curated" / "occurrence_slim"
                      / f"ingest_batch={s}").rglob("*.parquet"):
                f.read_bytes()
        print("  warm.")

    rows = []
    for sl in slices:
        for cfg_name in CONFIGS:
            print(f"\n  running {cfg_name} on {sl} ...", end="", flush=True)
            r = run_once(sl, cfg_name, args.timeout)
            rows.append(r)
            print(f" {'ok' if r['ok'] else 'FAILED'} in {r['wall_s']:.0f}s"
                  f"{'' if r['ok'] else '  (' + r['note'] + ')'}")

    result_file.parent.mkdir(parents=True, exist_ok=True)
    result_file.write_text(json.dumps(rows, indent=2, default=float))
    print_matrix(rows)
    print_stage_delta(rows)

    banner("what worked, what did not")
    print("""  WORKED

  Removing the fact-table cache. It did not move a percentage, it moved the
  job from OOMing above 1 GB to completing. The cached thing was a parquet
  scan plus a broadcast join: cheap to recompute, expensive to store, and
  impossible to column-prune once cached.

  Writing each aggregate in the stage that built it. Every result DataFrame
  used to be forced with count() to time it and then recomputed by the write
  stage: 22 passes over the fact table for 11 outputs, now 13.

  DID NOT WORK

  Shuffle partitions. The shuffles are kilobytes because every aggregate
  reduces on the map side, so 48 vs 800 is 48 vs 800 near-empty tasks and
  AQE coalesces them anyway.

  Input partitioning. The default 128 MB already packs the slice into about
  one task per core. 32 MB is worse.

  Partition pruning. The job filters on nothing, so the decade layout does
  not help it. It stays for the ad-hoc queries that do filter on decade.

  Broadcast joins. Already happening: the dimension is 0.2 MB of parquet and
  Spark has a real size statistic for it. F.broadcast() changed nothing and
  would be a hint to maintain.

  Skew handling. datasetkey is heavily skewed, but the join is a broadcast,
  so there is no shuffle and no skew to handle. AQE skew join stays on
  because it costs nothing when idle.""")

if __name__ == "__main__":
    main()
