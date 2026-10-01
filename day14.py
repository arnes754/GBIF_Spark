"""Week 3, day 5 (day 14) - before and after.

Four days of measurements are worth nothing until the same job is run both
ways, on the same data, and the difference is written down. This script does
that and nothing else.

HOW IT RUNS THEM. Each configuration is a fresh `day9.py` subprocess, not a
loop inside one session. That is deliberate and it is the bit that is easy to
get wrong: a Spark session accumulates state - cached blocks, a warm JIT,
conf values set by an earlier experiment, a page cache full of the files the
last run touched. Comparing two configs inside one session measures the order
you ran them in as much as the configs. A new JVM per run is slower and it is
the only version of the number that is worth printing.

The page cache is still shared across processes, so the FIRST run of a slice
pays for cold files and the rest do not. `--warm` reads the slice once before
the matrix starts, so every configuration gets the same warm cache.

WHAT IS BEING COMPARED.

  before   day 9's defaults: persist the enriched fact table in
           MEMORY_AND_DISK, 48 shuffle partitions, AQE on.
  nocache  the same job with the cache removed and nothing else changed.
  after    also stops computing every aggregate twice (day 10's finding) and
           persists the one small, expensive intermediate instead (day 13's).

Three configurations and not two, because "we changed five things and it got
faster" is not a measurement. Each row isolates one change from the one above
it, so the table attributes the difference instead of just showing it.

Two slices, because the interesting part of this comparison is not a
percentage - it is that the configurations do not even fail in the same place.

    uv run python day14.py                       # the matrix, then the table
    uv run python day14.py --report              # just re-print from runs.jsonl
    uv run python day14.py --slices b0003,b0000,b0004
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

# The two configurations the week is about. Each value is a list of day9.py
# flags, so this table is the complete and only definition of "before" and
# "after" - no setting is applied anywhere else.
CONFIGS = {
    # day 9 as shipped: cache the enriched fact table, compute every aggregate
    # once to time it and once again to write it.
    "before":  ["--cache", "memory_and_disk",
                "--no-write-in-place", "--no-cache-results"],
    # day 13 only: drop the cache, change nothing else. This isolates the
    # change from the next one, which is the whole reason there are three
    # configurations and not two.
    "nocache": ["--cache", "none",
                "--no-write-in-place", "--no-cache-results"],
    # day 13 + day 10: no fact-table cache, each aggregate written by the stage
    # that built it, and the one small expensive intermediate persisted.
    "after":   [],          # job.py's defaults ARE week 3's conclusions
}


def run_once(slice_name, config_name, timeout_s):
    """One day9.py run. Returns a dict even when it fails - a configuration
    that cannot finish is a result, and silently dropping it would turn this
    table into a comparison of the runs that happened to survive.

    The subprocess gets its own process GROUP, and the group is killed
    afterwards whether the run succeeded or not. This is not tidiness. When
    day9.py dies of an OutOfMemoryError the python process exits but the JVM it
    started does not - it is a child of a dead parent, it keeps its ~4 GB and
    it keeps port 4040. The next run then starts on a machine with half the
    memory gone, binds its UI to 4041, and fails in six seconds with an
    unrelated NullPointerException. Two of the three configurations in this
    matrix exist to OOM on purpose, so this is the normal case, not an edge.
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
    """Kill the whole process group and wait for the machine to be quiet again.

    SIGKILL rather than SIGTERM: a JVM whose heap is exhausted may never get
    far enough through its shutdown hooks to notice a polite signal.
    """
    try:
        os.killpg(os.getpgid(pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        pass
    # Give the OS a moment to release the memory and the UI port before the
    # next run starts measuring.
    for _ in range(40):
        if not java_running():
            return
        time.sleep(0.5)


def java_running():
    out = subprocess.run(["pgrep", "-f", "org.apache.spark.deploy"],
                         capture_output=True, text=True)
    return bool(out.stdout.strip())


def classify_failure(stderr):
    """Say WHY it failed in three words, from the stderr we already have.
    'it crashed' is not a measurement; 'OutOfMemoryError in the cache stage'
    is the finding."""
    if "OutOfMemoryError" in stderr:
        return "OOM (java heap)"
    if "SparkEnv" in stderr and "NullPointerException" in stderr:
        # Not this run's fault: a previous run's JVM was still holding memory
        # and the UI port. `reap()` exists to stop this happening; if it shows
        # up anyway, the matrix is not measuring what it says it is.
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
            print(f"    -> not a speedup. The job went from not running to "
                  f"running, which is\n       the only kind of improvement "
                  f"that cannot be argued with.")
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
    """Which stage the difference came from. A total that moved without a
    stage moving means something was measured wrong."""
    pairs = {}
    for r in rows:
        if r["ok"]:
            pairs.setdefault(r["slice"], {})[r["config"]] = r
    for sl, pair in pairs.items():
        # The baseline is whichever of the earlier configurations actually
        # completed. On the slices where `before` OOMs there is no before to
        # subtract, and `nocache` becomes the thing `after` is measured
        # against - which is the honest comparison anyway.
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

  Removing the cache (day 13). The only change this week that moved anything,
  and it did not move a percentage - it moved the job from "OOMs above 1 GB"
  to "completes". The cached thing was a parquet scan plus a broadcast join:
  cheap to recompute, expensive to store, and impossible to column-prune once
  cached.

  DID NOT WORK - and these are the more useful four days

  Shuffle partitions (day 11). The job's shuffles are kilobytes. Every
  aggregate reduces hard on the map side, so there is almost nothing crossing
  the exchange, and 48 vs 200 vs 800 is 48 vs 200 vs 800 near-empty tasks.
  AQE coalesces them anyway. No change made.

  Input partitioning (day 11). The curated table's files are already close to
  maxPartitionBytes, so the default 128 MB already produces roughly one task
  per file and saturates twelve cores. No change made.

  Partition pruning (day 11). The table is partitioned by decade and the job
  filters on nothing, so the layout does not help it at all. Kept anyway,
  because day 15's analysis and the ad-hoc queries in days 5-8 do filter on
  decade - but it is a cost this job carries for someone else's benefit, and
  that is worth being explicit about rather than claiming it as a win.

  Broadcast joins (day 12). Already happening. The dimension is 0.2 MB of
  parquet, Spark has a real size statistic for it, and it broadcasts without
  being asked. Adding F.broadcast() changed nothing measurable and would have
  been a hint to maintain forever.

  Skew handling (day 12). datasetkey is genuinely skewed - the top ten keys
  hold a large share of the rows - but the join is a broadcast, so there is no
  shuffle, so there is no skew to handle. AQE's skew join stays on because it
  is free when idle, not because it is doing anything today.

  THE HONEST SUMMARY

  Four of the five optimisation days ended in "no change, and here is the
  measurement that says why". That is what measuring first buys you: four
  changes not made, each one a thing that would have been code to maintain,
  a number in a config file nobody could justify, and a plausible-sounding
  slide with nothing behind it.""")


if __name__ == "__main__":
    main()
