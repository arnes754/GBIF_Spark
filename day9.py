"""Day 9 - the end-to-end run, timed stage by stage.

Days 5-8 each answered one question in isolation. This is the job: read the
curated table, enrich it with the registry dimension, compute every aggregate
the analysis needs, write the results, read them back to check them.

The rule is full data in, small aggregates out. The
inputs are gigabytes, the outputs are kilobytes, and nothing downstream ever
reads the fact table again.

    uv run python day9.py --batches b0004 --tag run
    uv run python day9.py --batches b0004 --cache memory_and_disk \
        --no-write-in-place --no-cache-results --tag as-day-9-shipped
    uv run python day9.py --report          # compare past runs

Every run appends a row to data/reports/runs.jsonl with per-stage wall time,
bytes read and bytes shuffled, so "did that change help" is a lookup rather
than a memory.

WEEK 3 NOTE: the nine stages moved into `job.py` so days 10-14 can run them
with one knob moved. This file is now the CLI and the run log; the pipeline
itself is `job.run()`. Nothing about what it computes changed.
"""
import argparse
import datetime
import json
import os
import pathlib
import platform
import time

import job
from bench import banner, gb

HERE = pathlib.Path(__file__).parent
REPORTS = HERE / "data" / "reports"
RUNS = REPORTS / "runs.jsonl"


def parse_batches(s):
    """--batches b0000,b0003 -> ("b0000", "b0003"); empty means the whole table."""
    return tuple(b.strip() for b in s.split(",") if b.strip()) if s else ()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--table", default=job.DEFAULT_TABLE)
    p.add_argument("--dim", default=job.DEFAULT_DIM)
    p.add_argument("--out", default=str(HERE / "data" / "results"))
    p.add_argument("--batches", default="",
                   help="comma-separated ingest batches, e.g. b0000,b0003. "
                        "Empty = the whole 90 GB table.")
    p.add_argument("--tag", default="default", help="label this run in runs.jsonl")
    p.add_argument("--shuffle-partitions", type=int, default=48)
    p.add_argument("--max-partition-bytes", type=int,
                   default=job.DEFAULT_MAX_PARTITION_BYTES,
                   help="spark.sql.files.maxPartitionBytes (day 11)")
    p.add_argument("--cache", default="none", choices=job.CACHE_MODES,
                   help="how to persist the enriched table. Default changed "
                        "from memory_and_disk to none - see day13.py")
    p.add_argument("--join", default="auto", choices=job.JOIN_MODES,
                   help="let Spark choose, force broadcast, or forbid it (day 12)")
    p.add_argument("--no-aqe", action="store_true", help="turn AQE off (day 12)")
    p.add_argument("--no-skew-join", action="store_true",
                   help="turn AQE's skew-join split off (day 12)")
    # Both of these are ON by default - they are week 3's conclusions. The
    # flags turn them OFF, so day14.py can reconstruct day 9's behaviour
    # exactly without anyone having to remember what it used to be.
    p.add_argument("--no-write-in-place", action="store_true",
                   help="go back to one write stage at the end, which computes "
                        "every aggregate a second time (day 10)")
    p.add_argument("--no-cache-results", action="store_true",
                   help="do not persist the small per-dataset aggregate that "
                        "three stages read (day 13)")
    p.add_argument("--no-coalesce-output", action="store_true",
                   help="write result tables without coalesce(1) (day 11)")
    p.add_argument("--driver-memory", default="4g")
    p.add_argument("--no-cache", action="store_true",
                   help="shorthand for --cache none")
    p.add_argument("--report", action="store_true", help="print past runs and exit")
    args = p.parse_args()

    if args.report:
        return print_report()

    cfg = job.Config(
        table=args.table, dim=args.dim, out=args.out,
        batches=parse_batches(args.batches),
        shuffle_partitions=args.shuffle_partitions,
        max_partition_bytes=args.max_partition_bytes,
        cache="none" if args.no_cache else args.cache,
        join=args.join,
        aqe=not args.no_aqe,
        aqe_skew_join=not args.no_skew_join,
        write_in_place=not args.no_write_in_place,
        cache_results=not args.no_cache_results,
        coalesce_output=not args.no_coalesce_output,
        driver_memory=args.driver_memory,
        tag=args.tag,
    )

    t_total = time.perf_counter()
    started = datetime.datetime.now()
    in_bytes = cfg.input_bytes_on_disk()

    banner(f"end-to-end run  [{cfg.tag}]  {started:%Y-%m-%d %H:%M}")
    print(f"  config    : {cfg.describe()}")
    print(f"  input     : {gb(in_bytes)} on disk")
    print(f"  out       : {cfg.out}")
    print(f"  host      : {platform.machine()} / "
          f"{os.cpu_count()} cores, driver {cfg.driver_memory}")

    spark = job.session_for(cfg, app=f"gbif-e2e-{cfg.tag}")
    st, _results, summary = job.run(spark, cfg)

    wall = time.perf_counter() - t_total
    rows = st.report(wall)
    tot = job.totals(rows)
    print(f"\n  total wall time            {wall:.1f}s")
    print(f"  sum of stages              {tot['seconds']:.1f}s"
          f"   (the gap is session startup and plan construction)")
    print(f"  total bytes read           {gb(tot['input_bytes'])}")
    print(f"  total bytes shuffled       {gb(tot['shuffle_write_bytes'])}")
    print(f"  total executor task time   {tot['task_ms'] / 1000:.0f}s"
          f"   ({tot['task_ms'] / 1000 / max(wall, 1):.1f}x wall"
          f" = effective parallelism)")

    job.print_answer(summary)

    record = {
        "tag": cfg.tag,
        "at": started.isoformat(timespec="seconds"),
        "table": cfg.table,
        "batches": list(cfg.batches),
        "input_bytes_on_disk": in_bytes,
        "fact_rows": summary["n_facts"],
        "dim_rows": summary["n_dim"],
        "shuffle_partitions": cfg.shuffle_partitions,
        "max_partition_bytes": cfg.max_partition_bytes,
        "cache": cfg.cache,
        "join": cfg.join,
        "join_strategy": summary["join_strategy"],
        "aqe": cfg.aqe,
        "aqe_skew_join": cfg.aqe_skew_join,
        "coalesce_output": cfg.coalesce_output,
        "write_in_place": cfg.write_in_place,
        "cache_results": cfg.cache_results,
        "driver_memory": cfg.driver_memory,
        "wall_seconds": round(wall, 1),
        "verified": summary["verified"],
        "stages": [{k: m.get(k, 0) for k in
                    ("label", "seconds", "input_bytes", "files_read",
                     "scan_rows", "shuffle_write_bytes", "shuffle_read_bytes",
                     "disk_spill_bytes", "tasks", "task_ms")} for m in rows],
        "result_bytes": summary["result_bytes"],
        "headline": summary["headline"],
        "variance": summary["variance"],
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    with RUNS.open("a") as f:
        f.write(json.dumps(record, default=float) + "\n")
    print(f"\nappended to {RUNS}   (uv run python day9.py --report)")

    spark.stop()
    print("\ndone.")


def load_runs():
    if not RUNS.exists():
        return []
    return [json.loads(l) for l in RUNS.open() if l.strip()]


def print_report():
    runs = load_runs()
    if not runs:
        return print("no runs yet - try: uv run python day9.py --batches b0000,b0003")
    banner(f"{len(runs)} runs")
    print(f"  {'tag':<16}{'when':<18}{'rows':>15}{'shuf':>6}{'cache':>16}"
          f"{'wall s':>9}{'read':>11}{'shuffled':>11}  ok")
    for r in runs:
        rd = sum(s["input_bytes"] for s in r["stages"])
        sh = sum(s["shuffle_write_bytes"] for s in r["stages"])
        print(f"  {r['tag'][:15]:<16}{r['at'][:16]:<18}{r['fact_rows']:>15,}"
              f"{r['shuffle_partitions']:>6}{str(r.get('cache')):>16}"
              f"{r['wall_seconds']:>9.1f}{gb(rd):>11}{gb(sh):>11}"
              f"  {'y' if r.get('verified') else 'n'}")
    banner("stage breakdown, most recent run")
    last = runs[-1]
    w = last["wall_seconds"]
    for s in last["stages"]:
        bar = "#" * int(round(40 * s["seconds"] / max(w, 1e-9)))
        print(f"  {s['label']:<30}{s['seconds']:>8.1f}s "
              f"{100 * s['seconds'] / w:>5.0f}%  {bar}")


if __name__ == "__main__":
    main()
