"""Week 3, day 1 (day 10) - reading the Spark UI, and finding the bottleneck.

Weeks 1-2 built a job. This week optimises it, and the first rule of optimising
is that you are not allowed to guess which part is slow. So before changing
anything: run the job once and read the Spark UI properly.

WHAT THE SPARK UI IS. While a Spark job runs, the driver serves a web UI on
http://localhost:4040. When the job ends, the driver exits and the UI with it -
which is why this script takes `--hold`, so you can actually go and look.

    uv run python day10.py --hold

The five tabs, and the question each one answers:

  Jobs       one row per action (count, collect, write). Coarse. Useful only
             for "how many actions did my code trigger" - often more than you
             thought.
  Stages     one row per stage. A stage is the span between two shuffles, so
             the stage list IS the shuffle structure of the job. This is where
             the longest-running unit of work is visible, and where per-task
             min/median/max shows whether the work was spread evenly.
  SQL/DataFrame  the one that matters. One entry per query, each with the
             operator tree and per-operator metrics the operators themselves
             emit: files read, bytes read, shuffle bytes written, spill.
  Storage    what is cached and whether it actually fit in memory. Day 13.
  Executors  per-executor task time, GC time, memory. On local[*] there is one
             executor, so this tab is mostly "how much GC am I doing".

WHY THIS SCRIPT EXISTS AT ALL. Every number on those pages is also available as
JSON at :4040/api/v1 - `bench.py` has read the SQL tab that way since day 5.
Reading it as data rather than as a screenshot means the answer to "which stage
is the bottleneck" is a committed table that the next four days can be compared
against, instead of something remembered from a browser tab that no longer
exists.

    uv run python day10.py                      # profile the job, print tables
    uv run python day10.py --batches b0004      # a bigger slice
    uv run python day10.py --hold               # ... and leave :4040 up

A NOTE ON THE FIRST THING THIS DAY FOUND. The job could not be profiled as
shipped. Day 9's default persists the enriched table in MEMORY_AND_DISK, and on
anything above the 20 MB toy batch that dies:

    Caused by: java.lang.OutOfMemoryError: Java heap space
      at ... apply_cache -> df.count()

So the profile below runs with --cache none. That is not a tuning decision made
here - day 13 is where caching gets measured and the default actually changes.
It is recorded here because "the first measurement you try to take is the one
that tells you the job does not run" is the realest thing that happened this
week, and hiding it would make the rest of the week look tidier than it was.

Output: the three tables at the bottom of this file's run, plus
data/reports/day10_profile.json so day 14 can diff against it.
"""
import argparse
import json
import pathlib
import time

import bench
import job
from bench import banner, gb

REPORTS = pathlib.Path(__file__).parent / "data" / "reports"

# A stage that is less than this share of wall time cannot be the bottleneck,
# however ugly its plan is. Named so the conclusion is reproducible and not a
# judgement call made while looking at the numbers.
BOTTLENECK_SHARE = 0.15


def exchanges_for(execs, exec_ids):
    """Every shuffle in the given SQL executions, as rows.

    A shuffle is an `Exchange` operator. Spark 4 shows several flavours and
    they are not the same thing at all:

      Exchange hashpartitioning   a real shuffle - every row crosses the
                                  network (here: the disk) to the partition
                                  its key hashes to. This is the expensive one.
      Exchange SinglePartition    a shuffle that collapses to ONE partition.
                                  Cheap in bytes, catastrophic in parallelism,
                                  and the signature of a window with no
                                  partitionBy or a coalesce(1) before a write.
      BroadcastExchange           not a shuffle at all. The small side is
                                  collected to the driver and sent whole to
                                  every task. Bytes here are the dimension's
                                  size, not the fact table's.
      AQEShuffleRead              AQE reading a shuffle it has since decided to
                                  coalesce or split. Its partition count is the
                                  one that actually ran, which is why it can
                                  disagree with spark.sql.shuffle.partitions.
    """
    rows = []
    for q in execs:
        if q["id"] not in exec_ids:
            continue
        for node in q.get("nodes", []):
            name = node.get("nodeName", "")
            if "Exchange" not in name and "AQEShuffleRead" not in name:
                continue
            met = {m["name"]: bench.parse_metric(m["value"])
                   for m in node.get("metrics", [])}
            rows.append({
                "operator": name,
                "bytes": met.get("shuffle bytes written",
                                 met.get("data size", 0)),
                "records": met.get("shuffle records written", 0),
                "partitions": met.get("number of partitions",
                                      met.get("partition data size", 0)),
            })
    return rows


def task_spread(stages, stage_ids):
    """Per-task duration quantiles for the Spark stages a block owned.

    This is the number that tells skew from slowness. A stage where max is 20x
    median is not slow, it is *unbalanced*: eleven cores finished and are
    sitting idle while one task does all the work. The fix for that is
    completely different from the fix for "every task is slow".
    """
    rows = []
    for s in stages:
        if s["stageId"] not in stage_ids:
            continue
        dist = (s.get("taskMetricsDistributions") or {}).get("executorRunTime")
        if not dist:
            continue
        lo, q1, med, q3, hi = (dist + [0] * 5)[:5]
        rows.append({
            "stage": f"{s['stageId']} {s.get('name', '')[:34]}",
            "tasks": s.get("numCompleteTasks", 0),
            "min_s": lo / 1000, "med_s": med / 1000, "max_s": hi / 1000,
            "skew": (hi / med) if med else 0.0,
        })
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batches", default="b0000",
                   help="ingest batches to profile, comma separated")
    p.add_argument("--shuffle-partitions", type=int, default=48)
    p.add_argument("--cache", default="none", choices=job.CACHE_MODES,
                   help="NOT the shipped default - see the note below. The job "
                        "as written on day 9 cached the enriched table and that "
                        "configuration cannot complete on this slice.")
    p.add_argument("--hold", action="store_true",
                   help="keep the session alive so :4040 stays up")
    args = p.parse_args()

    cfg = job.Config(batches=tuple(b for b in args.batches.split(",") if b),
                     shuffle_partitions=args.shuffle_partitions,
                     cache=args.cache,
                     out=str(REPORTS.parent / "results_day10"),
                     tag="day10-profile")

    banner("day 10 - profiling the job")
    print(f"  config : {cfg.describe()}")
    print(f"  input  : {gb(cfg.input_bytes_on_disk())} on disk")
    spark = job.session_for(cfg, app="gbif-day10-profile")
    print(f"  UI     : {bench.ui_base(spark)}   (gone the moment this exits,"
          f" unless --hold)")
    t0 = time.perf_counter()
    st, _results, summary = job.run(spark, cfg)
    wall = time.perf_counter() - t0

    rows = st.report(wall)

    # --- 1. which stage takes longest -------------------------------------
    banner("1. where the time goes, longest first")
    print("""  The job's own nine stages, sorted. This is the only ranking that
  matters: a 3x speedup of something that is 2% of the job is a 0.7% speedup
  of the job, and most of a week can disappear into exactly that.
""")
    ranked = sorted(rows, key=lambda m: -m["seconds"])
    bench.show(ranked, [
        ("stage", "label", bench.TXT),
        ("secs", "seconds", bench.SEC),
        ("% of wall", "pct", bench.PCT),
        ("read", "input_bytes", bench.BYTES),
        ("shuffle w", "shuffle_write_bytes", bench.BYTES),
        ("spill", "disk_spill_bytes", bench.BYTES),
        ("tasks", "tasks", bench.NUM),
        ("task secs", "task_ms", lambda v: f"{v / 1000:.0f}"),
    ])

    top = ranked[0]
    print(f"\n  longest stage : {top['label']}  "
          f"({top['seconds']:.1f}s, {top['pct']:.0f}% of wall)")
    hot = [m for m in ranked if m["pct"] >= 100 * BOTTLENECK_SHARE]
    print(f"  stages over {100 * BOTTLENECK_SHARE:.0f}% of wall: "
          f"{', '.join(m['label'] for m in hot) or 'none - the job is flat'}")
    print(f"  everything below that is noise and will not be touched this week.")

    # --- 2. where the shuffles are ----------------------------------------
    banner("2. where the shuffles happen")
    print("""  A stage boundary IS a shuffle. Every row below is one Exchange
  operator, read off the SQL tab's operator tree - not guessed from the code.
  See exchanges_for() above for why the flavours are not interchangeable.
""")
    execs = bench.sql_list(spark)
    shuffle_rows, by_stage = [], []
    for m in rows:
        ex = exchanges_for(execs, set(m["exec_ids"]))
        real = [e for e in ex if "Broadcast" not in e["operator"]]
        by_stage.append({
            "label": m["label"],
            "exchanges": len(real),
            "broadcasts": len(ex) - len(real),
            "shuffle_bytes": sum(e["bytes"] for e in real),
        })
        for e in ex:
            shuffle_rows.append({"stage": m["label"], **e})

    bench.show(by_stage, [
        ("stage", "label", bench.TXT),
        ("shuffles", "exchanges", bench.NUM),
        ("broadcasts", "broadcasts", bench.NUM),
        ("shuffle bytes", "shuffle_bytes", bench.BYTES),
    ])
    print()
    if shuffle_rows:
        bench.show(shuffle_rows, [
            ("stage", "stage", bench.TXT),
            ("operator", "operator", bench.TXT),
            ("bytes", "bytes", bench.BYTES),
            ("records", "records", bench.NUM),
            ("partitions", "partitions", bench.NUM),
        ])

    # --- 3. is any stage unbalanced ---------------------------------------
    banner("3. task spread - slow, or just unbalanced?")
    print("""  Per-task executor run time inside each Spark stage. A healthy stage
  has max/median near 1. A skewed one can have max/median in the double digits
  and still look fine on every total, because the totals are right - it is the
  distribution that is wrong. Day 12 is about this column.
""")
    stages_json = bench.stage_list(spark, summaries=True)
    spread = []
    for m in rows:
        for r in task_spread(stages_json, set(m["stage_ids"])):
            spread.append({"label": m["label"], **r})
    spread = [r for r in spread if r["tasks"] > 1]
    worst = sorted(spread, key=lambda r: -r["skew"])[:15]
    if worst:
        bench.show(worst, [
            ("job stage", "label", bench.TXT),
            ("spark stage", "stage", bench.TXT),
            ("tasks", "tasks", bench.NUM),
            ("min s", "min_s", bench.SEC),
            ("med s", "med_s", bench.SEC),
            ("max s", "max_s", bench.SEC),
            ("max/med", "skew", lambda v: f"{v:.1f}x"),
        ])
    else:
        print("  no stage reported task summaries (every stage had one task)")

    # --- 4. the conclusion -------------------------------------------------
    tot = job.totals(rows)
    banner("4. the bottleneck, stated")
    print(f"  wall time                 {wall:.1f}s")
    print(f"  sum of stage times        {tot['seconds']:.1f}s")
    print(f"  executor task time        {tot['task_ms'] / 1000:.0f}s"
          f"   -> {tot['task_ms'] / 1000 / max(wall, 1):.1f}x wall")
    print(f"  bytes read                {gb(tot['input_bytes'])}")
    print(f"  bytes shuffled            {gb(tot['shuffle_write_bytes'])}")
    print(f"  bytes spilled to disk     {gb(tot['disk_spill_bytes'])}")
    print(f"  read : shuffle ratio      "
          f"{tot['input_bytes'] / max(tot['shuffle_write_bytes'], 1):.0f} : 1")
    print()
    print(f"  SHAPE: ", end="")
    if tot["input_bytes"] > 10 * tot["shuffle_write_bytes"]:
        print("read-bound. Far more is read than shuffled, so the wins are in")
        print("         reading less (partitioning, pruning), not in shuffling better.")
    else:
        print("shuffle-bound. The wins are in moving less across the exchange")
        print("         (broadcast, pre-aggregation, fewer/better partitions).")
    if tot["disk_spill_bytes"] > 0:
        print(f"  SPILL: {gb(tot['disk_spill_bytes'])} went to disk. Something did not "
              f"fit in memory;\n         day 13 decides whether that is the cache.")
    max_skew = max((r["skew"] for r in spread), default=0)
    print(f"  SKEW : worst task max/median is {max_skew:.1f}x"
          f"{'  - worth a look on day 12' if max_skew > 3 else '  - evenly spread'}")

    profile = {
        "at": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "config": cfg.describe(),
        "batches": list(cfg.batches),
        "input_bytes_on_disk": cfg.input_bytes_on_disk(),
        "fact_rows": summary["n_facts"],
        "wall_seconds": round(wall, 1),
        "totals": tot,
        "stages": [{k: m.get(k, 0) for k in
                    ("label", "seconds", "pct", "input_bytes", "files_read",
                     "scan_rows", "shuffle_write_bytes", "disk_spill_bytes",
                     "tasks", "task_ms")} for m in rows],
        "shuffles": shuffle_rows,
        "task_spread": spread,
        "bottleneck": top["label"],
        "max_skew": max_skew,
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "day10_profile.json").write_text(json.dumps(profile, indent=2,
                                                           default=float))
    print(f"\n  written: {REPORTS / 'day10_profile.json'}")

    banner("what to go and click, now that you know what you are looking for")
    print("""  1. SQL/DataFrame tab -> the slowest query -> click its description.
     You get the operator tree with the metrics this script just printed.
     Read it BOTTOM UP: the leaves are the scans, and the first Exchange
     above them is where the data stopped being local.
  2. In that tree, every Exchange is a stage boundary. Count them. That
     number is how many times the whole dataset was written to disk and
     read back.
  3. Stages tab -> the longest stage -> the task table at the bottom, sorted
     by Duration. If the top task is many times the median, it is skew.
     The "Shuffle Read Size / Records" column on that same table tells you
     whether the slow task also read more - skew in the data - or the same
     amount slowly - a straggler.
  4. Storage tab: if a cached table shows "Fraction Cached" below 100%, the
     cache is costing more than it saves. Day 13.""")

    if args.hold:
        print(f"\n  holding. open {bench.ui_base(spark)} - ctrl-c to stop.")
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            pass
    spark.stop()
    print("\ndone.")


if __name__ == "__main__":
    main()
