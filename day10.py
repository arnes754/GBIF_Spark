"""Profile the job and find its slowest stage.

Reads the driver UI REST API and prints three tables: stages ranked by wall
time, every Exchange with its shuffle bytes, and per-task min/median/max so
skew can be told apart from slowness. --hold keeps the session alive so the
UI at :4040 stays reachable after the run.

    uv run python day10.py
    uv run python day10.py --batches b0004
    uv run python day10.py --hold

Runs with --cache none by default: persisting the fact table OOMs the driver
on anything above the smallest batch. See day13.py.

Writes data/reports/day10_profile.json.
"""
import argparse
import json
import pathlib
import time

import bench
import job
from bench import banner, gb

REPORTS = pathlib.Path(__file__).parent / "data" / "reports"

# Minimum share of wall time for a stage to count as a bottleneck.
BOTTLENECK_SHARE = 0.15


def exchanges_for(execs, exec_ids):
    """Every Exchange operator in the given SQL executions, as rows.

    The flavours differ: hashpartitioning is a real shuffle, SinglePartition
    collapses everything to one task, BroadcastExchange is not a shuffle at
    all, and AQEShuffleRead reports the partition count that actually ran
    after AQE coalesced or split it.
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
    """Per-task duration quantiles for the stages a block owned.

    max/median near 1 is healthy. A large ratio means the stage is unbalanced
    rather than slow, which needs a different fix.
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
                   help="caching the fact table OOMs above the smallest "
                        "batch, so the default here is none")
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
    print("  Stages sorted by wall time. Speeding up a stage that is 2% of\n"
          "  the job can only ever buy 2%.\n")
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
    print("  anything below that share is not worth optimising.")

    # --- 2. where the shuffles are ----------------------------------------
    banner("2. where the shuffles happen")
    print("  One row per Exchange operator, read off the SQL operator tree.\n"
          "  A stage boundary is a shuffle.\n")
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
    print("  Per-task run time inside each stage. max/median near 1 is\n"
          "  healthy; a large ratio means one task is doing the work.\n")
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
        print("read-bound. More is read than shuffled, so look at reading")
        print("         less (partitioning, pruning) before shuffling better.")
    else:
        print("shuffle-bound. Look at moving less across the exchange")
        print("         (broadcast, pre-aggregation, partition count).")
    if tot["disk_spill_bytes"] > 0:
        print(f"  SPILL: {gb(tot['disk_spill_bytes'])} went to disk, so something "
              f"did not fit in memory.")
    max_skew = max((r["skew"] for r in spread), default=0)
    print(f"  SKEW : worst task max/median is {max_skew:.1f}x"
          f"{'' if max_skew > 3 else '  (evenly spread)'}")

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

    banner("where to look in the UI")
    print("""  SQL/DataFrame tab, slowest query: the operator tree with the
  metrics printed above. Read it bottom up - the leaves are the scans and
  the first Exchange above them is the stage boundary.

  Stages tab, longest stage: the task table at the bottom sorted by
  duration. A top task much larger than the median is skew; the shuffle
  read column says whether it also read more.

  Storage tab: "Fraction Cached" below 100% means partitions are being
  recomputed anyway.""")

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
