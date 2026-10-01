"""Week 3, day 3 (day 12) - broadcast joins, and skew.

Two things that are always taught together and are actually one thing: both are
about a shuffle, and both stop mattering if you can avoid the shuffle.

A join has to get rows with the same key onto the same machine. There are two
ways:

  SortMergeJoin   shuffle BOTH sides by the key, sort each partition, merge.
                  Works at any size. Costs writing and re-reading both sides.
  BroadcastHashJoin  send the small side, whole, to every task, and stream the
                  big side past it. No shuffle of the big side at all. Only
                  possible if the small side fits in each task's memory.

Spark picks broadcast by itself when it believes one side is under
`spark.sql.autoBroadcastJoinThreshold` (10 MB by default). "Believes" is the
load-bearing word, and day 4 of this project got burned by it: a 13-row lookup
got a SortMergeJoin, because the lookup was RDD-backed and had no size
*statistic*. The data was tiny; Spark did not know it was tiny.

SKEW is the other half. A shuffle sends each key to a partition chosen by its
hash. If one key has 40% of the rows, one partition gets 40% of the rows, and
one task does 40% of the work while the other eleven finish and wait. Nothing
is broken, every total is correct, and the job takes as long as its slowest
task. GBIF has this: a handful of datasetkeys are enormous.

    uv run python day12.py
    uv run python day12.py --batches b0000,b0003

Experiments:
  1  does Spark broadcast the dimension on its own, and how does it decide
  2  broadcast vs sort-merge, measured on the job's actual join
  3  how skewed is datasetkey, really
  4  a skewed sort-merge join, with AQE skew handling on and off
  5  salting the hot keys by hand, measured against AQE
"""
import argparse

from pyspark.sql import functions as F

import bench
import curate
import job
from bench import banner, gb, measure

# How many extra partitions a hot key is spread over when salting by hand.
# More = flatter, but every salt value multiplies the small side.
SALT = 16


def size_stat(df):
    """What Spark's optimizer BELIEVES this DataFrame weighs.

    Not what it weighs. This is the number the broadcast decision is made on,
    and the gap between it and reality is where day 4's bug lived.
    """
    return int(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())


def strategy(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    for name in ("BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin",
                 "BroadcastNestedLoopJoin"):
        if name in plan:
            return name
    return "none"


# --- 1 ----------------------------------------------------------------------
def exp_does_it_broadcast(spark, cfg):
    banner("1. does Spark broadcast the dimension without being asked?")
    facts = curate.read_table(spark, cfg.table, cfg.batches or None)
    dim = spark.read.parquet(cfg.dim).select(*job.DIM_COLUMNS)

    threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
    dim_disk = sum(f.stat().st_size for f in
                   __import__("pathlib").Path(cfg.dim).rglob("*.parquet"))
    print(f"  autoBroadcastJoinThreshold   {gb(threshold)}")
    print(f"  dimension on disk            {gb(dim_disk)}  ({dim.count():,} rows)")
    print(f"  dimension sizeInBytes stat   {gb(size_stat(dim))}")
    print(f"  fact table sizeInBytes stat  {gb(size_stat(facts))}")

    auto = facts.join(dim, "datasetkey", "left")
    print(f"\n  with no hint at all          -> {strategy(auto)}")
    print(f"  with F.broadcast(dim)        -> "
          f"{strategy(facts.join(F.broadcast(dim), 'datasetkey', 'left'))}")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    print(f"  with the threshold disabled  -> "
          f"{strategy(facts.join(dim, 'datasetkey', 'left'))}")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(threshold))

    print(f"""
  The statistic is the whole mechanism. A parquet-backed relation has one -
  Spark reads the file sizes without opening a single row group - so it knows
  the dimension is {gb(size_stat(dim))} and broadcasts it unprompted. Day 4's lookup was
  built with createDataFrame on an RDD, which has no statistic, so Spark fell
  back to "assume it is enormous" and shuffled 13 rows.

  Which is why the fix for a missing broadcast is usually not F.broadcast().
  It is "write the small side to parquet so Spark can see how small it is".
  F.broadcast() is a hint that overrides the estimate; useful when you know
  better than the statistic, and a liability when you do not - broadcasting
  something that turns out to be 2 GB kills the driver.
""")
    return {"threshold": threshold, "dim_stat": size_stat(dim),
            "auto_strategy": strategy(auto)}


# --- 2 ----------------------------------------------------------------------
def exp_broadcast_vs_sortmerge(spark, cfg):
    banner("2. broadcast vs sort-merge, on the job's actual join")
    facts = (curate.read_table(spark, cfg.table, cfg.batches or None)
             .select("datasetkey", "n_issues", "usable_for_mapping"))
    dim = spark.read.parquet(cfg.dim).select(*job.DIM_COLUMNS)
    threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")

    rows = []
    for label, setup in [("broadcast (what Spark picks)", lambda: None),
                         ("sort-merge (threshold = -1)",
                          lambda: spark.conf.set(
                              "spark.sql.autoBroadcastJoinThreshold", "-1"))]:
        setup()
        joined = (facts.join(dim, "datasetkey", "left")
                       .groupBy("publisher_key")
                       .agg(F.count("*").alias("n"),
                            F.avg("n_issues").alias("flags")))
        with measure(spark, label) as m:
            n = joined.count()
        rows.append({**m, "strategy": strategy(joined), "groups": n})
        spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(threshold))

    bench.show(rows, [
        ("join", "label", bench.TXT),
        ("plan chose", "strategy", bench.TXT),
        ("secs", "seconds", bench.SEC),
        ("read", "input_bytes", bench.BYTES),
        ("shuffle w", "shuffle_write_bytes", bench.BYTES),
        ("spill", "disk_spill_bytes", bench.BYTES),
        ("tasks", "tasks", bench.NUM),
    ])
    b, s = rows[0], rows[1]
    print(f"""
  The shuffle column is the finding, not the seconds. Sort-merge shuffles
  {gb(s['shuffle_write_bytes'])} where broadcast shuffles {gb(b['shuffle_write_bytes'])}: the whole fact table has to
  cross a stage boundary so that rows with equal datasetkey land together.
  Broadcast sends {gb(cfg_dim_bytes(cfg))} to every task instead and the fact table never moves.

  That ratio is also why skew disappears in experiment 4 when the join is a
  broadcast. Skew is a property of the shuffle. No shuffle, no skew.
""")
    return rows


def cfg_dim_bytes(cfg):
    import pathlib
    return sum(f.stat().st_size for f in pathlib.Path(cfg.dim).rglob("*.parquet"))


# --- 3 ----------------------------------------------------------------------
def exp_how_skewed(spark, cfg):
    banner("3. how skewed is datasetkey?")
    facts = curate.read_table(spark, cfg.table, cfg.batches or None)
    per_key = (facts.groupBy("datasetkey").count()
                    .orderBy(F.desc("count")).cache())
    total = facts.count()
    top = per_key.limit(10).collect()
    n_keys = per_key.count()

    print(f"  {total:,} rows over {n_keys:,} datasetkeys\n")
    print(f"  {'rank':>4}  {'datasetkey':<38}{'rows':>14}{'share':>8}"
          f"{'x mean':>9}")
    mean = total / max(n_keys, 1)
    for i, r in enumerate(top, 1):
        print(f"  {i:>4}  {r['datasetkey']:<38}{r['count']:>14,}"
              f"{100 * r['count'] / total:>7.2f}%{r['count'] / mean:>8.0f}x")

    top10 = sum(r["count"] for r in top)
    print(f"\n  top 10 keys hold {100 * top10 / total:.1f}% of the rows")
    print(f"  mean rows per key {mean:,.0f}, biggest key {top[0]['count']:,} "
          f"({top[0]['count'] / mean:,.0f}x the mean)")
    print(f"""
  What this means for a SHUFFLE on datasetkey: partitions are assigned by
  hash(key) % n. The biggest key cannot be split across partitions, so one
  partition is at least {top[0]['count']:,} rows no matter how many partitions you ask
  for. Raising shuffle.partitions does nothing for this - that is the single
  most common wrong fix.
""")
    per_key.unpersist()
    return {"keys": n_keys, "rows": total, "top10_share": top10 / total,
            "max_key_rows": top[0]["count"], "max_over_mean": top[0]["count"] / mean}


# --- 4 ----------------------------------------------------------------------
def skewed_join(spark, cfg):
    """A deliberately skewed sort-merge join: the fact table joined to a
    per-datasetkey summary of itself. Both sides keyed on the hot column, and
    the right side made too big to broadcast, so the shuffle is unavoidable."""
    facts = (curate.read_table(spark, cfg.table, cfg.batches or None)
             .select("gbifid", "datasetkey", "n_issues"))
    summary = (facts.groupBy("datasetkey")
                    .agg(F.count("*").alias("key_rows"),
                         F.avg("n_issues").alias("key_flags")))
    return facts, summary


def exp_aqe_skew(spark, cfg):
    banner("4. a skewed sort-merge join, with and without AQE skew handling")
    print("""  AQE's skew join works at runtime: after the shuffle it can SEE how
  big each partition came out, and it splits the oversized ones into several
  tasks, replicating the matching rows from the other side. A static optimiser
  cannot do this, because the sizes are not known until the shuffle has run.
""")
    threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # force shuffle

    rows = []
    for label, aqe, skew in [("AQE off", False, False),
                             ("AQE on, skewJoin off", True, False),
                             ("AQE on, skewJoin on", True, True)]:
        spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", str(skew).lower())
        facts, summary = skewed_join(spark, cfg)
        j = facts.join(summary, "datasetkey").select(
            F.sum(F.col("n_issues") - F.col("key_flags")).alias("x"))
        with measure(spark, label) as m:
            j.collect()
        rows.append({**m, "worst": worst_task(spark, m)})

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(threshold))
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

    bench.show(rows, [
        ("setting", "label", bench.TXT),
        ("secs", "seconds", bench.SEC),
        ("shuffle w", "shuffle_write_bytes", bench.BYTES),
        ("spill", "disk_spill_bytes", bench.BYTES),
        ("tasks", "tasks", bench.NUM),
        ("worst task / median", "worst", lambda v: f"{v:.1f}x"),
    ])
    print("""
  The column to read is the last one. Wall time can be flat - on local[*] with
  12 threads and a warm page cache, one slow task is often absorbed. The task
  distribution is where skew is visible before it becomes a problem, and it is
  what will blow up first when this runs on a real cluster with 100 GB.
""")
    return rows


def worst_task(spark, m):
    """max/median task time over the Spark stages this measurement owned."""
    worst = 0.0
    for s in bench.stage_list(spark, summaries=True):
        if s["stageId"] not in set(m["stage_ids"]):
            continue
        d = (s.get("taskMetricsDistributions") or {}).get("executorRunTime")
        if d and len(d) >= 5 and d[2]:
            worst = max(worst, d[4] / d[2])
    return worst


# --- 5 ----------------------------------------------------------------------
def exp_salting(spark, cfg):
    banner("5. salting the hot key by hand")
    print(f"""  The manual version of what AQE does. Add a random salt 0..{SALT - 1} to the
  big side's key, and explode the small side {SALT} times so every salt value has a
  copy to match. The hot key is now spread over {SALT} partitions instead of one.

  The cost is explicit: the small side gets {SALT}x bigger, and building it is an
  extra shuffle. You also have to KNOW which key is hot, in advance, which in
  practice means re-measuring every time the data changes.
""")
    threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")

    facts, summary = skewed_join(spark, cfg)

    rows = []
    j = facts.join(summary, "datasetkey").select(
        F.sum(F.col("n_issues") - F.col("key_flags")).alias("x"))
    with measure(spark, "plain (skewJoin off)") as m:
        j.collect()
    rows.append({**m, "worst": worst_task(spark, m)})

    salted_facts = facts.withColumn(
        "salt", (F.rand() * SALT).cast("int"))
    salted_summary = (summary
                      .withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(SALT)]))))
    js = (salted_facts.join(salted_summary, ["datasetkey", "salt"])
          .select(F.sum(F.col("n_issues") - F.col("key_flags")).alias("x")))
    with measure(spark, f"salted x{SALT}") as m:
        js.collect()
    rows.append({**m, "worst": worst_task(spark, m)})

    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    j2 = facts.join(summary, "datasetkey").select(
        F.sum(F.col("n_issues") - F.col("key_flags")).alias("x"))
    with measure(spark, "AQE skewJoin on") as m:
        j2.collect()
    rows.append({**m, "worst": worst_task(spark, m)})
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(threshold))

    bench.show(rows, [
        ("approach", "label", bench.TXT),
        ("secs", "seconds", bench.SEC),
        ("shuffle w", "shuffle_write_bytes", bench.BYTES),
        ("spill", "disk_spill_bytes", bench.BYTES),
        ("tasks", "tasks", bench.NUM),
        ("worst task / median", "worst", lambda v: f"{v:.1f}x"),
    ])
    print("""
  Salting works, and it is more code, more shuffle bytes, and one more thing
  that goes stale. AQE needs no code and cannot be wrong about which key is
  hot, because it looks. Salting earns its place when AQE cannot help - a
  skewed groupBy rather than a skewed join, for instance, which AQE will not
  split.
""")
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batches", default="b0000")
    p.add_argument("--only", default="")
    args = p.parse_args()

    cfg = job.Config(batches=tuple(b for b in args.batches.split(",") if b),
                     tag="day12")
    banner("day 12 - broadcast joins and skew")
    print(f"  input : {cfg.describe()}")
    print(f"          {gb(cfg.input_bytes_on_disk())} on disk")
    spark = job.session_for(cfg, app="gbif-day12")

    chosen = args.only or "12345"
    if "1" in chosen:
        exp_does_it_broadcast(spark, cfg)
    if "2" in chosen:
        exp_broadcast_vs_sortmerge(spark, cfg)
    if "3" in chosen:
        exp_how_skewed(spark, cfg)
    if "4" in chosen:
        exp_aqe_skew(spark, cfg)
    if "5" in chosen:
        exp_salting(spark, cfg)

    banner("what day 12 changes about the job")
    print("""  Nothing, and that is the result.

  The job's only join is fact x dimension on datasetkey. The dimension is
  0.2 MB of parquet, Spark has a real statistic for it, and it broadcasts it
  without being asked. So:

    - there is no shuffle on the join, so there is no skew on the join,
      however skewed datasetkey is. Experiment 3 measures real skew in the
      column and experiment 2 shows the join does not care.
    - F.broadcast() stays OUT of job.py's default path. It is kept behind
      --join broadcast purely so the comparison can be re-run. Hinting what
      the optimiser already gets right is how you end up with a hint that is
      wrong after the data grows.
    - AQE's skew handling stays on. It costs nothing when there is no skew,
      and the day the dimension grows past 10 MB it is the thing that saves
      the job.

  The transferable version: before tuning a join, check whether it is a
  shuffle at all. Most "my join is slow" is a join that should have been a
  broadcast and is not, and the usual cause is a missing statistic - not a
  missing hint.""")
    spark.stop()
    print("\ndone.")


if __name__ == "__main__":
    main()
