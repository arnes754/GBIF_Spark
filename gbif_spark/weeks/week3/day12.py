"""Broadcast joins and data skew.

A join has to get rows with the same key onto the same machine. SortMergeJoin
shuffles both sides and sorts; BroadcastHashJoin sends the small side whole to
every task and never moves the big side. Spark picks broadcast when it
estimates one side is under spark.sql.autoBroadcastJoinThreshold (10 MB), and
that estimate comes from a statistic, not from the real size.

Skew is the other half: a shuffle assigns keys to partitions by hash, so one
very common key puts a disproportionate share of the rows in one task. Totals
stay correct and the job runs as slow as its slowest task.

    uv run python -m gbif_spark day 12
    uv run python -m gbif_spark day 12 --batches b0000,b0003
    uv run python -m gbif_spark day 12 --only 3

  1  does Spark broadcast the dimension on its own
  2  broadcast vs sort-merge on the job's actual join
  3  how skewed datasetkey is
  4  a skewed sort-merge join, with AQE skew handling on and off
  5  hand-salting, measured against AQE
"""
import argparse
import pathlib

from pyspark.sql import functions as F

from gbif_spark.helpers import bench
from gbif_spark.helpers.bench import banner, gb, measure
from gbif_spark.pipeline import curate, job

SALT = 16


def size_stat(df):
    """The optimizer's size estimate for this DataFrame, which is what the
    broadcast decision is made on - not the real size."""
    return int(df._jdf.queryExecution().optimizedPlan().stats().sizeInBytes())


def strategy(df):
    plan = df._jdf.queryExecution().executedPlan().toString()
    for name in ("BroadcastHashJoin", "ShuffledHashJoin", "SortMergeJoin",
                 "BroadcastNestedLoopJoin"):
        if name in plan:
            return name
    return "none"


def exp_does_it_broadcast(spark, cfg):
    banner("1. does Spark broadcast the dimension without being asked?")
    facts = curate.read_table(spark, cfg.table, cfg.batches or None)
    dim = spark.read.parquet(cfg.dim).select(*job.DIM_COLUMNS)

    threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
    dim_disk = dimension_bytes(cfg)
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
  The size statistic is the mechanism. A parquet relation has one - Spark
  reads the file footers without opening a row group - so it knows the
  dimension is {gb(size_stat(dim))} and broadcasts it. An RDD-backed DataFrame has no
  statistic, so Spark assumes it is large and shuffles it even when it holds
  a handful of rows.

  So the fix for a missing broadcast is usually to write the small side to
  parquet, not to add F.broadcast(). The hint overrides the estimate, which
  also means it can be wrong once the data grows.
""")
    return {"threshold": threshold, "dim_stat": size_stat(dim),
            "auto_strategy": strategy(auto)}


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
  Read the shuffle column rather than the seconds. Sort-merge shuffles
  {gb(s['shuffle_write_bytes'])} against broadcast's {gb(b['shuffle_write_bytes'])}: the whole fact table crosses a
  stage boundary so rows with equal datasetkey land together. Broadcast
  sends {gb(dimension_bytes(cfg))} to every task and the fact table never moves.

  Skew is a property of the shuffle, so no shuffle means no skew.
""")
    return rows


def dimension_bytes(cfg):
    """How big the publisher dimension is on disk - the number that decides
    whether Spark will broadcast it."""
    return sum(f.stat().st_size for f in pathlib.Path(cfg.dim).rglob("*.parquet"))


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
  For a shuffle on datasetkey, partitions are assigned by hash(key) % n. The
  biggest key cannot be split across partitions, so one partition holds at
  least {top[0]['count']:,} rows however many partitions are requested. Raising
  shuffle.partitions does not help.
""")
    per_key.unpersist()
    return {"keys": n_keys, "rows": total, "top10_share": top10 / total,
            "max_key_rows": top[0]["count"], "max_over_mean": top[0]["count"] / mean}


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
    print("  AQE's skew join runs after the shuffle, when the real partition\n"
          "  sizes are known, and splits oversized partitions across several\n"
          "  tasks. A static optimiser cannot do this.\n")
    threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

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
    print("\n  The last column is the one to read. Wall time can stay flat on\n"
          "  local[*] with a warm page cache, because one slow task gets\n"
          "  absorbed. The task distribution shows the skew first.\n")
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


def exp_salting(spark, cfg):
    banner("5. salting the hot key by hand")
    print(f"""  The manual version of what AQE does: add a random salt 0..{SALT - 1} to
  the big side's key and explode the small side {SALT} times so every salt value
  has a copy to match. The hot key spreads over {SALT} partitions.

  The costs: the small side is {SALT}x bigger, building it is an extra shuffle,
  and you have to know in advance which key is hot.
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
  Salting works, at the cost of more code, more shuffle bytes and a hot-key
  list that goes stale. AQE needs no code and measures the sizes itself.
  Salting is worth it where AQE does not help, such as a skewed groupBy.
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

    banner("conclusions")
    print("""  No change to the job.

  Its only join is fact x dimension on datasetkey. The dimension is 0.2 MB of
  parquet, Spark has a real size statistic for it and broadcasts it without
  being asked, so there is no shuffle on the join and therefore no skew on it
  however skewed the column is.

  F.broadcast() stays out of the default path and is kept behind
  --join broadcast so the comparison can be re-run. AQE skew handling stays
  on: it costs nothing when there is no skew and matters if the dimension
  ever grows past the broadcast threshold.

  Before tuning a join, check whether it is a shuffle at all.""")
    spark.stop()
    print("\ndone.")


if __name__ == "__main__":
    main()
