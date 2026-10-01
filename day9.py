"""Day 9 - the end-to-end run, timed stage by stage.

Days 5-8 each answered one question in isolation. This is the job: read the
curated table, enrich it with the registry dimension, compute every aggregate
SCOPE.md needs, write the results, read them back to prove they are right.

The rule from SCOPE.md section 4: full data in, small aggregates out. The
inputs are gigabytes, the outputs are kilobytes, and nothing downstream ever
reads the fact table again.

    uv run python day9.py
    uv run python day9.py --out data/results --tag baseline
    uv run python day9.py --shuffle-partitions 200 --tag tuned
    uv run python day9.py --report          # compare past runs

Every run appends a row to data/reports/runs.jsonl with per-stage wall time,
bytes read and bytes shuffled, so "did that change help" is a lookup rather
than a memory.
"""
import argparse
import datetime
import json
import pathlib
import platform
import time

from pyspark.sql import Window, functions as F

import bench
import gbif
from bench import banner, gb, measure

HERE = pathlib.Path(__file__).parent
REPORTS = HERE / "data" / "reports"


class Stages:
    """One list of measurements, printed as the report at the end.

    Stage boundaries are chosen to match the questions someone would ask about
    the job - "is it read-bound or shuffle-bound", "how much of it is the
    join" - not to match the code's function boundaries.
    """
    def __init__(self, spark):
        self.spark, self.rows = spark, []

    def run(self, label, fn):
        print(f"\n--- {label} " + "-" * (60 - len(label)))
        with measure(self.spark, label) as m:
            out = fn()
        self.rows.append(m)
        print(f"    {m}")
        return out

    def report(self, wall):
        """The deliverable of day 9: where the time went, as a table."""
        for m in self.rows:
            m["pct"] = 100 * m["seconds"] / max(wall, 1e-9)
        banner("stage timings")
        bench.show(self.rows, [
            ("stage", "label", bench.TXT),
            ("secs", "seconds", bench.SEC),
            ("%", "pct", bench.PCT),
            ("read", "input_bytes", bench.BYTES),
            ("files", "files_read", bench.NUM),
            ("rows scanned", "scan_rows", bench.NUM),
            ("shuffle w", "shuffle_write_bytes", bench.BYTES),
            ("spill", "disk_spill_bytes", bench.BYTES),
            ("tasks", "tasks", bench.NUM),
        ])
        print()
        for m in self.rows:
            bar = "#" * int(round(46 * m["seconds"] / max(wall, 1e-9)))
            print(f"  {m['label']:<30}{m['seconds']:>8.1f}s {m['pct']:>5.0f}%  {bar}")
        return self.rows


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--table", default="data/curated/occurrence_slim")
    p.add_argument("--dim", default="data/curated/dataset_dim")
    p.add_argument("--out", default="data/results")
    p.add_argument("--tag", default="default", help="label this run in runs.jsonl")
    p.add_argument("--shuffle-partitions", type=int, default=48)
    p.add_argument("--driver-memory", default="4g")
    p.add_argument("--no-cache", action="store_true",
                   help="do not cache the enriched table - every stage re-reads")
    p.add_argument("--report", action="store_true", help="print past runs and exit")
    args = p.parse_args()

    if args.report:
        return print_report()

    out = pathlib.Path(args.out)
    t_total = time.perf_counter()
    started = datetime.datetime.now()

    banner(f"end-to-end run  [{args.tag}]  {started:%Y-%m-%d %H:%M}")
    print(f"table               : {args.table}")
    print(f"dimension           : {args.dim}")
    print(f"out                 : {out}")
    print(f"shuffle.partitions  : {args.shuffle_partitions}")
    print(f"driver memory       : {args.driver_memory}")
    print(f"cache enriched      : {not args.no_cache}")
    print(f"host                : {platform.machine()} / "
          f"{__import__('os').cpu_count()} cores")

    spark = gbif.spark_session(app=f"gbif-e2e-{args.tag}",
                               driver_memory=args.driver_memory,
                               shuffle_partitions=args.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")
    st = Stages(spark)

    # -- 1. read ------------------------------------------------------------
    state = {}

    def stage_read():
        facts = spark.read.option("mergeSchema", "true").parquet(args.table)
        dim = spark.read.parquet(args.dim)
        state["facts"], state["dim"] = facts, dim
        state["n_facts"] = facts.count()
        state["n_dim"] = dim.count()
        print(f"    facts {state['n_facts']:,} rows x {len(facts.columns)} cols"
              f"   dim {state['n_dim']:,} rows")
        return facts

    st.run("1 read + schema resolve", stage_read)

    # -- 2. enrich ----------------------------------------------------------
    def stage_enrich():
        dim = state["dim"].select("datasetkey", "publisher_key", "publisher_title",
                                  "publisher_country", "license")
        e = (state["facts"]
             .join(F.broadcast(dim), "datasetkey", "left")
             .withColumn("publisher_key",
                         F.coalesce("publisher_key", F.lit("UNREGISTERED")))
             .withColumn("publisher_title",
                         F.coalesce("publisher_title", F.lit("<not in registry>")))
             .select("gbifid", "datasetkey", "publisher_key", "publisher_title",
                     "publisher_country", "country", "decade", "year", "basis",
                     "species", "specieskey", "cell_id", "n_issues",
                     "n_geo_issues", "issues", "usable_for_mapping",
                     "uncertainty_known", "interpreted_year"))
        if not args.no_cache:
            e = e.cache()
            e.count()
        state["e"] = e
        plan = e._jdf.queryExecution().executedPlan().toString()
        print(f"    join strategy: "
              f"{'BroadcastHashJoin' if 'BroadcastHashJoin' in plan else 'SortMergeJoin'}")
        return e

    st.run("2 enrich (broadcast join)", stage_enrich)
    e = state["e"]

    # -- 3-7. the aggregates ------------------------------------------------
    results = {}

    def headline():
        r = e.agg(
            F.count("*").alias("records"),
            F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"),
            F.avg((F.col("usable_for_mapping") & F.col("uncertainty_known")).cast("int")).alias("usable_strict"),
            F.avg("n_issues").alias("mean_flags"),
            F.avg((F.col("n_issues") > 0).cast("int")).alias("any_flag"),
            F.avg((F.col("n_geo_issues") > 0).cast("int")).alias("geo_flag"),
            F.approx_count_distinct("specieskey", 0.02).alias("species"),
            F.approx_count_distinct("cell_id", 0.02).alias("cells"),
            F.approx_count_distinct("publisher_key", 0.02).alias("publishers"),
        )
        results["headline"] = r
        return r.collect()[0]

    head = st.run("3 headline aggregate", headline)

    def breakdowns():
        usable = F.avg(F.col("usable_for_mapping").cast("int")).alias("usable")
        n = F.count("*").alias("records")
        flags = F.avg("n_issues").alias("mean_flags")
        for name, keys in [("by_decade", ["decade"]),
                           ("by_country", ["country"]),
                           ("by_basis", ["basis"]),
                           ("by_publisher_country", ["publisher_country"]),
                           ("by_decade_country", ["decade", "country"])]:
            results[name] = e.groupBy(*keys).agg(n, usable, flags)
        # force them so the stage timing is honest - a lazy DataFrame costs 0
        return [d.count() for d in
                [results[k] for k in ("by_decade", "by_country", "by_basis",
                                      "by_publisher_country", "by_decade_country")]]

    st.run("4 breakdowns (5 groupBys)", breakdowns)

    def publisher_stats():
        per_dataset = (e.groupBy("publisher_key", "publisher_title", "datasetkey")
                        .agg(F.count("*").alias("records"),
                             F.avg("n_issues").alias("mean_flags"),
                             F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"))
                        .where(F.col("records") >= 1000))
        w = Window.partitionBy("publisher_key")
        ranked = (per_dataset
                  .withColumn("pub_records", F.sum("records").over(w))
                  .withColumn("pub_mean_flags", F.avg("mean_flags").over(w))
                  .withColumn("pub_datasets", F.count("*").over(w))
                  .withColumn("rank_in_pub",
                              F.row_number().over(w.orderBy(F.desc("records"))))
                  .withColumn("flag_gap", F.col("mean_flags") - F.col("pub_mean_flags")))
        results["by_dataset"] = ranked
        results["by_publisher"] = (e.groupBy("publisher_key", "publisher_title",
                                             "publisher_country")
                                    .agg(F.count("*").alias("records"),
                                         F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"),
                                         F.avg("n_issues").alias("mean_flags"),
                                         F.approx_count_distinct("datasetkey", 0.02).alias("datasets")))
        return ranked.count()

    n_ds = st.run("5 publisher/dataset windows", publisher_stats)

    def variance():
        w = Window.partitionBy("publisher_key")
        s = (results["by_dataset"].select("publisher_key", "mean_flags")
             .withColumn("pub_mean", F.avg("mean_flags").over(w))
             .withColumn("pub_n", F.count("*").over(w))
             .where(F.col("pub_n") >= 3))
        grand = s.agg(F.avg("mean_flags")).collect()[0][0] or 0.0
        v = s.agg(F.avg(F.pow(F.col("mean_flags") - F.col("pub_mean"), 2)).alias("within"),
                  F.avg(F.pow(F.col("pub_mean") - F.lit(grand), 2)).alias("between"),
                  F.count("*").alias("datasets")).collect()[0]
        row = {"grand_mean": grand, "within": v["within"] or 0.0,
               "between": v["between"] or 0.0, "datasets": v["datasets"]}
        row["between_share"] = row["between"] / max(row["within"] + row["between"], 1e-12)
        results["variance"] = spark.createDataFrame([row])
        return row

    var = st.run("6 variance decomposition", variance)

    def flag_stats():
        f = e.select("publisher_key", "decade", "country",
                     F.explode("issues").alias("flag"))
        results["by_flag"] = f.groupBy("flag").count()
        results["by_flag_publisher"] = f.groupBy("publisher_key", "flag").count()
        results["by_flag_decade"] = f.groupBy("decade", "flag").count()
        return results["by_flag"].count()

    n_flags = st.run("7 explode + flag aggregates", flag_stats)

    # -- 8. write -----------------------------------------------------------
    def write():
        out.mkdir(parents=True, exist_ok=True)
        sizes = {}
        for name, d in results.items():
            path = out / name
            d.coalesce(1).write.mode("overwrite").option("compression", "zstd") \
             .parquet(str(path))
            sizes[name] = sum(f.stat().st_size for f in path.rglob("*.parquet"))
        state["sizes"] = sizes
        print(f"    {len(sizes)} result tables, {gb(sum(sizes.values()))} total")
        return sizes

    sizes = st.run("8 write aggregates", write)

    # -- 9. read back -------------------------------------------------------
    def verify():
        back = spark.read.parquet(str(out / "by_decade"))
        total = back.agg(F.sum("records")).collect()[0][0]
        ok = total == state["n_facts"]
        print(f"    sum(by_decade.records) = {total:,}  "
              f"vs facts {state['n_facts']:,}  -> {'OK' if ok else 'MISMATCH'}")
        state["verified"] = bool(ok)
        return ok

    st.run("9 read back + verify", verify)

    # -- report -------------------------------------------------------------
    wall = time.perf_counter() - t_total
    rows = st.report(wall)
    print(f"\n  total wall time            {wall:.1f}s")
    print(f"  sum of stages              {sum(m['seconds'] for m in rows):.1f}s"
          f"   (the gap is session startup and plan construction)")
    print(f"  total bytes read           {gb(sum(m['input_bytes'] for m in rows))}")
    print(f"  total bytes shuffled       {gb(sum(m['shuffle_write_bytes'] for m in rows))}")
    print(f"  total executor task time   {sum(m['task_ms'] for m in rows) / 1000:.0f}s"
          f"   ({sum(m['task_ms'] for m in rows) / 1000 / max(wall, 1):.1f}x wall"
          f" = effective parallelism)")

    banner("the answer")
    print(f"  records                    {head['records']:>14,}")
    print(f"  usable for mapping         {100 * head['usable']:>13.2f}%")
    print(f"  usable, uncertainty known  {100 * head['usable_strict']:>13.2f}%")
    print(f"  carries any flag           {100 * head['any_flag']:>13.2f}%")
    print(f"  carries a fatal geo flag   {100 * head['geo_flag']:>13.2f}%")
    print(f"  mean flags per record      {head['mean_flags']:>14.2f}")
    print(f"  distinct species (~2%)     {head['species']:>14,}")
    print(f"  1-degree cells (~2%)       {head['cells']:>14,}")
    print(f"  publishers                 {head['publishers']:>14,}")
    print(f"\n  between-publisher variance {100 * var['between_share']:>13.0f}% of total")
    print(f"  claim (flags describe the publisher): "
          f"{'SUPPORTED' if var['between_share'] > 0.5 else 'NOT SUPPORTED'}")

    record = {
        "tag": args.tag,
        "at": started.isoformat(timespec="seconds"),
        "table": args.table,
        "fact_rows": state["n_facts"],
        "dim_rows": state["n_dim"],
        "shuffle_partitions": args.shuffle_partitions,
        "driver_memory": args.driver_memory,
        "cached": not args.no_cache,
        "wall_seconds": round(wall, 1),
        "verified": state["verified"],
        "stages": [{k: m.get(k, 0) for k in
                    ("label", "seconds", "input_bytes", "files_read",
                     "scan_rows", "shuffle_write_bytes", "shuffle_read_bytes",
                     "disk_spill_bytes", "tasks", "task_ms")} for m in rows],
        "result_bytes": sizes,
        "headline": {k: head[k] for k in head.asDict()},
        "variance": var,
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    with (REPORTS / "runs.jsonl").open("a") as f:
        f.write(json.dumps(record, default=float) + "\n")
    print(f"\nappended to {REPORTS / 'runs.jsonl'}   "
          f"(uv run python day9.py --report)")

    spark.stop()
    print("\ndone.")


def print_report():
    p = REPORTS / "runs.jsonl"
    if not p.exists():
        return print("no runs yet")
    runs = [json.loads(l) for l in p.open()]
    banner(f"{len(runs)} runs")
    print(f"  {'tag':<14}{'when':<18}{'rows':>13}{'shuf':>6}{'cache':>7}"
          f"{'wall s':>9}{'read':>11}{'shuffled':>11}  ok")
    for r in runs:
        rd = sum(s["input_bytes"] for s in r["stages"])
        sh = sum(s["shuffle_write_bytes"] for s in r["stages"])
        print(f"  {r['tag'][:13]:<14}{r['at'][:16]:<18}{r['fact_rows']:>13,}"
              f"{r['shuffle_partitions']:>6}{str(r['cached']):>7}"
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
