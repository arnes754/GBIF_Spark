"""The pipeline: read the curated table, join the publisher dimension, write
the aggregates.

    cfg = Config(batches=["b0004"], shuffle_partitions=200, cache="disk")
    spark = session_for(cfg)
    stages, results, summary = run(spark, cfg)

Every tunable setting is a field on Config and no stage reads a global, so two
runs differ by exactly the fields that differ in their Config. day9.py is the
CLI over this module.
"""
import dataclasses
import pathlib

from pyspark.sql import Window, functions as F

import bench
import curate
import gbif
from bench import gb, measure

HERE = pathlib.Path(__file__).parent
DEFAULT_TABLE = str(HERE / "data" / "curated" / "occurrence_slim")
DEFAULT_DIM = str(HERE / "data" / "curated" / "dataset_dim")

# Spark's own default.
DEFAULT_MAX_PARTITION_BYTES = 128 * 1024**2

CACHE_MODES = ("none", "memory", "memory_and_disk", "disk")
JOIN_MODES = ("auto", "broadcast", "sortmerge")


@dataclasses.dataclass
class Config:
    """Everything a run is allowed to differ by."""
    # --- what to read -------------------------------------------------------
    table: str = DEFAULT_TABLE
    dim: str = DEFAULT_DIM
    batches: tuple = ()           # () means the whole table
    out: str = str(HERE / "data" / "results")

    # --- tuning -------------------------------------------------------------
    shuffle_partitions: int = 48
    max_partition_bytes: int = DEFAULT_MAX_PARTITION_BYTES
    # Persisting the fact table OOMs the driver above ~20 MB and saves nothing
    # when it does fit, so the default is off.
    cache: str = "none"                 # see CACHE_MODES
    # Each aggregate stage writes its own results. Off, the write stage
    # recomputes every aggregate a second time.
    write_in_place: bool = True
    # Persist the per-dataset aggregate (~1.4k rows); stages 5, 6 and 8 read it.
    cache_results: bool = True
    join: str = "auto"                  # see JOIN_MODES
    aqe: bool = True
    aqe_skew_join: bool = True
    coalesce_output: bool = True        # one file per result table
    driver_memory: str = "4g"
    tag: str = "default"

    def describe(self):
        """One-line summary of the settings, printed above each run."""
        where = ",".join(self.batches) if self.batches else "all batches"
        return (f"{where}  shuffle={self.shuffle_partitions}  "
                f"cache={self.cache}  join={self.join}  "
                f"aqe={'on' if self.aqe else 'off'}  "
                f"maxPartitionBytes={self.max_partition_bytes // 1024**2}MB  "
                f"write_in_place={self.write_in_place}  "
                f"cache_results={self.cache_results}")

    def input_bytes_on_disk(self):
        return curate.table_bytes(self.table, self.batches or None)


def session_for(cfg, app=None):
    """Build a session from a Config. AQE is set here, not in
    gbif.spark_session, so day12.py can turn it off and measure the difference."""
    spark = gbif.spark_session(app=app or f"gbif-{cfg.tag}",
                               driver_memory=cfg.driver_memory,
                               shuffle_partitions=cfg.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")
    c = spark.conf
    c.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))
    c.set("spark.sql.files.maxPartitionBytes", str(cfg.max_partition_bytes))
    c.set("spark.sql.adaptive.enabled", str(cfg.aqe).lower())
    c.set("spark.sql.adaptive.skewJoin.enabled", str(cfg.aqe_skew_join).lower())
    if cfg.join == "sortmerge":
        # -1 turns auto-broadcast off. Only used to force the comparison.
        c.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    return spark


# --- the stages -------------------------------------------------------------

def stage_read(spark, cfg):
    facts = curate.read_table(spark, cfg.table, cfg.batches or None)
    dim = spark.read.parquet(cfg.dim)
    return facts, dim


DIM_COLUMNS = ["datasetkey", "publisher_key", "publisher_title",
               "publisher_country", "license"]

# The job uses 15 of the curated table's 47 columns.
FACT_COLUMNS = ["gbifid", "datasetkey", "country", "decade", "year", "basis",
                "species", "specieskey", "cell_id", "n_issues", "n_geo_issues",
                "issues", "usable_for_mapping", "uncertainty_known",
                "interpreted_year"]


def stage_enrich(facts, dim, cfg):
    """Left join the publisher dimension and project to the columns used.

    Left, not inner: an inner join would drop rows with no registered
    publisher, and those rows are part of the answer.
    """
    d = dim.select(*DIM_COLUMNS)
    if cfg.join == "broadcast":
        d = F.broadcast(d)
    e = (facts
         .join(d, "datasetkey", "left")
         .withColumn("publisher_key",
                     F.coalesce("publisher_key", F.lit("UNREGISTERED")))
         .withColumn("publisher_title",
                     F.coalesce("publisher_title", F.lit("<not in registry>")))
         .select(*FACT_COLUMNS, "publisher_key", "publisher_title",
                 "publisher_country"))
    return apply_cache(e, cfg)


def apply_cache(df, cfg):
    """Persist the enriched table if the config asks for it."""
    from pyspark import StorageLevel
    levels = {"memory": StorageLevel.MEMORY_ONLY,
              "memory_and_disk": StorageLevel.MEMORY_AND_DISK,
              "disk": StorageLevel.DISK_ONLY}
    if cfg.cache == "none":
        return df
    df = df.persist(levels[cfg.cache])
    df.count()          # materialise it, so the cost lands in this stage
    return df


def join_strategy(df):
    """Which join Spark actually chose, read off the executed plan."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    return "BroadcastHashJoin" if "BroadcastHashJoin" in plan else "SortMergeJoin"


def stage_headline(e, results):
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


BREAKDOWNS = [("by_decade", ["decade"]),
              ("by_country", ["country"]),
              ("by_basis", ["basis"]),
              ("by_publisher_country", ["publisher_country"]),
              ("by_decade_country", ["decade", "country"])]


def stage_breakdowns(e, results, force=True):
    usable = F.avg(F.col("usable_for_mapping").cast("int")).alias("usable")
    n = F.count("*").alias("records")
    flags = F.avg("n_issues").alias("mean_flags")
    for name, keys in BREAKDOWNS:
        results[name] = e.groupBy(*keys).agg(n, usable, flags)
    # Without an action a lazy DataFrame costs nothing and the stage timing
    # is meaningless. force=False means the caller writes these straight
    # after, so the write is the action and counting first would double the
    # work.
    if not force:
        return []
    return [results[name].count() for name, _ in BREAKDOWNS]


def stage_publisher_stats(e, results, force=True, cache_results=False):
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
    if cache_results:
        # ~1.4k rows, read again by stages 6 and 8.
        ranked = ranked.persist()
    results["by_dataset"] = ranked
    results["by_publisher"] = (e.groupBy("publisher_key", "publisher_title",
                                         "publisher_country")
                                .agg(F.count("*").alias("records"),
                                     F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"),
                                     F.avg("n_issues").alias("mean_flags"),
                                     F.approx_count_distinct("datasetkey", 0.02).alias("datasets")))
    return ranked.count() if (force or cache_results) else 0


def stage_variance(spark, results):
    """Between-publisher variance over total variance: how much of the
    variation in flag counts is explained by who published the record."""
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


def stage_flags(e, results, force=True):
    f = e.select("publisher_key", "decade", "country",
                 F.explode("issues").alias("flag"))
    results["by_flag"] = f.groupBy("flag").count()
    results["by_flag_publisher"] = f.groupBy("publisher_key", "flag").count()
    results["by_flag_decade"] = f.groupBy("decade", "flag").count()
    return results["by_flag"].count() if force else 0


def write_one(df, name, cfg):
    """Write one result table, return its size on disk."""
    path = pathlib.Path(cfg.out) / name
    path.parent.mkdir(parents=True, exist_ok=True)
    w = df.coalesce(1) if cfg.coalesce_output else df
    w.write.mode("overwrite").option("compression", "zstd").parquet(str(path))
    return sum(f.stat().st_size for f in path.rglob("*.parquet"))


def stage_write(results, cfg, skip=()):
    out = pathlib.Path(cfg.out)
    out.mkdir(parents=True, exist_ok=True)
    return {name: write_one(d, name, cfg)
            for name, d in results.items() if name not in skip}


def stage_verify(spark, cfg, n_facts):
    """Read one result back and check it sums to the input row count. Cheap,
    and it catches a tuning change that quietly loses rows."""
    back = spark.read.parquet(str(pathlib.Path(cfg.out) / "by_decade"))
    total = back.agg(F.sum("records")).collect()[0][0]
    return total, bool(total == n_facts)


# --- the whole job ----------------------------------------------------------
class Stages:
    """Collects one measurement per stage and prints the report."""

    def __init__(self, spark, quiet=False):
        self.spark, self.rows, self.quiet = spark, [], quiet

    def run(self, label, fn):
        if not self.quiet:
            print(f"\n--- {label} " + "-" * max(0, 60 - len(label)))
        with measure(self.spark, label) as m:
            out = fn()
        self.rows.append(m)
        if not self.quiet:
            print(f"    {m}")
        return out

    def report(self, wall):
        for m in self.rows:
            m["pct"] = 100 * m["seconds"] / max(wall, 1e-9)
        bench.banner("stage timings")
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


def run(spark, cfg, quiet=False):
    """The nine stages, measured. Returns (stage rows, results, summary)."""
    st = Stages(spark, quiet=quiet)
    results, summary = {}, {}

    def _read():
        facts, dim = stage_read(spark, cfg)
        summary["n_facts"] = facts.count()
        summary["n_dim"] = dim.count()
        summary["n_fact_columns"] = len(facts.columns)
        if not quiet:
            print(f"    facts {summary['n_facts']:,} rows x "
                  f"{len(facts.columns)} cols   dim {summary['n_dim']:,} rows")
        return facts, dim

    facts, dim = st.run("1 read + schema resolve", _read)

    def _enrich():
        e = stage_enrich(facts, dim, cfg)
        summary["join_strategy"] = join_strategy(e)
        if not quiet:
            print(f"    join strategy: {summary['join_strategy']}")
        return e

    e = st.run("2 enrich (join + cache)", _enrich)

    # Without write_in_place each aggregate stage forces its results with a
    # count() and stage 8 recomputes them all to write them: 22 passes over
    # the fact table for 11 result tables. With it, each stage writes what it
    # built, so nothing is computed twice.
    sizes = {}
    force = not cfg.write_in_place

    def staged(label, fn):
        """Run one aggregate stage and, if asked, write what it produced."""
        def inner():
            before = set(results)
            out = fn()
            if cfg.write_in_place:
                for name in [k for k in results if k not in before]:
                    sizes[name] = write_one(results[name], name, cfg)
            return out
        return st.run(label, inner)

    head = staged("3 headline aggregate", lambda: stage_headline(e, results))
    staged("4 breakdowns (5 groupBys)",
           lambda: stage_breakdowns(e, results, force))
    staged("5 publisher/dataset windows",
           lambda: stage_publisher_stats(e, results, force, cfg.cache_results))
    var = staged("6 variance decomposition", lambda: stage_variance(spark, results))
    staged("7 explode + flag aggregates", lambda: stage_flags(e, results, force))
    sizes = st.run("8 write aggregates",
                   lambda: {**sizes, **stage_write(results, cfg, skip=set(sizes))})

    def _verify():
        total, ok = stage_verify(spark, cfg, summary["n_facts"])
        if not quiet:
            print(f"    sum(by_decade.records) = {total:,} vs facts "
                  f"{summary['n_facts']:,} -> {'OK' if ok else 'MISMATCH'}")
        return ok

    summary["verified"] = st.run("9 read back + verify", _verify)
    summary["headline"] = {k: head[k] for k in head.asDict()}
    summary["variance"] = var
    summary["result_bytes"] = sizes
    summary["result_total_bytes"] = sum(sizes.values())
    if e.is_cached:
        e.unpersist()
    if cfg.cache_results and results.get("by_dataset") is not None \
            and results["by_dataset"].is_cached:
        results["by_dataset"].unpersist()
    return st, results, summary


def print_answer(summary):
    head, var = summary["headline"], summary["variance"]
    bench.banner("the answer")
    print(f"  records                    {head['records']:>14,.0f}")
    print(f"  usable for mapping         {100 * head['usable']:>13.2f}%")
    print(f"  usable, uncertainty known  {100 * head['usable_strict']:>13.2f}%")
    print(f"  carries any flag           {100 * head['any_flag']:>13.2f}%")
    print(f"  carries a fatal geo flag   {100 * head['geo_flag']:>13.2f}%")
    print(f"  mean flags per record      {head['mean_flags']:>14.2f}")
    print(f"  distinct species (~2%)     {head['species']:>14,.0f}")
    print(f"  1-degree cells (~2%)       {head['cells']:>14,.0f}")
    print(f"  publishers                 {head['publishers']:>14,.0f}")
    print(f"\n  between-publisher variance {100 * var['between_share']:>13.0f}% of total")
    print(f"  claim (flags describe the publisher): "
          f"{'SUPPORTED' if var['between_share'] > 0.5 else 'NOT SUPPORTED'}")


def totals(rows):
    """Per-run totals, summed over the stages."""
    return {"seconds": sum(m["seconds"] for m in rows),
            "input_bytes": sum(m["input_bytes"] for m in rows),
            "shuffle_write_bytes": sum(m["shuffle_write_bytes"] for m in rows),
            "disk_spill_bytes": sum(m["disk_spill_bytes"] for m in rows),
            "task_ms": sum(m["task_ms"] for m in rows),
            "tasks": sum(m["tasks"] for m in rows)}


__all__ = ["Config", "session_for", "run", "print_answer", "totals", "gb"]
