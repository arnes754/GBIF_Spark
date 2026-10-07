"""Caching: what it costs as well as what it saves.

A DataFrame is a recipe, not a table, so every action re-runs it from the
files. persist() keeps the result of the first computation and serves later
reads from it:

    pay  : materialising and storing it, once
    save : recomputing it, on every read after the first

so caching only wins when save x (readers - 1) > pay.

Storage levels: MEMORY_ONLY keeps the columnar format in the JVM heap and
silently skips partitions that do not fit, which means they get recomputed.
MEMORY_AND_DISK spills those to local disk instead. DISK_ONLY always writes to
disk. Building the cache is not free either: each task unrolls its partition
into the columnar format in the same heap the query is using.

    uv run python -m gbif_spark day 13
    uv run python -m gbif_spark day 13 --batches b0003 --big b0000
    uv run python -m gbif_spark day 13 --only 2

  1  how many times the job reads the enriched table
  2  no cache vs the three storage levels, end to end
  3  the break-even: how many readers before caching pays
  4  what happens when the cache does not fit
"""
import argparse
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F

from gbif_spark.helpers import bench
from gbif_spark.helpers.bench import banner, gb, measure
from gbif_spark.pipeline import curate, job

LEVELS = {
    "none": None,
    "memory": StorageLevel.MEMORY_ONLY,
    "memory_and_disk": StorageLevel.MEMORY_AND_DISK,
    "disk": StorageLevel.DISK_ONLY,
}


def enriched(spark, cfg):
    """The job's stage 2 output, without the caching decision baked in."""
    facts = curate.read_table(spark, cfg.table, cfg.batches or None)
    dim = spark.read.parquet(cfg.dim).select(*job.DIM_COLUMNS)
    return (facts.join(dim, "datasetkey", "left")
            .withColumn("publisher_key",
                        F.coalesce("publisher_key", F.lit("UNREGISTERED")))
            .select(*job.FACT_COLUMNS, "publisher_key"))


def consumers(e):
    """The aggregates the real job runs over the enriched table. Each one is
    an action, so each one is a separate pass if nothing is cached."""
    return [
        ("headline", lambda: e.agg(
            F.count("*"), F.avg(F.col("usable_for_mapping").cast("int")),
            F.avg("n_issues")).collect()),
        ("by_decade", lambda: e.groupBy("decade").count().collect()),
        ("by_country", lambda: e.groupBy("country").count().collect()),
        ("by_basis", lambda: e.groupBy("basis").count().collect()),
        ("by_publisher", lambda: e.groupBy("publisher_key").agg(
            F.avg("n_issues")).collect()),
        ("by_dataset", lambda: e.groupBy("datasetkey").agg(
            F.count("*"), F.avg("n_issues")).collect()),
        ("flags", lambda: e.select(F.explode("issues").alias("f"))
                           .groupBy("f").count().collect()),
    ]


def storage_report(spark):
    """The Storage tab, as numbers. `fraction cached` below 100% is the single
    most useful cache diagnostic there is, and it is invisible unless you look.
    """
    try:
        return bench.ui_json(spark, "/storage/rdd")
    except Exception:
        return []


def exp_how_many_readers(spark, cfg):
    banner("1. how many times does the job read the enriched table?")
    e = enriched(spark, cfg)
    rows = []
    for name, fn in consumers(e):
        with measure(spark, name) as m:
            fn()
        rows.append(m)
    bench.show(rows, [
        ("consumer", "label", bench.TXT),
        ("secs", "seconds", bench.SEC),
        ("read", "input_bytes", bench.BYTES),
        ("files", "files_read", bench.NUM),
        ("rows scanned", "scan_rows", bench.NUM),
    ])
    total_read = sum(m["input_bytes"] for m in rows)
    once = max((m["input_bytes"] for m in rows), default=0)
    print(f"""
  {len(rows)} consumers, {gb(total_read)} read in total, {gb(once)} per pass. With nothing
  cached the table is scanned {len(rows)} times, which is the argument for caching.

  The per-consumer reads differ because of column pruning: a consumer that
  touches three columns reads less than one that touches eight. A cache holds
  whatever columns the cached DataFrame had, so every reader pays for all of
  them.
""")
    return rows


def exp_levels(spark, cfg):
    banner("2. no cache vs the three storage levels, end to end")
    print("  Same seven consumers each time. `build` is materialising the\n"
          "  cache, `consume` is the reads afterwards. Caching wins only if\n"
          "  build + consume beats consume with nothing cached.\n")
    rows = []
    for name, level in LEVELS.items():
        e = enriched(spark, cfg)
        build_s, cached_bytes, fraction = 0.0, 0, 1.0
        if level is not None:
            e = e.persist(level)
            t = time.perf_counter()
            e.count()
            build_s = time.perf_counter() - t
            rdds = storage_report(spark)
            if rdds:
                r = rdds[-1]
                cached_bytes = r.get("memoryUsed", 0) + r.get("diskUsed", 0)
                fraction = (r.get("numCachedPartitions", 0)
                            / max(r.get("numPartitions", 1), 1))
        t = time.perf_counter()
        with measure(spark, name) as m:
            for _, fn in consumers(e):
                fn()
        consume_s = time.perf_counter() - t
        rows.append({"level": name, "build_s": build_s, "consume_s": consume_s,
                     "total_s": build_s + consume_s,
                     "read": m["input_bytes"], "spill": m["disk_spill_bytes"],
                     "stored": cached_bytes, "fraction": 100 * fraction})
        if level is not None:
            e.unpersist(blocking=True)

    base = next(r["total_s"] for r in rows if r["level"] == "none")
    for r in rows:
        r["speedup"] = base / max(r["total_s"], 1e-9)

    bench.show(rows, [
        ("storage level", "level", bench.TXT),
        ("build s", "build_s", bench.SEC),
        ("consume s", "consume_s", bench.SEC),
        ("total s", "total_s", bench.SEC),
        ("vs no cache", "speedup", lambda v: f"{v:.2f}x"),
        ("bytes read", "read", bench.BYTES),
        ("cache size", "stored", bench.BYTES),
        ("% cached", "fraction", bench.PCT),
        ("spill", "spill", bench.BYTES),
    ])
    print("""
  Two columns matter:

  `bytes read` - caching should drive this towards a single pass. If it does
  not, the cache is not being hit and something is recomputing.
  `% cached`   - below 100 means partitions were evicted or never stored, so
  the storage cost and the recompute cost are both being paid.
""")
    return rows


def exp_break_even(spark, cfg):
    banner("3. the break-even - how many readers before caching pays?")
    print("  One scan timed, one cache build timed, then the arithmetic.\n")
    e = enriched(spark, cfg)
    with measure(spark, "one uncached pass") as cold:
        e.groupBy("decade").count().collect()
    scan_s = cold["seconds"]

    e2 = enriched(spark, cfg).persist(StorageLevel.MEMORY_AND_DISK)
    t = time.perf_counter()
    e2.count()
    build_s = time.perf_counter() - t
    with measure(spark, "one cached pass") as warm:
        e2.groupBy("decade").count().collect()
    cached_s = warm["seconds"]
    e2.unpersist(blocking=True)

    saved = scan_s - cached_s
    break_even = (build_s / saved + 1) if saved > 0 else float("inf")

    print(f"  cost of one uncached pass      {scan_s:>8.1f}s")
    print(f"  cost of one cached pass        {cached_s:>8.1f}s")
    print(f"  saved per read after the first {saved:>8.1f}s")
    print(f"  cost of building the cache     {build_s:>8.1f}s")
    print()
    if saved <= 0:
        print("  BREAK-EVEN: never. A cached read is not faster than a fresh\n"
              "  read here, so the build cost is pure loss. This happens when\n"
              "  the recipe being cached is cheap - a parquet scan with column\n"
              "  pruning is already close to the fastest thing on the machine.")
    else:
        print(f"  BREAK-EVEN: {break_even:.1f} readers. The job has "
              f"{len(consumers(e))}, so caching "
              f"{'pays' if break_even <= len(consumers(e)) else 'does NOT pay'}.")
    print(f"""
  Caching pays when the cached thing was expensive to produce - a shuffle, a
  join, a UDF, a wide filter. It does not pay for a cheap scan, because
  parquet with column pruning is already fast and a cache cannot prune
  columns per reader. The enriched table here is a broadcast join over a
  parquet scan, with no shuffle in it.

  Caveat: on a slice small enough for all four levels to complete, one pass
  costs a fraction of a second, which is the same order as the measurement
  noise, so the break-even number above means "too small to matter" rather
  than a precise count. Measuring on a bigger slice is not possible because
  the cache OOMs there (experiment 4).
""")
    return {"scan_s": scan_s, "cached_s": cached_s, "build_s": build_s,
            "break_even": break_even}


def exp_does_not_fit(spark, cfg, big):
    banner("4. when the cache does not fit")
    print(f"""  Experiments 1-3 used {','.join(cfg.batches) or 'the whole table'}, which fits. This one uses
  {big}, which does not:

      Caused by: java.lang.OutOfMemoryError: Java heap space
        at ... stage_enrich -> apply_cache -> df.count()

  The driver JVM has {cfg.driver_memory} and Spark gives roughly 60% of that to execution
  and storage together. Caching millions of rows x 16 columns in the columnar
  format, with 12 tasks unrolling partitions into that same heap at once,
  exceeds it. The unrolling happens before the storage level is consulted, so
  MEMORY_AND_DISK does not help: the "and disk" half decides where a finished
  block goes, not where it is built.
""")
    big_cfg = job.Config(table=cfg.table, dim=cfg.dim, batches=(big,))
    print(f"  attempting MEMORY_AND_DISK on {big} "
          f"({gb(big_cfg.input_bytes_on_disk())} on disk)...")
    e = enriched(spark, big_cfg).persist(StorageLevel.MEMORY_AND_DISK)
    try:
        t = time.perf_counter()
        n = e.count()
        print(f"  it fit: {n:,} rows cached in {time.perf_counter() - t:.1f}s")
        rdds = storage_report(spark)
        if rdds:
            r = rdds[-1]
            print(f"  memory {gb(r.get('memoryUsed', 0))}  "
                  f"disk {gb(r.get('diskUsed', 0))}  "
                  f"partitions cached {r.get('numCachedPartitions')}"
                  f"/{r.get('numPartitions')}")
    except Exception as exc:
        print(f"  it did not fit: {type(exc).__name__}")
        print(f"  {str(exc)[:400]}")
    finally:
        try:
            e.unpersist(blocking=True)
        except Exception:
            pass
    print("""
  The point is not that 4 GB is too small. It is that cache() replaces
  gradual slowdown with a hard limit, at a data size that is not written
  down anywhere. An uncached job reading 90 GB is slow; a cached one does
  not finish.
""")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batches", default="b0003",
                   help="a slice small enough that all four levels complete")
    p.add_argument("--big", default="b0000",
                   help="a slice big enough to not fit, for experiment 4")
    p.add_argument("--only", default="")
    args = p.parse_args()

    cfg = job.Config(batches=tuple(b for b in args.batches.split(",") if b),
                     tag="day13")
    banner("day 13 - caching and persistence")
    print(f"  input : {cfg.describe()}")
    print(f"          {gb(cfg.input_bytes_on_disk())} on disk")
    spark = job.session_for(cfg, app="gbif-day13")

    chosen = args.only or "1234"
    if "1" in chosen:
        exp_how_many_readers(spark, cfg)
    if "2" in chosen:
        exp_levels(spark, cfg)
    if "3" in chosen:
        exp_break_even(spark, cfg)
    if "4" in chosen and args.big:
        exp_does_not_fit(spark, cfg, args.big)

    banner("conclusions")
    print("""  The default is cache=none.

  The enriched table was cached because seven stages read it. Three problems
  with that:

  1. The cached thing is cheap - a parquet scan plus a broadcast join, with
     no shuffle and nothing to amortise.
  2. A cache cannot prune columns per reader. Uncached, the headline query
     reads three columns; cached, every reader pays for all sixteen.
  3. It does not degrade, it fails. Above ~1 GB, building the cache OOMs the
     driver, in the one stage that was added to make the job faster.

  The other direction: on the smallest batch the full job IS faster with the
  cache (50s vs 67s), because it makes many more passes than the seven
  consumers above. That window is 19 MB wide.

  --cache memory_and_disk stays available so the comparison can be re-run and
  because the answer differs on a machine with more memory.

  Cache what was expensive to compute, not what is read often, and check
  "% cached" afterwards.""")
    try:
        spark.stop()
    except Exception:
        pass
    print("\ndone.")


if __name__ == "__main__":
    main()
