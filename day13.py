"""Week 3, day 4 (day 13) - caching and persistence, and what they cost.

`cache()` is the first thing anyone reaches for and the last thing anyone
measures. Day 9 put a `.cache()` on the enriched table for the obvious reason -
seven stages read it, so surely it should only be computed once - and never
checked. This day checks.

WHAT CACHING ACTUALLY DOES. A DataFrame is a recipe, not a table. Every action
re-runs the recipe from the files. `persist()` says: the first time you compute
this, keep the result, and serve later reads from the kept copy. So the deal is

    pay  : the cost of materialising and storing it, once
    save : the cost of recomputing it, on every read after the first

and caching wins only when `save x (readers - 1)` is bigger than `pay`. That is
arithmetic, and it is the arithmetic nobody does.

THE STORAGE LEVELS, and what they really trade:

  MEMORY_ONLY       Spark's columnar cache format, in the JVM heap. Fastest to
                    read. If a partition does not fit, it is simply NOT cached
                    and gets recomputed silently - a cache that is 40% cached
                    is 60% of the original work plus all of the storage cost.
  MEMORY_AND_DISK   same, but partitions that do not fit are written to local
                    disk instead of dropped. The usual advice, and the default
                    this project shipped with.
  DISK_ONLY         always to disk. Slower to read than memory, much faster
                    than recomputing if the recipe is expensive.

The one everyone is surprised by: building the in-memory cache is not free
memory-wise either. Each task unrolls its partition into the columnar format
before it can be stored, and that unrolling happens in the SAME heap the query
is using. On this laptop that is where the job died - see experiment 4.

    uv run python day13.py
    uv run python day13.py --batches b0003 --big b0000

Experiments:
  1  how many times does the job read the enriched table, really
  2  no cache vs the three storage levels, end to end
  3  the break-even: how many readers before caching pays
  4  when the cache does not fit (the failure that started this day)
"""
import argparse
import time

from pyspark import StorageLevel
from pyspark.sql import functions as F

import bench
import curate
import job
from bench import banner, gb, measure

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


# --- 1 ----------------------------------------------------------------------
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
  {len(rows)} consumers, {gb(total_read)} read in total, {gb(once)} per pass.
  With nothing cached the table is scanned {len(rows)} times - which is the
  case FOR caching, and the only honest reason to consider it.

  Note the per-consumer read is not identical: column pruning means a consumer
  that touches three columns reads less than one that touches eight. Caching
  destroys that - the cache holds whatever columns the cached DataFrame had,
  for every reader. That is a real cost and it never appears in the tutorials.
""")
    return rows


# --- 2 ----------------------------------------------------------------------
def exp_levels(spark, cfg):
    banner("2. no cache vs the three storage levels, end to end")
    print("""  Same seven consumers each time. `build` is the cost of materialising
  the cache; `consume` is the seven reads afterwards. Caching is worth it only
  if build + consume beats consume-with-nothing-cached.
""")
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
  Two columns to actually read:

  `bytes read` - caching should drive this towards one pass. If it does not,
  the cache is not being hit and something is quietly recomputing.
  `% cached`   - below 100 means partitions were evicted or never stored. At
  that point you are paying the storage cost AND the recompute cost. This is
  the failure mode that looks like a working cache.
""")
    return rows


# --- 3 ----------------------------------------------------------------------
def exp_break_even(spark, cfg):
    banner("3. the break-even - how many readers before caching pays?")
    print("""  One scan, timed. One cache build, timed. Then the arithmetic, which
  is the entire decision and takes one line.
""")
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
  The general shape, worth remembering past this project: caching pays when
  the thing being cached was EXPENSIVE to produce - a shuffle, a join, a UDF,
  a wide filter. It does not pay for a cheap scan, because parquet + column
  pruning is already fast and the cache cannot prune columns per reader.

  The job's enriched table is a broadcast join over a parquet scan. There is
  no shuffle in it. That is why this number comes out the way it does.

  AND THE CAVEAT, because this number is weaker than it looks. On a slice
  small enough for all four storage levels to complete, one pass costs a
  fraction of a second, which is the same order as the measurement noise -
  so "saved per read" is being computed from a difference the harness cannot
  really resolve, and the break-even figure above should be read as "too
  small to matter" rather than as a precise count.

  The obvious fix is to measure on a bigger slice. That is exactly what
  cannot be done: on a bigger slice the cache OOMs (experiment 4). There is
  no slice on this machine where the cache both fits AND is large enough for
  the saving to be measurable. That is not a gap in the experiment - it IS
  the finding, and it is the reason the default changed.
""")
    return {"scan_s": scan_s, "cached_s": cached_s, "build_s": build_s,
            "break_even": break_even}


# --- 4 ----------------------------------------------------------------------
def exp_does_not_fit(spark, cfg, big):
    banner("4. when the cache does not fit")
    print(f"""  Experiments 1-3 used {','.join(cfg.batches) or 'the whole table'}, which fits. This one uses
  {big}, which does not, because that is how this day started: the job as
  written on day 9 did not get slower at scale, it DIED.

      py4j.protocol.Py4JJavaError ...
      Caused by: java.lang.OutOfMemoryError: Java heap space
        at ... stage_enrich -> apply_cache -> df.count()

  The arithmetic: the driver JVM has {cfg.driver_memory}. Spark gives roughly 60% of that
  to execution and storage combined. Caching N million rows x 16 columns in
  the columnar in-memory format, with 12 tasks unrolling their partitions into
  the same heap at the same time, exceeds it - and unrolling happens BEFORE
  the storage level gets a say, so MEMORY_AND_DISK does not save you. The
  "and disk" half only decides where a finished block goes, not where it is
  built.
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
  The lesson is not "4 GB is too small". It is that `cache()` turns a job that
  degrades gracefully into one with a hard cliff, and the cliff is at a data
  size nobody writes down. An uncached job reading 90 GB is slow. A cached one
  is dead. Given the choice, prefer the one that finishes.
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

    banner("what day 13 changes about the job")
    print("""  The default changes from cache=memory_and_disk to cache=none.

  Day 9 cached because seven stages read the enriched table and "compute it
  once" sounded obviously right. Three things were wrong with that:

  1. The thing being cached is cheap. It is a parquet scan plus a broadcast
     join - no shuffle, no UDF, nothing to amortise. Re-reading parquet with
     column pruning is close to the fastest thing this machine does.
  2. The cache cannot prune columns per reader. Uncached, the headline query
     reads three columns; cached, every reader pays for all sixteen.
  3. It does not degrade, it fails. At 1.1 GB and above, building the cache
     OOMs the driver. The job did not get slower at scale, it stopped
     running - and it stopped running in the one stage that was added to
     make it faster.

  One result that argues the other way, and belongs here rather than in a
  footnote: on the 19 MB toy batch, day 14 measures the FULL job as faster
  with the cache than without (50s vs 67s). That is not a contradiction. The
  full job makes far more passes over the enriched table than the seven
  consumers above, so at a size where the cache fits comfortably it does pay.
  The window where caching the fact table helps is 19 MB wide, and 19 MB is a
  size at which nothing about this job matters.

  --cache memory_and_disk stays available, because the comparison has to stay
  re-runnable, because the answer is different on a machine with enough
  memory, and because that toy-batch result deserves to stay reproducible. It
  is just not the default any more.

  The rule to take away: cache what was expensive to compute, not what is
  read often. And check the Storage tab's "% cached" afterwards, every time -
  a 40%-cached table is the worst of all worlds and looks exactly like
  success.""")
    try:
        spark.stop()
    except Exception:
        # experiment 4 can leave the JVM in a bad way on purpose. Failing to
        # shut down cleanly after that is not a result worth a traceback.
        pass
    print("\ndone.")


if __name__ == "__main__":
    main()
