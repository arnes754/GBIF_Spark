"""Partitioning experiments: input, shuffle and output.

Three different things called partitioning:

  input    how many tasks read the files, set by bin-packing files into
           spark.sql.files.maxPartitionBytes (default 128 MB)
  shuffle  how many pieces a wide operation redistributes into,
           spark.sql.shuffle.partitions, then AQE coalescing
  output   how many files get written, controlled by repartition/coalesce

partitionBy() at write time is a fourth thing: it makes directories, and only
pays off if later queries filter on that column.

    uv run python -m gbif_spark day 11
    uv run python -m gbif_spark day 11 --batches b0000,b0003
    uv run python -m gbif_spark day 11 --only 3

  1  input partitioning: maxPartitionBytes, task count, file sizes
  2  partition pruning: which filters reach the directory listing
  3  shuffle partitions: 12 / 48 / 200 / 800, with AQE and without
  4  output: repartition vs coalesce vs neither, before a write
"""
import argparse
import collections
import pathlib
import shutil

from pyspark.sql import functions as F

from gbif_spark import paths
from gbif_spark.helpers import bench
from gbif_spark.helpers.bench import banner, gb, measure
from gbif_spark.pipeline import curate, job

SCRATCH = paths.SCRATCH / "day11"

TARGET_SHUFFLE_PARTITION_BYTES = 128 * 1024**2


def file_sizes(table, batches):
    root = pathlib.Path(table)
    dirs = [root / f"ingest_batch={b}" for b in batches] if batches else [root]
    return sorted(f.stat().st_size for d in dirs for f in d.rglob("*.parquet"))


def histogram(sizes):
    """File size histogram. A total hides the small-file problem."""
    buckets = [("< 1 MB", 0, 1024**2), ("1-16 MB", 1024**2, 16 * 1024**2),
               ("16-64 MB", 16 * 1024**2, 64 * 1024**2),
               ("64-128 MB", 64 * 1024**2, 128 * 1024**2),
               ("> 128 MB", 128 * 1024**2, float("inf"))]
    rows = []
    for name, lo, hi in buckets:
        chunk = [s for s in sizes if lo <= s < hi]
        rows.append({"bucket": name, "files": len(chunk),
                     "bytes": sum(chunk),
                     "share": 100 * len(chunk) / max(len(sizes), 1)})
    return rows


def exp_input_partitioning(spark, cfg):
    banner("1. input partitioning - how many tasks read the files")
    sizes = file_sizes(cfg.table, cfg.batches)
    print(f"  {len(sizes)} parquet files, {gb(sum(sizes))} total, "
          f"median {gb(sizes[len(sizes) // 2])}\n")
    bench.show(histogram(sizes), [
        ("file size", "bucket", bench.TXT),
        ("files", "files", bench.NUM),
        ("bytes", "bytes", bench.BYTES),
        ("% of files", "share", bench.PCT),
    ])
    print("\n  Spark packs files into read tasks up to maxPartitionBytes and"
          "\n  charges spark.sql.files.openCostInBytes (4 MB) per file on top."
          "\n  That open cost is why many small files pack badly.\n")

    rows = []
    for mb in (32, 128, 512):
        spark.conf.set("spark.sql.files.maxPartitionBytes", str(mb * 1024**2))
        df = curate.read_table(spark, cfg.table, cfg.batches or None)
        parts = df.rdd.getNumPartitions()
        with measure(spark, f"maxPartitionBytes={mb}MB") as m:
            df.select(F.sum("n_issues")).collect()
        rows.append({**m, "mb": mb, "input_partitions": parts})
    spark.conf.set("spark.sql.files.maxPartitionBytes",
                   str(job.DEFAULT_MAX_PARTITION_BYTES))

    bench.show(rows, [
        ("maxPartitionBytes", "mb", lambda v: f"{int(v)} MB"),
        ("input partitions", "input_partitions", bench.NUM),
        ("tasks", "tasks", bench.NUM),
        ("secs", "seconds", bench.SEC),
        ("read", "input_bytes", bench.BYTES),
        ("task secs", "task_ms", lambda v: f"{v / 1000:.0f}"),
    ])
    print(f"\n  Partitions is the parallelism available. Below the core count"
          f"\n  ({spark.sparkContext.defaultParallelism}) the machine idles; far above it the task launch"
          f"\n  overhead is wasted. Bytes read barely move either way.\n")
    return rows


def exp_pruning(spark, cfg):
    banner("2. partition pruning - which filters reach the directory listing")
    print("  The table is laid out ingest_batch=.../decade=.../. A filter on\n"
          "  a partition column skips the directory listing, so the files are\n"
          "  never opened. Any other filter still opens every file.\n\n"
          "  `files` is files opened, from the Scan operator's own metric.\n")
    rows = []
    cases = [
        ("no filter", lambda d: d),
        ("decade = 2010  (partition column)", lambda d: d.where(F.col("decade") == 2010)),
        ("decade >= 2000 (partition column)", lambda d: d.where(F.col("decade") >= 2000)),
        ("year = 2015    (regular column)", lambda d: d.where(F.col("year") == 2015)),
        ("country = 'US' (regular column)", lambda d: d.where(F.col("country") == "US")),
    ]
    for label, f in cases:
        df = f(curate.read_table(spark, cfg.table, cfg.batches or None))
        with measure(spark, label) as m:
            n = df.count()
        stats = bench.scan_stats(df)
        rows.append({**m, "rows": n,
                     "pushed": stats["PushedFilters"][:38],
                     "partf": stats["PartitionFilters"][:38]})

    bench.show(rows, [
        ("filter", "label", bench.TXT),
        ("rows", "rows", bench.NUM),
        ("files opened", "files_read", bench.NUM),
        ("read", "input_bytes", bench.BYTES),
        ("secs", "seconds", bench.SEC),
    ])
    print()
    for r in rows:
        print(f"  {r['label']:<36} PartitionFilters {r['partf']}")
        print(f"  {'':<36} PushedFilters    {r['pushed']}")
    print("""
  Three separate mechanisms:
    PartitionFilters - directories are not listed. Needs the column in the
                       path. Biggest win.
    PushedFilters    - predicate handed to the parquet reader, which skips
                       row groups whose min/max cannot match. Files are still
                       opened, so `files` does not move but time does.
    neither          - read everything and filter in Spark.
""")
    return rows


def exp_shuffle_partitions(spark, cfg):
    banner("3. shuffle partitioning - 12 / 48 / 200 / 800")
    print("  One groupBy, four settings, AQE on and off. AQE coalesces small\n"
          "  shuffle partitions afterwards, which hides the setting, so both.\n")
    df = curate.read_table(spark, cfg.table, cfg.batches or None) \
               .select("datasetkey", "n_issues", "usable_for_mapping")

    rows = []
    for aqe in (False, True):
        for n in (12, 48, 200, 800):
            spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
            spark.conf.set("spark.sql.shuffle.partitions", str(n))
            label = f"aqe={'on ' if aqe else 'off'} shuffle.partitions={n}"
            agg = df.groupBy("datasetkey").agg(
                F.count("*").alias("n"), F.avg("n_issues").alias("f"))
            with measure(spark, label) as m:
                agg.count()
            rows.append({**m, "setting": n, "aqe": "on" if aqe else "off"})
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.shuffle.partitions", str(cfg.shuffle_partitions))

    bench.show(rows, [
        ("aqe", "aqe", bench.TXT),
        ("shuffle.partitions", "setting", bench.NUM),
        ("secs", "seconds", bench.SEC),
        ("shuffle w", "shuffle_write_bytes", bench.BYTES),
        ("spill", "disk_spill_bytes", bench.BYTES),
        ("tasks", "tasks", bench.NUM),
        ("task secs", "task_ms", lambda v: f"{v / 1000:.0f}"),
    ])

    shuffled = max((r["shuffle_write_bytes"] for r in rows), default=0)
    ideal = max(1, round(shuffled / TARGET_SHUFFLE_PARTITION_BYTES))
    print(f"""
  This groupBy shuffles {gb(shuffled)}. At the usual {gb(TARGET_SHUFFLE_PARTITION_BYTES)} per partition
  that is {ideal} partition(s); the job is configured for {cfg.shuffle_partitions}. With AQE off
  the rest are near-empty tasks, and with AQE on they get coalesced away,
  which is what the tasks column shows.

  shuffle.partitions is only worth tuning when the shuffle is large.
""")
    return rows


def exp_output_partitioning(spark, cfg):
    banner("4. output partitioning - repartition vs coalesce vs neither")
    print("  One output written four ways. The read-back column matters as\n"
          "  much as the write column: a fast write that leaves hundreds of\n"
          "  small files charges the cost to the next reader.\n")
    if SCRATCH.exists():
        shutil.rmtree(SCRATCH)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    src = (curate.read_table(spark, cfg.table, cfg.batches or None)
           .select("gbifid", "datasetkey", "decade", "country", "n_issues",
                   "usable_for_mapping"))
    n_in = src.rdd.getNumPartitions()
    print(f"  source has {n_in} input partitions\n")

    layouts = [
        ("as-is (one file per task)", lambda d: d, {}),
        ("coalesce(4)", lambda d: d.coalesce(4), {}),
        ("repartition(4)", lambda d: d.repartition(4), {}),
        ("repartition(decade) + partitionBy", lambda d: d.repartition("decade"),
         {"partitionBy": "decade"}),
    ]
    rows = []
    for label, shape, opts in layouts:
        path = SCRATCH / label.split("(")[0].strip().replace(" ", "_")
        with measure(spark, f"write {label}") as m:
            w = shape(src).write.mode("overwrite").option("compression", "zstd")
            if opts.get("partitionBy"):
                w = w.partitionBy(opts["partitionBy"])
            w.parquet(str(path))
        files = list(path.rglob("*.parquet"))
        with measure(spark, f"read back {label}") as r:
            spark.read.parquet(str(path)).where(F.col("decade") == 2010).count()
        rows.append({"layout": label, "write_s": m["seconds"],
                     "write_shuffle": m["shuffle_write_bytes"],
                     "files": len(files),
                     "bytes": sum(f.stat().st_size for f in files),
                     "readback_s": r["seconds"],
                     "readback_files": r["files_read"]})

    bench.show(rows, [
        ("layout", "layout", bench.TXT),
        ("write s", "write_s", bench.SEC),
        ("write shuffle", "write_shuffle", bench.BYTES),
        ("files out", "files", bench.NUM),
        ("size", "bytes", bench.BYTES),
        ("read-back s", "readback_s", bench.SEC),
        ("files opened", "readback_files", bench.NUM),
    ])
    print("""
  What each row does:

  as-is        one file per input task, no shuffle. Fastest write. The file
               count is whatever the input happened to be.
  coalesce(4)  merges partitions without a shuffle, so write shuffle is 0.
               It reaches backwards though: the whole upstream computation
               now runs on 4 tasks, not just the write.
  repartition(4)  a real shuffle. Keeps upstream parallelism and gives even
               file sizes, but the output is larger - repartition spreads
               rows at random, and parquet compresses better when similar
               values sit together.
  repartition(decade) + partitionBy  what the curated table uses. The
               repartition is not optional: without it every task writes
               into every decade directory. Reading back with a decade
               filter then opens a fraction of the files.
""")
    shutil.rmtree(SCRATCH, ignore_errors=True)
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--batches", default="b0000")
    p.add_argument("--only", default="", help="run one experiment: 1, 2, 3 or 4")
    args = p.parse_args()

    cfg = job.Config(batches=tuple(b for b in args.batches.split(",") if b),
                     tag="day11")
    banner("day 11 - partitioning")
    print(f"  input : {cfg.describe()}")
    print(f"          {gb(cfg.input_bytes_on_disk())} on disk")
    spark = job.session_for(cfg, app="gbif-day11")

    chosen = args.only or "1234"
    if "1" in chosen:
        exp_input_partitioning(spark, cfg)
    if "2" in chosen:
        exp_pruning(spark, cfg)
    if "3" in chosen:
        exp_shuffle_partitions(spark, cfg)
    if "4" in chosen:
        exp_output_partitioning(spark, cfg)

    banner("conclusions")
    print("""  INPUT. Half the files are under 1 MB but ten files of 64-128 MB
  hold 80% of the bytes, so the default 128 MB packing gives 15 read tasks
  for 12 cores. 32 MB gives 43 smaller tasks and a worse wall time. No
  change. The small files are a real cost and `curate.py compact` fixes
  them, but they are not what limits this job.

  PRUNING. The job reads the whole table and filters nothing, so the decade
  partitioning does not help it. It is there for the ad-hoc queries and the
  analysis, and it costs directories and small files.

  SHUFFLE. The shuffles are kilobytes because every aggregate reduces on the
  map side. 48 partitions is far too many and AQE coalesces them anyway. No
  change.

  OUTPUT. The results are kilobytes, so coalesce(1) is the right number of
  files. What to avoid is coalesce() where the upstream work is expensive.

  Partitioning is not where this job's time goes.""")
    spark.stop()
    print("\ndone.")


if __name__ == "__main__":
    main()
