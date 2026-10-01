"""Week 3, day 2 (day 11) - partitioning, in and out.

Day 10 said where the time goes. This day is about the knob underneath most of
it: how many pieces the data is cut into, at three different moments.

Those three moments get confused constantly, so they are named here once and
kept apart for the rest of the file:

  INPUT partitions   how many tasks read the files. Spark decides this by
                     bin-packing files into `spark.sql.files.maxPartitionBytes`
                     (default 128 MB). You influence it with that setting and
                     with how big you wrote the files in the first place.
  SHUFFLE partitions how many pieces a wide operation redistributes into.
                     `spark.sql.shuffle.partitions`, default 200, ours 48.
                     AQE can coalesce them down afterwards.
  OUTPUT partitions  how many files get written. One per task that reaches the
                     write, which is what `repartition` / `coalesce` control.

And one thing that is NOT a partition in that sense: `partitionBy("decade")` at
write time makes *directories*. It is a storage layout, and it only pays off if
a later query filters on that column. The curated table is laid out
`ingest_batch=.../decade=.../`, so experiment 2 asks the obvious question -
does the job this week is optimising actually use it?

    uv run python day11.py
    uv run python day11.py --batches b0000,b0003

Experiments:
  1  input partitioning: maxPartitionBytes, task count, and the small files
  2  partition pruning: which filters reach the directory listing
  3  shuffle partitioning: 12 / 48 / 200 / 800, with AQE and without
  4  output partitioning: repartition vs coalesce vs neither, before a write
"""
import argparse
import collections
import pathlib
import shutil

from pyspark.sql import functions as F

import bench
import curate
import job
from bench import banner, gb, measure

SCRATCH = pathlib.Path(__file__).parent / "data" / "scratch" / "day11"

# Rule of thumb everyone quotes and nobody sources: aim for a shuffle partition
# of roughly this size. Written down so experiment 3's conclusion is a
# comparison against a stated target, not a vibe.
TARGET_SHUFFLE_PARTITION_BYTES = 128 * 1024**2


def file_sizes(table, batches):
    root = pathlib.Path(table)
    dirs = [root / f"ingest_batch={b}" for b in batches] if batches else [root]
    return sorted(f.stat().st_size for d in dirs for f in d.rglob("*.parquet"))


def histogram(sizes):
    """How big are the files actually? The small-file problem is invisible in
    a total and obvious in a histogram."""
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


# --- 1 ----------------------------------------------------------------------
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
    print(f"""
  Spark packs these files into read tasks up to maxPartitionBytes, adding
  `spark.sql.files.openCostInBytes` (4 MB) per file as a charge for opening it.
  That open cost is why 1000 tiny files are not free: 1000 x 4 MB of imaginary
  bytes dominate the packing, and you get tasks that are mostly overhead.
""")

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
    print(f"""
  Read it as: partitions is the parallelism you are allowed. Below the number
  of cores ({spark.sparkContext.defaultParallelism}) you are leaving the machine idle; far above it you are
  paying task launch overhead for nothing. The total bytes read barely moves,
  because the same bytes are being read either way - only the shape changes.
""")
    return rows


# --- 2 ----------------------------------------------------------------------
def exp_pruning(spark, cfg):
    banner("2. partition pruning - which filters reach the directory listing")
    print("""  The table is laid out ingest_batch=.../decade=.../. A filter on a
  partition column is answered by NOT LISTING directories - the files are never
  opened. A filter on any other column still has to open every file and look.

  `files` below is the proof. It is files opened, from the Scan operator's own
  metric, not from a plan string.
""")
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
  Three different mechanisms, worth keeping straight:
    PartitionFilters - directories not listed. Biggest win, needs the column
                       to be in the path.
    PushedFilters    - the predicate handed to the parquet reader, which skips
                       row GROUPS whose min/max cannot match. Files are still
                       opened; `files` does not move, time does.
    neither          - read everything, filter in Spark.
""")
    return rows


# --- 3 ----------------------------------------------------------------------
def exp_shuffle_partitions(spark, cfg):
    banner("3. shuffle partitioning - 12 / 48 / 200 / 800")
    print("""  One groupBy, four settings, AQE on and off. AQE on is the honest
  default (it is on in the job), but it coalesces small shuffle partitions
  afterwards, which hides the setting. So: both.
""")
    # deliberately NOT cached: day 13 is the day that explains why caching a
    # parquet scan on this machine is a bad trade, and doing it here would be
    # measuring the cache rather than the shuffle.
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
  The arithmetic nobody does: this groupBy shuffles {gb(shuffled)}. At the
  {gb(TARGET_SHUFFLE_PARTITION_BYTES)} per partition everyone recommends, that is {ideal} partition(s).
  The job is configured for {cfg.shuffle_partitions}. With AQE off, the extra partitions are
  {cfg.shuffle_partitions} near-empty tasks; with AQE on, Spark coalesces them back down and the
  setting stops mattering - which is exactly what the `tasks` column shows.

  The real lesson: shuffle.partitions is only worth tuning when the shuffle is
  big. Here it is not, and the honest answer is "leave AQE on and stop
  thinking about it".
""")
    return rows


# --- 4 ----------------------------------------------------------------------
def exp_output_partitioning(spark, cfg):
    banner("4. output partitioning - repartition vs coalesce vs neither")
    print("""  One output written four ways. The read-back column matters as much
  as the write column: a write that is fast because it made 500 tiny files
  charges the cost to whoever reads it next.
""")
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
  What each row is actually doing:

  as-is        one file per input task. Fastest write, because nothing moves.
               File count is whatever the input happened to be, which is how
               tables end up with thousands of small files nobody chose.
  coalesce(4)  merges partitions without a shuffle - `write shuffle` is 0.
               The catch is that it reaches BACKWARDS: the whole upstream
               computation now runs on 4 tasks, not just the write. Free when
               the upstream is trivial, a serious regression when it is not.
  repartition(4)  a real shuffle, visible in `write shuffle`. Costs more, but
               keeps upstream parallelism and gives evenly sized files. Note
               the `size` column: the output is BIGGER than the other layouts.
               repartition() distributes rows at random, which destroys the
               clustering the rows arrived in - and parquet compresses a
               column far better when similar values sit next to each other.
               Shuffling for even file sizes is not free; you pay for it in
               compression. This is the same mechanism, inverted, as the
               sortWithinPartitions() the curated table is written with.
  repartition(decade) + partitionBy  the layout the curated table actually
               uses. The repartition is not optional: without it every task
               writes into every decade directory and you get
               tasks x decades files. Read-back with a decade filter opens a
               fraction of the files - that is the payoff, and it only ever
               arrives if queries filter on decade.
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

    banner("what day 11 changes about the job")
    print("""  1. INPUT. Not what I expected, and worth stating carefully. The file
     COUNT is dominated by small files - half of them are under 1 MB - but
     the BYTES are not: ten files of 64-128 MB hold 80% of the slice. So the
     default 128 MB packing produces 15 read tasks for 12 cores, which is
     the right shape, and dropping to 32 MB only buys 43 tasks that each do
     less while the wall time gets worse. Nothing to change.

     The small files are still a real cost - they are `decade` directories
     with very few rows, one per batch - and `curate.py compact` is the fix
     that already exists. They are just not what is limiting this job.

  2. PRUNING. The job reads the whole table and filters nothing, so the
     decade partitioning buys IT nothing - it was written for the ad-hoc
     queries in days 5-8 and for day 15's analysis. Being honest about that
     matters: partitioning is not free, it costs directories and small files,
     and a layout that no query uses is pure cost.

  3. SHUFFLE. The job's shuffles are kilobytes, not gigabytes, because every
     aggregate reduces hard at the map side. 48 shuffle partitions is far too
     many for that, and AQE is already coalescing them away. Verdict: leave
     AQE on, stop tuning shuffle.partitions, and do not claim a win here.

  4. OUTPUT. The results are kilobytes, so coalesce(1) is right and the
     "small file problem" argument does not apply - one small file is the
     correct number of files for a 6 KB table. The thing to avoid is
     coalesce() on a path where the upstream work is expensive.

  The honest summary of day 11: partitioning is NOT where this job's time
  goes. Days 10 and 13 say where it goes. Writing that down is the point of
  measuring first.""")
    spark.stop()
    print("\ndone.")


if __name__ == "__main__":
    main()
