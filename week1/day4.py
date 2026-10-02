"""Day 4 - execution plans and partitions.

Same 2 GB slice as days 2-3. explain() reads no data, so those plans are the
same ones the full 266 GB would give; only timings change. Sections marked SCAN
actually execute - they stay cheap by asking for one or two columns only.

Run: uv run python -m week1.day4
     FULL=1 uv run python -m week1.day4          # all 9,898 shards
     HOLD_FOR_UI=1 uv run python -m week1.day4   # keep session alive for :4040
"""
import os
import time

from pyspark.sql import functions as F

import gbif

SLICE_GB = float(os.environ.get("SLICE_GB", 2.0))
FULL = os.environ.get("FULL") == "1"
HOLD_FOR_UI = os.environ.get("HOLD_FOR_UI") == "1"
SHUFFLE_PARTITIONS = 24


def banner(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def parts_of(d):
    """Partition count. Free with AQE off. With AQE on it executes the query,
    because AQE cannot pick the final plan without running the stages first."""
    return d.rdd.getNumPartitions()


def timed(label, fn):
    t0 = time.perf_counter()
    out = fn()
    print(f"  [{time.perf_counter() - t0:6.1f}s] {label}")
    return out


spark = gbif.spark_session(app="gbif-day4", shuffle_partitions=SHUFFLE_PARTITIONS)
spark.sparkContext.setLogLevel("ERROR")
conf = spark.conf

if FULL:
    df = gbif.read_snapshot(spark)
    scope = f"full snapshot, {gbif.snapshot_size_gb():.0f} GB"
else:
    df = spark.read.parquet(*gbif.pick_slice(target_gb=SLICE_GB))
    scope = f"{SLICE_GB:g} GB slice  (FULL=1 for all {gbif.snapshot_size_gb():.0f} GB)"

banner("1. partitions at read")
print(f"scope                             : {scope}")
print(f"shards in snapshot                : {len(gbif.list_shards()):,}")
print(f"partitions after read             : {parts_of(df)}")
print(f"spark.sql.files.maxPartitionBytes : {conf.get('spark.sql.files.maxPartitionBytes')}")
print(f"spark.sql.files.openCostInBytes   : {conf.get('spark.sql.files.openCostInBytes')}")
print(f"defaultParallelism                : {spark.sparkContext.defaultParallelism}")

print("\nSCAN: rows per partition (one int column)")
rpp = timed("groupBy(spark_partition_id()).count()", lambda: sorted(
    (r["count"] for r in
     df.select("year").withColumn("pid", F.spark_partition_id())
       .groupBy("pid").count().collect()), reverse=True))
print(f"\n  rows           : {sum(rpp):,}")
print(f"  partitions     : {len(rpp):,}")
print(f"  rows/partition : min {min(rpp):,} / median {rpp[len(rpp) // 2]:,} / max {max(rpp):,}")
print(f"  spread         : {max(rpp) / min(rpp):.1f}x")
for i, c in enumerate(rpp[:8]):
    print(f"    {i:>2}. {c:>12,}  {'#' * int(round(40 * c / rpp[0]))}")

banner("2. narrow plan")
narrow = (df
          .select("gbifid", "species", "countrycode", "year", "basisofrecord")
          .where(F.col("year") >= 2000)
          .where(F.col("countrycode").isNotNull()))
narrow.explain()
print(f"\npartitions before filter : {parts_of(df)}")
print(f"partitions after filter  : {parts_of(narrow)}")

banner("3. wide plan")
agg = narrow.groupBy("countrycode").agg(F.count("*").alias("records"))
agg.explain()

banner("4. shuffle partitions and AQE  (SCAN)")
print(f"spark.sql.shuffle.partitions : {conf.get('spark.sql.shuffle.partitions')}")
print(f"spark.sql.adaptive.enabled   : {conf.get('spark.sql.adaptive.enabled')}\n")


def country_counts():
    """Fresh plan per call. Needed because explain() memoises the physical plan
    (so a later conf change does nothing) and Dataset.rdd is a lazy val (so the
    same DataFrame answers twice from cache)."""
    return (df.select("countrycode", "year")
              .where(F.col("year") >= 2000)
              .where(F.col("countrycode").isNotNull())
              .groupBy("countrycode")
              .agg(F.count("*").alias("records")))


conf.set("spark.sql.adaptive.enabled", "false")
static = timed("AQE off: partitions after groupBy",
               lambda: parts_of(country_counts()))
print(f"           -> {static}")

conf.set("spark.sql.adaptive.enabled", "true")
adaptive = timed("AQE on:  partitions after groupBy",
                 lambda: parts_of(country_counts()))
print(f"           -> {adaptive}")

banner("5. skew  (SCAN)")
# F.hash is the same Murmur3 as HashPartitioning, so placing the per-country
# counts by hand gives the layout Spark would actually produce - without
# shuffling the data to find out
counts = timed("groupBy(countrycode).count()", lambda: agg.cache().collect())
placed = (agg
          .withColumn("pid", F.pmod(F.hash("countrycode"), F.lit(SHUFFLE_PARTITIONS)))
          .groupBy("pid")
          .agg(F.sum("records").alias("rows"),
               F.count("*").alias("countries"),
               F.max_by("countrycode", "records").alias("biggest"))
          .orderBy(F.desc("rows"))
          .collect())
tot = sum(r["rows"] for r in placed)
print(f"\n  {len(counts)} country codes -> {SHUFFLE_PARTITIONS} partitions")
print(f"  {'pid':>4}{'rows':>16}{'share':>8}{'countries':>11}  heaviest")
for r in placed[:8]:
    print(f"  {r['pid']:>4}{r['rows']:>16,}{100 * r['rows'] / tot:>7.1f}%"
          f"{r['countries']:>11}  {r['biggest']}")
hi, med = placed[0]["rows"], placed[len(placed) // 2]["rows"]
print(f"  ... max {hi:,} / median {med:,} -> {hi / med:.1f}x")
agg.unpersist()   # so section 8 explains the file scan, not InMemoryTableScan

banner("6. repartition vs coalesce")
conf.set("spark.sql.adaptive.enabled", "false")   # keeps parts_of free
base = parts_of(narrow)
down, up = max(1, base // 3), base * 3
print(f"{'start':<22}: {base:,}")
print(f"{f'coalesce({down})':<22}: {parts_of(narrow.coalesce(down)):,}")
print(f"{f'repartition({down})':<22}: {parts_of(narrow.repartition(down)):,}")
print(f"{f'repartition({up})':<22}: {parts_of(narrow.repartition(up)):,}")
print(f"{f'coalesce({up})':<22}: {parts_of(narrow.coalesce(up)):,}   <- cannot go up\n")
narrow.coalesce(down).explain()
print()
narrow.repartition(down).explain()
conf.set("spark.sql.adaptive.enabled", "true")

banner("7. join strategy")
REGIONS = [("US", "North America"), ("CA", "North America"), ("MX", "North America"),
           ("GB", "Europe"), ("FR", "Europe"), ("DE", "Europe"), ("SE", "Europe"),
           ("AU", "Oceania"), ("NZ", "Oceania"), ("BR", "South America"),
           ("ZA", "Africa"), ("JP", "Asia"), ("IN", "Asia")]
regions = spark.createDataFrame(REGIONS, ["countrycode", "region"])
print(f"autoBroadcastJoinThreshold : {conf.get('spark.sql.autoBroadcastJoinThreshold')}")
print(f"rows in lookup table       : {len(REGIONS)}\n")
narrow.join(regions, "countrycode").explain()
print("\nsame join with F.broadcast():\n")
narrow.join(F.broadcast(regions), "countrycode").explain()

banner('8. explain("cost")')
agg.explain(mode="cost")

banner("9. notes")
print(f"""findings
  - partitions at read come from file SIZES, decided from the listing before
    anything is read; fewer partitions than files because Spark bin-packs them
  - {max(rpp) / min(rpp):.1f}x row spread across partitions before any shuffle ran. Spark
    balances BYTES, but parquet compresses shards differently, so equal bytes
    is not equal rows - and rows are what cost time
  - a filter never changes the partition count, it only empties partitions,
    unevenly
  - Exchange in a plan = shuffle. HashAggregate appears twice (partial below
    the Exchange, final above) so only subtotals cross the network
  - AQE off gives exactly shuffle.partitions ({static}); AQE on coalesced to
    {adaptive}, and had to read data to decide that
  - partition {placed[0]['pid']} holds {100 * hi / tot:.0f}% of rows because US lands in it. A stage ends
    when its slowest task ends, so that fraction is effectively single-threaded
  - a {len(REGIONS)}-row lookup did NOT broadcast: createDataFrame over a Python list is
    RDD-backed, Spark has no size statistics for it, so it assumed infinity and
    chose SortMergeJoin. F.broadcast() fixes it. The query was correct the whole
    time, just permanently slow

gotchas hit
  - explain() memoises the physical plan, so changing a conf afterwards does
    nothing to that DataFrame
  - Dataset.rdd is a lazy val, so measuring the same DataFrame twice returns a
    cached answer

questions
  - is F.broadcast() on small lookups standard practice, or is there a way to
    give Spark real statistics?
  - shuffle.partitions is 24 in gbif.py - should it scale with data size?
  - for skew like section 5, do we salt keys or rely on AQE?

to try
  - FULL=1 and see which numbers change; anything unchanged came from the plan
  - Spark UI :4040 SQL tab for shuffle read/write bytes and per-task times""")

if HOLD_FOR_UI:
    input("\nSpark UI on http://localhost:4040 - Enter to stop...")

spark.stop()
print("\ndone.")
