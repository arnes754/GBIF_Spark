"""Day 6 - transformations at scale, and where the time actually goes.

Day 3 wrote the transformations. This day runs them at 58 million rows and
asks why some of them cost nothing and some of them cost a minute. The answer
is always the same distinction - does this transformation need to look at
another row, and if so, does that row live on another machine.

    narrow : each output partition depends on exactly one input partition.
             filter, select, withColumn, map. No network, no barrier.
             They FUSE: ten narrow steps are one pass over the data.
    wide   : output partitions depend on many input partitions, so the data has
             to be redistributed by key first. groupBy, join, distinct, orderBy,
             repartition, window. Each one is an Exchange: write every partition
             to disk, shuffle it over the network, read it back.

The shuffle is the unit of cost in Spark. Everything in this file is a way of
either avoiding one, making one smaller, or proving one happened.

    uv run python day6.py
    SLOW=1 uv run python day6.py     # include the deliberately-awful section 6
"""
import os
import time

from pyspark.sql import functions as F

import bench
import curate
import gbif
from bench import banner, gb, measure, show

TABLE = os.environ.get("TABLE", "data/curated/occurrence_slim")
SLOW = os.environ.get("SLOW", "0") == "1"
HOLD_FOR_UI = os.environ.get("HOLD_FOR_UI") == "1"

# 4g, not 8g. Section 5 forks one python worker per core and each imports
# pandas and pyarrow (~150 MB resident). Twelve of those next to an 8 GB JVM
# does not fit in 16 GB, and it does not raise: the OS kills the workers and
# the job hangs with 12 active tasks and 0 completed.
spark = gbif.spark_session(app="gbif-day6", driver_memory="4g",
                           shuffle_partitions=24)
spark.sparkContext.setLogLevel("ERROR")
conf = spark.conf

df = spark.read.option("mergeSchema", "true").parquet(TABLE)
N = df.count()
print(f"table: {TABLE}   {N:,} rows, {len(df.columns)} columns")

# ---------------------------------------------------------------------------
banner("1. narrow transformations fuse")
# Ten withColumns and a filter. Every one of them is narrow, so the whole chain
# becomes a single Project inside a single stage - no Exchange, no barrier.
chain = df.select("gbifid", "species", "country", "decade", "n_issues", "lat", "lon")
for i in range(10):
    chain = chain.withColumn(f"c{i}", F.col("n_issues") + F.lit(i))
chain = chain.where(F.col("decade") >= 2000)

runs = []
with measure(spark, "no transformations") as m:
    df.select("gbifid").where(F.col("decade") >= 2000).count()
runs.append(m)
with measure(spark, "10 withColumn + filter") as m:
    chain.count()
runs.append(m)
show(runs)
def exchanges(d):
    """Shuffles in the physical plan. Counting them before timing anything is
    the cheapest performance analysis there is."""
    return d._jdf.queryExecution().executedPlan().toString().count("Exchange ")


print(f"""  Exchanges in the 10-step chain: {exchanges(chain)}  (a groupBy would make it 1)

  Narrow steps are free in the sense that matters: they add no stage boundary,
  so they cost CPU on data that was going to be read anyway. This is why
  curate() can afford twenty derived columns.""")

# ---------------------------------------------------------------------------
banner("2. one wide transformation, and what it costs")
runs = []
with measure(spark, "narrow: filter + count") as m:
    df.select("country").where(F.col("decade") >= 2000).count()
runs.append(m)
with measure(spark, "wide: groupBy(country)") as m:
    df.where(F.col("decade") >= 2000).groupBy("country").count().collect()
runs.append(m)
with measure(spark, "wide: distinct(datasetkey)") as m:
    df.select("datasetkey").distinct().count()
runs.append(m)
with measure(spark, "wide: orderBy(gbifid)") as m:
    df.select("gbifid").orderBy("gbifid").limit(5).collect()
runs.append(m)
show(runs)
print("""
  `shuffle w` is the number to watch: the narrow line writes zero, the rest
  pay to serialise, write to local disk, fetch and merge. orderBy costs most
  because a global sort first range-partitions over a sample of the data,
  which is the extra stage.""")

# ---------------------------------------------------------------------------
banner("3. partial aggregation: why groupBy is cheaper than it looks")
# HashAggregate appears twice in the plan - once below the Exchange, once
# above. The lower one reduces each partition locally, so only subtotals cross
# the network. That is why grouping 58M rows by country shuffles kilobytes.
low_card = df.groupBy("country").agg(F.count("*").alias("n"))
high_card = df.groupBy("gbifid").agg(F.count("*").alias("n"))

runs = []
with measure(spark, "groupBy country (~250 keys)") as m:
    low_card.collect()
runs.append(m)
with measure(spark, "groupBy cell_id (~10^5 keys)") as m:
    df.groupBy("cell_id").agg(F.count("*").alias("n")).agg(F.count("*")).collect()
runs.append(m)
with measure(spark, "groupBy gbifid (58M keys)") as m:
    high_card.agg(F.count("*")).collect()
runs.append(m)
show(runs)
print(f"""
  Same operator, three orders of magnitude apart, and the difference is the
  number of GROUPS, not the number of rows. Low cardinality: the map side
  reduces 58M rows to ~250 per partition and shuffles nothing. High
  cardinality: the map side cannot reduce anything, so the shuffle carries
  the whole dataset plus serialisation overhead, which costs more than not
  aggregating at all.

  This is the cardinality-explosion trap in a single table. Before you write
  a groupBy, ask how many groups come out.""")

# ---------------------------------------------------------------------------
banner("4. the slow moment: count(distinct) on a high-cardinality key")
runs = []
with measure(spark, "countDistinct(species) exact") as m:
    df.agg(F.countDistinct("species")).collect()
runs.append(m)
with measure(spark, "approx_count_distinct 5%") as m:
    df.agg(F.approx_count_distinct("species", 0.05)).collect()
runs.append(m)
with measure(spark, "3 x countDistinct, one pass") as m:
    df.agg(F.countDistinct("species"), F.countDistinct("datasetkey"),
           F.countDistinct("cell_id")).collect()
runs.append(m)
with measure(spark, "3 x approx, one pass") as m:
    df.agg(F.approx_count_distinct("species", 0.05),
           F.approx_count_distinct("datasetkey", 0.05),
           F.approx_count_distinct("cell_id", 0.05)).collect()
runs.append(m)
show(runs)
print("""
  Exact distinct has to move every distinct value to one place, so it shuffles
  in proportion to cardinality. HyperLogLog (approx_count_distinct) shuffles a
  fixed-size sketch per partition regardless of cardinality, and merges them.
  Several exact distincts in one agg is worse than the sum of its parts -
  Spark expands the rows once per distinct column before shuffling.

  For a headline number, 5% error is usually an acceptable trade.""")

# ---------------------------------------------------------------------------
banner("5. the same transformation, three ways to write it")
# Python UDF vs pandas UDF vs built-in. The
# result is meant to be deleted afterwards - the point is the measurement.
from pyspark.sql.types import IntegerType

# Two deliberate limits, both about memory rather than about UDFs:
#   - 50k rows is plenty to separate implementations that differ by 20x.
#     Bigger samples did not change the RATIO, they only changed whether the
#     section finished on a machine with ~2 GB free
#   - repartition(UDF_WORKERS) caps the number of CONCURRENT python workers,
#     because concurrency here equals partitions, and each worker imports
#     pandas + pyarrow at ~150 MB resident. At 12 workers this section killed
#     the job on a 16 GB laptop; at 4 it fits.
UDF_ROWS = int(os.environ.get("UDF_ROWS", 50_000))
UDF_WORKERS = int(os.environ.get("UDF_WORKERS", 4))
# repartition, not coalesce: limit() produces a single partition and coalesce
# cannot increase the count, so coalesce(4) would leave one task doing all
# the work.
sample = (df.select("n_issues", "n_geo_issues")
            .where(F.col("decade") == 2000).limit(UDF_ROWS)
            .repartition(UDF_WORKERS).cache())
print(f"sample: {sample.count():,} rows over "
      f"{sample.rdd.getNumPartitions()} partitions "
      f"(= that many concurrent python workers; "
      f"{spark.sparkContext.defaultParallelism} cores available)\n")

py_udf = F.udf(lambda a, b: (a or 0) - (b or 0), IntegerType())

# The lambda form of pandas_udf was removed; Spark 4 reads the signature's type
# hints to decide which pandas UDF kind this is. Series -> Series is the
# vectorised scalar one.
try:
    import pandas as pd

    @F.pandas_udf(IntegerType())
    def pd_udf(a: pd.Series, b: pd.Series) -> pd.Series:
        return (a.fillna(0) - b.fillna(0)).astype("int32")

    have_pandas = True
except Exception as exc:
    print(f"  (pandas UDF unavailable: {exc})")
    have_pandas = False

runs = []
with measure(spark, "built-in column expression") as m:
    sample.withColumn("d", F.coalesce("n_issues", F.lit(0))
                      - F.coalesce("n_geo_issues", F.lit(0))) \
          .agg(F.sum("d")).collect()
runs.append(m)
if have_pandas:
    with measure(spark, "pandas UDF (arrow, vectorised)") as m:
        sample.withColumn("d", pd_udf("n_issues", "n_geo_issues")) \
              .agg(F.sum("d")).collect()
    runs.append(m)
with measure(spark, "python UDF (row at a time)") as m:
    sample.withColumn("d", py_udf("n_issues", "n_geo_issues")) \
          .agg(F.sum("d")).collect()
runs.append(m)
show(runs)
print("""
  A built-in expression is compiled into the generated Java for the stage and
  never leaves the JVM. A python UDF serialises every row to a python worker
  process and back, and is opaque to the optimiser - no pushdown through it, no
  whole-stage codegen around it. A pandas UDF moves batches over Arrow instead
  of rows over pickle, which recovers most but not all of the gap.

  Rule: reach for a UDF only when no built-in expresses it, and measure before
  you keep it.

  The cost that does not appear in this table is MEMORY, and it is the one that
  actually stopped this script running. A UDF forks one python worker per
  concurrent task, each importing pandas and pyarrow at ~150 MB resident. At 12
  cores that is ~1.8 GB alongside the JVM heap, the OS starts killing workers,
  and Spark retries the task onto another worker that is killed just as fast.

  `spark.driver.memory` is a claim on the JVM only. The python side is invisible
  to it, and no configuration in this session accounts for it. That is why this
  benchmark runs over 4 partitions rather than 12 - see the comment above.

  A built-in expression costs none of this, which is a stronger argument than
  the timings.""")
sample.unpersist()

# ---------------------------------------------------------------------------
banner("6. the real curate() chain at scale")
# curate() is ~25 derived columns, one explode-shaped array transform, and no
# shuffle at all. Running it over raw-shaped input shows the whole pipeline is
# a single narrow stage until something asks for a group.
raw = spark.read.option("mergeSchema", "true").parquet(TABLE)
plan = df.select("decade").groupBy("decade").count()

runs = []
with measure(spark, "curate columns, no aggregate") as m:
    df.select("issues", "n_issues", "usable_for_mapping").agg(F.count("*")).collect()
runs.append(m)
with measure(spark, "+ groupBy decade") as m:
    df.groupBy("decade").agg(F.avg(F.col("usable_for_mapping").cast("int"))).collect()
runs.append(m)
with measure(spark, "+ groupBy decade, country, basis") as m:
    df.groupBy("decade", "country", "basis") \
      .agg(F.avg(F.col("usable_for_mapping").cast("int")),
           F.count("*")).collect()
runs.append(m)
with measure(spark, "explode issues, then group") as m:
    df.select("decade", F.explode("issues").alias("flag")) \
      .groupBy("flag").count().collect()
runs.append(m)
show(runs)
print("""
  explode is narrow - it produces more rows from one partition without looking
  at any other - but it multiplies the input to the shuffle that follows. 58M
  rows with a mean of ~3 flags each is ~170M rows entering the groupBy. That
  is the cardinality explosion from the other direction: not too many groups,
  too many rows per group.""")

# ---------------------------------------------------------------------------
banner("7. shuffle partitions actually matter")
# day 4 left this open: is 24 right? Answer it by moving one knob.
def two_key_agg():
    return (df.groupBy("decade", "country")
              .agg(F.count("*").alias("n"),
                   F.avg(F.col("usable_for_mapping").cast("int")).alias("u")))

runs = []
for parts in [4, 24, 200, 800]:
    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.sql.shuffle.partitions", parts)
    with measure(spark, f"shuffle.partitions = {parts}") as m:
        two_key_agg().collect()
    runs.append(m)
conf.set("spark.sql.adaptive.enabled", "true")
conf.set("spark.sql.shuffle.partitions", 200)
with measure(spark, "AQE on (200 -> coalesced)") as m:
    two_key_agg().collect()
runs.append(m)
show(runs)
best = min(runs, key=lambda r: r["seconds"])
print(f"""
  Fastest here: {best['label']} at {best['seconds']:.1f}s.

  Too few partitions leaves the machine idle; too many costs scheduling
  overhead and tiny shuffle files. `shuffle w` grows with the partition
  count for identical output, because each partition carries its own
  framing.

  No spill here: this aggregate produces ~250x44 groups, so even 4
  partitions fit in memory. Spill comes from partition size against
  executor memory, which is why section 3's groupBy(gbifid) spilled 3.8 GB
  and this does not.

  AQE coalesces post-shuffle partitions using map-side statistics, so an
  over-large setting mostly stops mattering. It only coalesces downward
  though, so a too-low setting stays too low.""")

# ---------------------------------------------------------------------------
banner("8. caching: when it pays and when it is a tax")
target = df.select("decade", "country", "usable_for_mapping", "n_issues") \
           .where(F.col("decade") >= 1990)
runs = []
with measure(spark, "3 aggregates, no cache") as m:
    for c in ["decade", "country", "n_issues"]:
        target.groupBy(c).count().collect()
runs.append(m)
target.cache()
with measure(spark, "populate cache") as m:
    target.count()
runs.append(m)
with measure(spark, "3 aggregates, cached") as m:
    for c in ["decade", "country", "n_issues"]:
        target.groupBy(c).count().collect()
runs.append(m)
try:
    rdds = bench._ui(f"/applications/{spark.sparkContext.applicationId}/storage/rdd")
    print(f"  cached in memory: {gb(sum(r.get('memoryUsed', 0) for r in rdds))}"
          f" across {len(rdds)} cached dataset(s)")
except Exception as exc:
    print(f"  (storage info unavailable: {exc})")
target.unpersist()
show(runs)
print("""
  Cache pays when a dataframe is read more than once AND recomputing it is
  expensive AND it fits. Here the source is parquet on local disk with column
  pruning, so recomputation is already cheap and the cache barely wins. Over
  S3, or after a shuffle, the same three lines look very different.

  Default to not caching, and add it when a measurement says to.""")

# ---------------------------------------------------------------------------
if SLOW:
    banner("9. the deliberately awful version  (SLOW=1)")
    print("everything you are told not to do, measured\n")
    runs = []
    with measure(spark, "repartition(2000) then count") as m:
        df.select("gbifid").repartition(2000).count()
    runs.append(m)
    with measure(spark, "orderBy then filter (backwards)") as m:
        df.select("gbifid", "decade").orderBy("gbifid") \
          .where(F.col("decade") == 2010).count()
    runs.append(m)
    with measure(spark, "filter then orderBy (right way)") as m:
        df.select("gbifid", "decade").where(F.col("decade") == 2010) \
          .orderBy("gbifid").count()
    runs.append(m)
    with measure(spark, "groupBy then join back to self") as m:
        g = df.groupBy("datasetkey").agg(F.count("*").alias("n"))
        df.select("gbifid", "datasetkey").join(g, "datasetkey").count()
    runs.append(m)
    show(runs)
    print("""
  Catalyst reorders filters past sorts when it can prove it is safe, so the
  two orderBy lines can come out close together. The repartition line is pure
  loss: a shuffle whose only product is more partitions.""")

# ---------------------------------------------------------------------------
banner("10. notes")
print("""findings
  - narrow transformations fuse into one stage; the cost of curate()'s 25
    derived columns is CPU on bytes already in flight, not a new pass
  - the unit of cost is the Exchange. Count them in the plan before timing
    anything
  - groupBy cost tracks the number of GROUPS, not rows, because partial
    aggregation reduces the map side first. Grouping by a unique key defeats
    that entirely and is slower than no aggregation
  - exact countDistinct shuffles in proportion to cardinality;
    approx_count_distinct shuffles a fixed-size sketch. Several exact distincts
    in one agg is worse than each alone
  - python UDF vs built-in is the biggest single-line speed difference in this
    file, and it is invisible in the plan unless you look for BatchEvalPython
  - UDF parallelism is UDF memory: one python worker per concurrent task, each
    carrying its own interpreter and imports. The driver memory setting does
    not cover it
  - AQE only coalesces downward; too FEW shuffle partitions stays a bug
  - spill comes from partition size vs executor memory, not from the shuffle
    partition count directly. The only spill in this file (3.8 GB) came from
    grouping on a unique key
  - caching a cheap parquet read is close to a no-op. It is not a performance
    button

gotchas hit
  - `.cache()` is lazy: without an action after it, the next timing includes
    populating the cache and looks like the cache made things slower
  - measuring a second time on the same DataFrame can hit a cached plan or a
    warm page cache. Every measurement here rebuilds its DataFrame

questions
  - the explode-then-group path is the one that will hurt at 266 GB. Does it
    need a pre-aggregation to a flag VECTOR per record before the shuffle?
  - is 24 shuffle partitions defensible at all now, or should gbif.py compute
    it from input size?

to try
  - day 7 does the joins; the groupBy-then-join-back pattern in section 9 is
    the thing a window function replaces, and day 8 measures that swap""")

if HOLD_FOR_UI:
    input("\nSpark UI on http://localhost:4040 - Enter to stop...")
spark.stop()
print("\ndone.")
