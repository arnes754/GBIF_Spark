"""Day 8 - aggregations and window functions.

A groupBy answers "what is the value per group" and throws the rows away. A
window answers "what is this row's value relative to its group" and keeps the
rows. That difference is the whole day, and it is the difference between
"which publisher has the most records" and "is this publisher's flag mix
unusual for its size".

SCOPE.md's claim needs the second kind. "Flag mix varies more BETWEEN
publishers than WITHIN them" is a variance decomposition, and every term in it
is a window or a grouped aggregate.

    uv run python registry.py build      # day 7 built the dimension
    uv run python day8.py
    HEAVY=1 uv run python day8.py        # include the unpartitioned-window trap
"""
import os

from pyspark.sql import Window, functions as F

import bench
import gbif
from bench import banner, gb, measure, show

TABLE = os.environ.get("TABLE", "data/curated/occurrence_slim")
DIM = os.environ.get("DIM", "data/curated/dataset_dim")
HEAVY = os.environ.get("HEAVY", "0") == "1"
HOLD_FOR_UI = os.environ.get("HOLD_FOR_UI") == "1"

# 4g, not 8g. This laptop has 16 GB and Docker Desktop holds a standing claim
# on part of it; day 6 proved that an oversized JVM heap plus anything else
# ends in the OS killing processes rather than in a Spark error. The curated
# table is 2.3 GB with column pruning on top, so 4 GB is not the constraint -
# see logs/gotchas.md.
spark = gbif.spark_session(app="gbif-day8", driver_memory="4g",
                           shuffle_partitions=48)
spark.sparkContext.setLogLevel("ERROR")
conf = spark.conf

facts = spark.read.option("mergeSchema", "true").parquet(TABLE)
dim = spark.read.parquet(DIM).select(
    "datasetkey", "publisher_key", "publisher_title", "publisher_country",
    "license", "dataset_title")

# Day 7 established this join broadcasts. Everything below reads from it, so it
# is built once and cached - the fact rows are read once and enriched once.
e = (facts.join(F.broadcast(dim), "datasetkey", "left")
          .withColumn("publisher_key",
                      F.coalesce("publisher_key", F.lit("UNREGISTERED")))
          .withColumn("publisher_title",
                      F.coalesce("publisher_title", F.lit("<not in registry>"))))
ENRICHED = (e.select("gbifid", "datasetkey", "publisher_key", "publisher_title",
                     "publisher_country", "country", "decade", "year",
                     "basis", "species", "specieskey", "cell_id",
                     "n_issues", "n_geo_issues", "issues",
                     "usable_for_mapping", "interpreted_year")
             .cache())
N = ENRICHED.count()
print(f"enriched fact table: {N:,} rows (cached)")

# ---------------------------------------------------------------------------
banner("1. aggregations: one pass, many answers")
# Every one of these is a separate groupBy = a separate shuffle = a separate
# pass. Doing them as several aggregate expressions over ONE groupBy is one
# shuffle. The difference is not subtle.
runs = []
with measure(spark, "4 separate groupBy(decade)") as m:
    for agg in [F.count("*"), F.avg("n_issues"),
                F.avg(F.col("usable_for_mapping").cast("int")),
                F.approx_count_distinct("species", 0.05)]:
        ENRICHED.groupBy("decade").agg(agg).collect()
runs.append(m)
with measure(spark, "1 groupBy, 4 aggregates") as m:
    ENRICHED.groupBy("decade").agg(
        F.count("*"), F.avg("n_issues"),
        F.avg(F.col("usable_for_mapping").cast("int")),
        F.approx_count_distinct("species", 0.05)).collect()
runs.append(m)
show(runs)

# ---------------------------------------------------------------------------
banner("2. the headline, and the breakdowns that make it interesting")
head = ENRICHED.agg(
    F.count("*").alias("records"),
    F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"),
    F.avg("n_issues").alias("mean_flags"),
    F.avg((F.col("n_geo_issues") > 0).cast("int")).alias("geo_flagged"),
).collect()[0]
print(f"  records                  {head['records']:>14,}")
print(f"  usable for mapping       {100 * head['usable']:>13.2f}%   <- the headline")
print(f"  mean flags per record    {head['mean_flags']:>14.2f}")
print(f"  carries a fatal geo flag {100 * head['geo_flagged']:>13.2f}%")

print("\n  by decade:")
(ENRICHED.groupBy("decade")
         .agg(F.count("*").alias("records"),
              F.round(100 * F.avg(F.col("usable_for_mapping").cast("int")), 1).alias("usable_pct"),
              F.round(F.avg("n_issues"), 2).alias("flags"))
         .where(F.col("records") > 50_000)
         .orderBy("decade").show(30, truncate=False))

print("  by basis of record:")
(ENRICHED.groupBy("basis")
         .agg(F.count("*").alias("records"),
              F.round(100 * F.avg(F.col("usable_for_mapping").cast("int")), 1).alias("usable_pct"))
         .orderBy(F.desc("records")).show(12, truncate=False))

# ---------------------------------------------------------------------------
banner("3. grouping sets: every breakdown in one shuffle")
# cube/rollup compute several grouping levels in a single pass. The trap is
# that a cube over k columns computes 2^k grouping sets, which is a cardinality
# explosion with a very innocent-looking API.
runs = []
with measure(spark, "3 separate groupBys") as m:
    for c in ["decade", "country", "basis"]:
        ENRICHED.groupBy(c).count().collect()
runs.append(m)
with measure(spark, "grouping_sets, one pass") as m:
    ENRICHED.cube("decade", "country", "basis").count() \
            .where(F.expr("(decade is null) + (country is null) + (basis is null) = 2")) \
            .collect()
runs.append(m)
with measure(spark, "full cube (2^3 sets)") as m:
    n_cube = ENRICHED.cube("decade", "country", "basis").count().count()
runs.append(m)
show(runs)
n_d = ENRICHED.select("decade").distinct().count()
n_c = ENRICHED.select("country").distinct().count()
n_b = ENRICHED.select("basis").distinct().count()
print(f"""
  distinct values: decade {n_d}, country {n_c}, basis {n_b}
  full cube output rows: {n_cube:,}   (roughly ({n_d}+1)x({n_c}+1)x({n_b}+1) worst case
  = {(n_d + 1) * (n_c + 1) * (n_b + 1):,})

  Three grouping columns is fine. Add cell_id (10^5 values) and the cube is
  billions of rows from a 58M-row input - an aggregation that produces more
  data than it consumed. That is the cardinality explosion, and `cube` is the
  easiest way to trigger it by accident.""")

# ---------------------------------------------------------------------------
banner("4. windows: the row survives the aggregate")
# The question "which datasets are unusually flagged FOR THEIR PUBLISHER" needs
# each dataset's value AND its publisher's value on the same row. A groupBy
# gives you one or the other; a window gives you both without a self-join.
per_dataset = (ENRICHED.groupBy("publisher_key", "publisher_title", "datasetkey")
                       .agg(F.count("*").alias("records"),
                            F.avg("n_issues").alias("mean_flags"),
                            F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"))
                       .where(F.col("records") >= 1000)
                       .cache())
print(f"datasets with >=1000 records: {per_dataset.count():,}")

w_pub = Window.partitionBy("publisher_key")
runs = []
with measure(spark, "window: dataset vs its publisher") as m:
    ranked = (per_dataset
              .withColumn("pub_records", F.sum("records").over(w_pub))
              .withColumn("pub_mean_flags", F.avg("mean_flags").over(w_pub))
              .withColumn("pub_datasets", F.count("*").over(w_pub))
              .withColumn("rank_in_pub",
                          F.row_number().over(w_pub.orderBy(F.desc("records"))))
              .withColumn("flag_gap", F.col("mean_flags") - F.col("pub_mean_flags")))
    out = (ranked.where((F.col("pub_datasets") >= 5) & (F.col("rank_in_pub") <= 3))
                 .orderBy(F.desc("pub_records"), "rank_in_pub").limit(15).collect())
runs.append(m)

# the pre-window way: aggregate, then join back. Same answer, two shuffles.
with measure(spark, "same thing via groupBy + join") as m:
    pub = per_dataset.groupBy("publisher_key").agg(
        F.sum("records").alias("pub_records"),
        F.avg("mean_flags").alias("pub_mean_flags"),
        F.count("*").alias("pub_datasets"))
    per_dataset.join(pub, "publisher_key") \
               .where(F.col("pub_datasets") >= 5).count()
runs.append(m)
show(runs)

print(f"\n  {'publisher':<30}{'#':>3}{'records':>11}{'flags':>7}{'vs pub':>8}")
for r in out:
    print(f"  {(r['publisher_title'] or '?')[:28]:<30}{r['rank_in_pub']:>3}"
          f"{r['records']:>11,}{r['mean_flags']:>7.2f}{r['flag_gap']:>+8.2f}")
print("""
  One window scan replaces an aggregate plus a join - one shuffle instead of
  two, and the rows are never discarded and re-attached. This is the single
  most useful thing window functions do.""")

# ---------------------------------------------------------------------------
banner("5. the claim, as a variance decomposition")
# If flags describe the publisher, then two datasets from the same publisher
# should look more alike than two datasets from different publishers. That is
# exactly between-group variance vs within-group variance.
stats = (per_dataset
         .withColumn("pub_mean", F.avg("mean_flags").over(w_pub))
         .withColumn("pub_n", F.count("*").over(w_pub))
         .where(F.col("pub_n") >= 3))
grand = stats.agg(F.avg("mean_flags")).collect()[0][0]
v = stats.agg(
    F.avg(F.pow(F.col("mean_flags") - F.col("pub_mean"), 2)).alias("within"),
    F.avg(F.pow(F.col("pub_mean") - F.lit(grand), 2)).alias("between"),
    F.count("*").alias("n"),
    F.approx_count_distinct("publisher_key").alias("publishers"),
).collect()[0]
total_v = v["within"] + v["between"]
print(f"  datasets considered   : {v['n']:,} across ~{v['publishers']:,} publishers")
print(f"  grand mean flags/rec  : {grand:.3f}")
print(f"  within-publisher var  : {v['within']:.4f}   ({100 * v['within'] / total_v:.0f}%)")
print(f"  between-publisher var : {v['between']:.4f}   ({100 * v['between'] / total_v:.0f}%)")
verdict = ("SUPPORTED - flag mix is mostly a publisher property"
           if v["between"] > v["within"] else
           "NOT SUPPORTED - datasets vary more within a publisher than between")
print(f"\n  SCOPE.md section 1 claim: {verdict}")
print("""
  Stated this way the claim is falsifiable by one number, which is what a
  claim is for. Day 16's job is to attack it: record age and GBIF's
  interpretation version are both confounded with publisher, and the
  decomposition above cannot tell them apart on its own.""")

print("\n  the same decomposition against interpretation year, as a control:")
w_int = Window.partitionBy("interp_bucket")
ctrl = (ENRICHED.where(F.col("interpreted_year").isNotNull())
        .withColumn("interp_bucket", F.col("interpreted_year"))
        .groupBy("interp_bucket", "datasetkey")
        .agg(F.avg("n_issues").alias("mean_flags"), F.count("*").alias("records"))
        .where(F.col("records") >= 1000)
        .withColumn("b_mean", F.avg("mean_flags").over(w_int)))
g2 = ctrl.agg(F.avg("mean_flags")).collect()[0][0]
v2 = ctrl.agg(F.avg(F.pow(F.col("mean_flags") - F.col("b_mean"), 2)).alias("within"),
              F.avg(F.pow(F.col("b_mean") - F.lit(g2), 2)).alias("between")).collect()[0]
t2 = v2["within"] + v2["between"]
print(f"    within-interpretation-year var : {v2['within']:.4f}  ({100 * v2['within'] / t2:.0f}%)")
print(f"    between-interpretation-year var: {v2['between']:.4f}  ({100 * v2['between'] / t2:.0f}%)")
print("  If this split is as strong as the publisher one, the claim needs a "
      "\n  joint model, not a comparison. Noted for day 16.")

# ---------------------------------------------------------------------------
banner("6. ordered windows: lag, lead, and running totals")
# A dataset's yearly record count, and how it changed. This is the shape every
# time series question takes, and it is one window with an ORDER BY.
yearly = (ENRICHED.where(F.col("year").between(1950, 2025))
                  .groupBy("publisher_key", "year")
                  .agg(F.count("*").alias("records"),
                       F.avg(F.col("usable_for_mapping").cast("int")).alias("usable")))
w_time = Window.partitionBy("publisher_key").orderBy("year")
w_roll = w_time.rowsBetween(-4, 0)
w_all = w_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)

with measure(spark, "lag + rolling + cumulative, one window spec") as m:
    trend = (yearly
             .withColumn("prev", F.lag("records").over(w_time))
             .withColumn("yoy", F.round(100 * (F.col("records") / F.col("prev") - 1), 1))
             .withColumn("roll5", F.round(F.avg("records").over(w_roll)))
             .withColumn("cumulative", F.sum("records").over(w_all))
             .withColumn("first_year", F.first("year").over(w_all)))
    big = (trend.where(F.col("publisher_key") ==
                       ENRICHED.groupBy("publisher_key").count()
                               .orderBy(F.desc("count")).first()["publisher_key"])
               .orderBy(F.desc("year")).limit(10).collect())
print(m)
print(f"\n  largest publisher, recent years:")
print(f"  {'year':>6}{'records':>12}{'yoy %':>9}{'roll5':>12}{'cumulative':>14}")
for r in sorted(big, key=lambda x: x["year"]):
    print(f"  {r['year']:>6}{r['records']:>12,}"
          f"{(r['yoy'] if r['yoy'] is not None else 0):>9.1f}"
          f"{int(r['roll5'] or 0):>12,}{r['cumulative']:>14,}")
print("""
  lag/lead/rolling/cumulative all share ONE window spec and therefore one
  shuffle, because they are partitioned and ordered identically. Change the
  partitionBy on any one of them and you have bought a second Exchange.
  Check the plan: adjacent Window operators over the same spec collapse.""")

# ---------------------------------------------------------------------------
banner("7. skew in a window, which is worse than skew in a join")
counts = (ENRICHED.groupBy("publisher_key").count()
                  .orderBy(F.desc("count")).limit(5).collect())
print(f"  {'publisher_key':<40}{'rows':>14}{'share':>8}")
for r in counts:
    print(f"  {r['publisher_key']:<40}{r['count']:>14,}{100 * r['count'] / N:>7.1f}%")
print(f"""
  A window partition must fit in ONE task's memory, because the frame is
  evaluated over the sorted partition as a unit. A groupBy can reduce the map
  side first; a window cannot. So a key holding {100 * counts[0]['count'] / N:.0f}% of the rows is a
  groupBy that is slow and a window that runs out of memory and spills.

  AQE's skew-join splitting does NOT apply to windows. The mitigations are:
    - aggregate FIRST and window over the small result (sections 4-6 all do
      this: the window runs over ~{per_dataset.count():,} dataset rows, not 58M fact rows)
    - or partition the window more finely (publisher_key, decade)""")

if HEAVY:
    print("\n  HEAVY=1: the unpartitioned window trap")
    w_none = Window.orderBy("gbifid")
    runs = []
    with measure(spark, "window with partitionBy") as m:
        ENRICHED.select("gbifid", "publisher_key") \
                .withColumn("rn", F.row_number().over(
                    Window.partitionBy("publisher_key").orderBy("gbifid"))) \
                .agg(F.max("rn")).collect()
    runs.append(m)
    with measure(spark, "window with NO partitionBy") as m:
        ENRICHED.select("gbifid").withColumn("rn", F.row_number().over(w_none)) \
                .agg(F.max("rn")).collect()
    runs.append(m)
    show(runs)
    print("""
  A window with no partitionBy has exactly one partition, so all 58M rows go to
  one task on one executor and are sorted there. The plan says it out loud -
  "No Partition Defined for Window operation! Moving all data to a single
  partition" - and it is the most common way to turn a working job into a
  hanging one.""")

# ---------------------------------------------------------------------------
banner("8. cardinality explosion, deliberately")
# explode turns 1 row into n. Combine it with a groupBy on a high-cardinality
# key and the intermediate is much larger than either input.
flags = ENRICHED.select("publisher_key", "decade", F.explode("issues").alias("flag"))
with measure(spark, "explode issues -> flag rows") as m:
    n_flag_rows = flags.count()
print(m)
print(f"  {N:,} records -> {n_flag_rows:,} flag rows "
      f"({n_flag_rows / N:.1f}x)")

runs = []
with measure(spark, "groupBy(flag) - 100s of keys") as m:
    top_flags = flags.groupBy("flag").count().orderBy(F.desc("count")).limit(10).collect()
runs.append(m)
with measure(spark, "groupBy(publisher, flag) - 10^5 keys") as m:
    flags.groupBy("publisher_key", "flag").count().agg(F.count("*")).collect()
runs.append(m)
with measure(spark, "groupBy(publisher, decade, flag)") as m:
    n_cells = flags.groupBy("publisher_key", "decade", "flag").count() \
                   .agg(F.count("*")).collect()[0][0]
runs.append(m)
show(runs)
print(f"\n  output rows of the three-key grouping: {n_cells:,}")
print(f"\n  {'flag':<46}{'records':>14}{'share':>8}")
for r in top_flags:
    print(f"  {r['flag'][:44]:<46}{r['count']:>14,}{100 * r['count'] / N:>7.1f}%")
print("""
  The top flag being informational and near-universal is day 2's finding, and
  it is why curate.py's GEO_FATAL is 8 flags and not "any flag". An aggregation
  can be fast, correct, and still tell you nothing if the category it counts is
  the wrong one.""")

# ---------------------------------------------------------------------------
banner("9. notes")
print(f"""findings
  - several aggregates over one groupBy is one shuffle; several groupBys is
    several. The API makes the expensive version look natural
  - cube over k columns is 2^k grouping sets. Fine for 3 low-cardinality
    columns, catastrophic the moment a high-cardinality one joins them
  - a window replaces aggregate-then-join-back with a single shuffle, and keeps
    the row. That is the reason to learn them
  - the SCOPE.md claim reduces to between- vs within-publisher variance, which
    is two window columns and one agg. Verdict this run: {verdict.split(' - ')[0]}
  - window partitions cannot be reduced map-side and AQE will not split them,
    so skew hurts a window more than a join. Aggregate first, window second
  - explode multiplies rows BEFORE the shuffle it feeds

gotchas hit
  - `Window.partitionBy(...)` with no orderBy is legal and gives a whole-
    partition frame; adding orderBy silently changes the default frame to
    unboundedPreceding..currentRow, so avg() over the same spec means two
    different things. This is the most dangerous default in the API
  - a window with no partitionBy at all moves everything to one task

questions
  - the interpretation-year control in section 5 is confounded with publisher.
    Day 16 needs a version that holds one constant while varying the other
  - does the variance verdict hold on the full snapshot, or is it an artifact
    of which 1,473 datasets landed in this slice? That is a day-16 question and
    it is the one that could kill the finding

to try
  - day 9 runs all of this end to end and times each stage""")

ENRICHED.unpersist()
per_dataset.unpersist()
if HOLD_FOR_UI:
    input("\nSpark UI on http://localhost:4040 - Enter to stop...")
spark.stop()
print("\ndone.")
