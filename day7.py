"""Day 7 - joins: a 58-million-row fact table meets a 54-thousand-row dimension.

The whole reason this join exists: the occurrence snapshot knows `datasetkey`
and nothing about who published it. SCOPE.md's claim is that flags describe the
PUBLISHER, not the record. Without registry.py's dimension table there is no
publisher column and no claim to test.

    uv run python registry.py fetch && uv run python registry.py build
    uv run python day7.py
    SKEW=1 uv run python day7.py      # include the salting experiment

The three questions this file answers, in order:
  1. does Spark broadcast the dimension on its own, and how does it decide
  2. what does the null key do to a join, and to your numbers
  3. what does skew on datasetkey cost, and does salting or AQE fix it
"""
import os

from pyspark.sql import functions as F

import bench
import gbif
from bench import banner, gb, measure, show

TABLE = os.environ.get("TABLE", "data/curated/occurrence_slim")
DIM = os.environ.get("DIM", "data/curated/dataset_dim")
DO_SKEW = os.environ.get("SKEW", "1") == "1"
HOLD_FOR_UI = os.environ.get("HOLD_FOR_UI") == "1"

# 4g, not 8g. This laptop has 16 GB and Docker Desktop holds a standing claim
# on part of it; day 6 proved that an oversized JVM heap plus anything else
# ends in the OS killing processes rather than in a Spark error. The curated
# table is 2.3 GB with column pruning on top, so 4 GB is not the constraint -
# see logs/gotchas.md.
spark = gbif.spark_session(app="gbif-day7", driver_memory="4g",
                           shuffle_partitions=24)
spark.sparkContext.setLogLevel("ERROR")
conf = spark.conf

facts = spark.read.option("mergeSchema", "true").parquet(TABLE)
dim = spark.read.parquet(DIM)

FACT_ROWS = facts.count()
DIM_ROWS = dim.count()
import pathlib
DIM_BYTES = sum(f.stat().st_size for f in pathlib.Path(DIM).rglob("*.parquet"))

print(f"facts : {TABLE}  {FACT_ROWS:,} rows")
print(f"dim   : {DIM}  {DIM_ROWS:,} rows, {gb(DIM_BYTES)} on disk")

# ---------------------------------------------------------------------------
banner("1. does Spark broadcast it by itself?")
# Day 4's open question. There, a 13-row lookup built with createDataFrame did
# NOT broadcast - an RDD-backed relation has no size statistics, so Catalyst
# assumed infinity. The fix day 4 guessed at was F.broadcast(). The real fix is
# to give Spark statistics, and a parquet file has them in its footer.
threshold = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
print(f"autoBroadcastJoinThreshold : {gb(threshold)}")
print(f"dimension on disk          : {gb(DIM_BYTES)}")

stats = dim._jdf.queryExecution().optimizedPlan().stats()
print(f"Catalyst's size estimate   : {gb(int(stats.sizeInBytes()))}"
      f"   <- from the parquet footer, not a guess")

in_mem = spark.createDataFrame(
    [(r.datasetkey, r.publisher_key) for r in dim.limit(200).collect()],
    ["datasetkey", "publisher_key"])
mem_stats = in_mem._jdf.queryExecution().optimizedPlan().stats()
print(f"same data via createDataFrame: {gb(int(mem_stats.sizeInBytes()))}"
      f"   <- 200 rows 'estimated' at 8 EB")

j = facts.select("gbifid", "datasetkey").join(dim.select("datasetkey", "publisher_key"),
                                              "datasetkey")
plan = j._jdf.queryExecution().executedPlan().toString()
kind = ("BroadcastHashJoin" if "BroadcastHashJoin" in plan else
        "SortMergeJoin" if "SortMergeJoin" in plan else "?")
print(f"\nchosen strategy            : {kind}")
print(f"""
  This is the answer to day 4's open question. Writing the lookup to parquet
  instead of building it from a Python list is what makes Spark broadcast it
  unprompted - because parquet's footer carries a real byte count and
  createDataFrame's RDD does not. F.broadcast() was never the fix; it was a
  way of overriding a missing statistic by hand.""")

# ---------------------------------------------------------------------------
banner("2. broadcast vs sort-merge, measured")
def joined(strategy):
    d = dim.select("datasetkey", "publisher_key", "publisher_country", "license")
    if strategy == "broadcast":
        d = F.broadcast(d)
    return (facts.select("gbifid", "datasetkey", "usable_for_mapping")
                 .join(d, "datasetkey")
                 .groupBy("publisher_country")
                 .agg(F.count("*").alias("records"),
                      F.avg(F.col("usable_for_mapping").cast("int")).alias("usable")))

runs = []
conf.set("spark.sql.autoBroadcastJoinThreshold", -1)   # force sort-merge
with measure(spark, "SortMergeJoin (broadcast off)") as m:
    joined("auto").collect()
runs.append(m)
conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
with measure(spark, "BroadcastHashJoin") as m:
    joined("broadcast").collect()
runs.append(m)
show(runs)
print("""
  Sort-merge shuffles BOTH sides: 58M fact rows across the network, sorted, to
  meet 54k dimension rows. Broadcast ships the dimension to every executor once
  and the fact table never moves - the join becomes a narrow transformation, a
  hash lookup inside the scan stage. Look at `shuffle w`: one line writes the
  fact table, the other writes only the group-by subtotals.

  The rule is not "small table". It is: does the small side fit in each
  executor's memory, several times over (one copy per concurrent task)?""")

# ---------------------------------------------------------------------------
banner("3. the null-key trap")
# datasetkey is never null here, but publishingorgkey and specieskey are, and
# the trap is generic: NULL never equals NULL, so null-keyed rows vanish from
# an inner join and are silently gone from every number downstream.
nulls = facts.where(F.col("specieskey").isNull()).count()
print(f"rows with NULL specieskey : {nulls:,} ({100 * nulls / FACT_ROWS:.1f}%)")

species_dim = (facts.where(F.col("specieskey").isNotNull())
                    .select("specieskey", "species").distinct())
inner = facts.join(species_dim, "specieskey").count()
left = facts.join(species_dim, "specieskey", "left").count()
print(f"\n  facts                   : {FACT_ROWS:,}")
print(f"  inner join on specieskey: {inner:,}   ({FACT_ROWS - inner:,} rows gone)")
print(f"  left join on specieskey : {left:,}")
print(f"""
  An inner join is a filter you did not write. {100 * (FACT_ROWS - inner) / FACT_ROWS:.0f}% of the table
  disappeared and nothing warned you. For SCOPE.md this matters more than
  usual: the question is what fraction of GBIF is unusable, and unusable
  records are exactly the ones with null keys. An inner join here would delete
  the evidence and then report that the data is clean.

  Rule for this project, same as curate()'s: left join, then count the nulls
  as a category.""")

matched = facts.join(dim.select("datasetkey", F.lit(True).alias("in_registry")),
                     "datasetkey", "left")
unmatched = matched.where(F.col("in_registry").isNull()).count()
print(f"  fact rows whose dataset is NOT in the registry: {unmatched:,} "
      f"({100 * unmatched / FACT_ROWS:.2f}%)")

# ---------------------------------------------------------------------------
banner("4. join types, and what each one is for")
small_dim = dim.select("datasetkey").limit(500)
for how in ["inner", "left", "left_semi", "left_anti"]:
    n = facts.select("gbifid", "datasetkey").join(small_dim, "datasetkey", how).count()
    print(f"  {how:<12}{n:>14,}")
print("""
  left_semi is a filter that cannot duplicate rows and cannot add columns; it
  is the right tool for "keep facts whose dataset is in this list", and it is
  cheaper than an inner join because the build side needs no payload.
  left_anti is its complement and is how you find unmatched rows without a
  null check.""")

# ---------------------------------------------------------------------------
banner("5. the join that answers the question")
enriched = (facts
            .join(F.broadcast(dim.select(
                "datasetkey", "publisher_key", "publisher_title",
                "publisher_country", "license")), "datasetkey", "left")
            .withColumn("publisher_title",
                        F.coalesce("publisher_title", F.lit("<not in registry>"))))

with measure(spark, "usable share by publisher") as m:
    by_pub = (enriched
              .groupBy("publisher_key", "publisher_title", "publisher_country")
              .agg(F.count("*").alias("records"),
                   F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"),
                   F.avg("n_issues").alias("mean_flags"))
              .where(F.col("records") >= 10_000)
              .orderBy(F.desc("records")))
    top = by_pub.limit(20).collect()
print(m)
print(f"\n  {'publisher':<42}{'cc':>4}{'records':>13}{'usable':>9}{'flags':>8}")
for r in top:
    print(f"  {(r['publisher_title'] or '?')[:40]:<42}{r['publisher_country'] or '--':>4}"
          f"{r['records']:>13,}{100 * (r['usable'] or 0):>8.1f}%"
          f"{r['mean_flags'] or 0:>8.2f}")
print("""
  This table IS the claim in SCOPE.md section 1, in its rawest form: if the
  usable-share column were roughly constant, the claim would be dead. It is
  not constant. Day 8 turns that observation into within-vs-between variance,
  which is the version that can be defended.""")

# ---------------------------------------------------------------------------
if DO_SKEW:
    banner("6. skew on datasetkey")
    counts = (facts.groupBy("datasetkey").count()
                   .orderBy(F.desc("count")).limit(10).collect())
    total = FACT_ROWS
    print(f"  {'datasetkey':<40}{'rows':>14}{'share':>8}")
    for r in counts:
        print(f"  {r['datasetkey']:<40}{r['count']:>14,}"
              f"{100 * r['count'] / total:>7.1f}%")
    hot = counts[0]["datasetkey"]
    hot_n = counts[0]["count"]
    print(f"""
  One dataset is {100 * hot_n / total:.0f}% of the table. In a sort-merge join every row
  with that key goes to ONE partition, so one task does {100 * hot_n / total:.0f}% of the work
  while the rest idle. A stage ends when its slowest task ends, so the job is
  effectively single-threaded for that stretch.""")

    dim_small = dim.select("datasetkey", "publisher_key")
    runs = []
    conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
    with measure(spark, "SMJ, AQE off") as m:
        facts.select("gbifid", "datasetkey").join(dim_small, "datasetkey") \
             .groupBy("publisher_key").count().collect()
    runs.append(m)

    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    with measure(spark, "SMJ, AQE skew join on") as m:
        facts.select("gbifid", "datasetkey").join(dim_small, "datasetkey") \
             .groupBy("publisher_key").count().collect()
    runs.append(m)

    # salting by hand: explode the small side N ways, spread the big side over
    # the same N buckets. The hot key becomes N keys and N tasks.
    SALT = 16
    salted_dim = (dim_small
                  .withColumn("salt", F.explode(F.array(*[F.lit(i) for i in range(SALT)])))
                  .withColumn("k", F.concat_ws("#", "datasetkey", "salt")))
    salted_facts = (facts.select("gbifid", "datasetkey")
                    .withColumn("salt", F.pmod(F.col("gbifid").cast("long"),
                                               F.lit(SALT)).cast("int"))
                    .withColumn("k", F.concat_ws("#", "datasetkey", "salt")))
    with measure(spark, f"SMJ, salted x{SALT}") as m:
        salted_facts.join(salted_dim.select("k", "publisher_key"), "k") \
                    .groupBy("publisher_key").count().collect()
    runs.append(m)

    conf.set("spark.sql.autoBroadcastJoinThreshold", threshold)
    with measure(spark, "broadcast (no shuffle at all)") as m:
        facts.select("gbifid", "datasetkey").join(F.broadcast(dim_small), "datasetkey") \
             .groupBy("publisher_key").count().collect()
    runs.append(m)
    show(runs)
    print(f"""
  This is day 4's third open question answered: salting vs AQE.

  AQE's skew handling splits an oversized shuffle partition into several and
  replicates the matching rows from the other side - salting, done by the
  engine, using statistics it collected at runtime. It needs no code and it
  cannot be wrong about which key is hot.
  Hand-salting still wins in one case: when the skew is on the side AQE cannot
  split, or when you need the salt for something else too. Here it costs an
  extra shuffle to build the {SALT}x dimension and is not worth it.

  And the honest answer for THIS join: none of it matters, because the
  dimension is {gb(DIM_BYTES)} and broadcasting removes the shuffle entirely. Skew is
  a shuffle problem. No shuffle, no skew.""")

# ---------------------------------------------------------------------------
banner("7. notes")
print(f"""findings
  - Spark broadcasts a parquet-backed dimension unprompted and refuses to
    broadcast the identical data from createDataFrame. The difference is
    statistics in the footer, not size. Day 4's SortMergeJoin-on-13-rows was a
    missing statistic, not a missing hint
  - the only argument for F.broadcast() is overriding a statistic you know to
    be wrong - or one that does not exist
  - an inner join is an unwritten filter. {100 * (FACT_ROWS - inner) / FACT_ROWS:.0f}% of rows vanish on a null
    specieskey and no warning is produced. For a project whose question is
    "how much is unusable", that is the worst possible silent behaviour
  - left_semi expresses "filter by membership" better than inner + distinct,
    and is cheaper
  - the top dataset is a double-digit percentage of the table. That is skew,
    and it is inherent to GBIF - eBird is genuinely that large
  - AQE skew join handling is the default answer; hand-salting is for the case
    AQE cannot see. Broadcasting beats both by removing the shuffle

gotchas hit
  - autoBroadcastJoinThreshold = -1 disables broadcasting entirely, which is
    how you force sort-merge for a comparison. Setting it back matters - the
    conf is session-wide and every later section inherits it
  - the salted join needs the salt on the BIG side to be deterministic and
    uniform; deriving it from gbifid works, deriving it from rand() breaks
    re-execution after a task failure

questions
  - the registry has {DIM_ROWS:,} datasets and this slice touches {facts.select('datasetkey').distinct().count():,}. At full
    scale the dimension is unchanged - it is a genuinely small dimension, so
    the broadcast holds at 266 GB. Worth re-checking on the cluster where
    executor memory is 2 GB, not 6
  - publisher_country vs the occurrence's own country: those disagree, and the
    disagreement is probably a finding

to try
  - day 8 needs this join, so it becomes a cached enriched view rather than
    something recomputed per question""")

if HOLD_FOR_UI:
    input("\nSpark UI on http://localhost:4040 - Enter to stop...")
spark.stop()
print("\ndone.")
