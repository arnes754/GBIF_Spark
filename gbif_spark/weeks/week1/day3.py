"""Day 3 - first transformations: select / filter / withColumn / groupBy / agg.

Run: uv run python -m gbif_spark day 3
     SLICE_GB=0.2 uv run python -m gbif_spark day 3
"""
import os
import time

from pyspark.sql import functions as F

from gbif_spark.helpers import gbif

SLICE_GB = float(os.environ.get("SLICE_GB", 2.0))


def banner(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def timed(label, fn):
    t0 = time.perf_counter()
    out = fn()
    dt = time.perf_counter() - t0
    print(f"  {label:<44}{dt:7.2f}s")
    return out, dt


spark = gbif.spark_session(app="gbif-day3")
spark.sparkContext.setLogLevel("ERROR")
df = spark.read.parquet(*gbif.pick_slice(target_gb=SLICE_GB))

banner("1. building a chain")
t0 = time.perf_counter()
chain = (df
         .select("gbifid", "species", "countrycode", "year", "basisofrecord",
                 "decimallatitude", "decimallongitude", "issue")
         .where(F.col("year") >= 1950)
         .where(F.col("species").isNotNull())
         .withColumn("decade", (F.col("year") / 10).cast("int") * 10))
first_build = time.perf_counter() - t0

t0 = time.perf_counter()
_other = df.select("gbifid", "year").where(F.col("year") >= 1990)
second_build = time.perf_counter() - t0

print(f"4-step chain built in   {first_build * 1000:8.1f} ms")
print(f"second chain built in   {second_build * 1000:8.1f} ms")
print(f"df cols {len(df.columns)} -> chain cols {len(chain.columns)}: {chain.columns}")

print()
n, _ = timed("chain.count()", chain.count)
print(f"  rows through the filters : {n:,}")
_, cold = timed("chain.count() again", chain.count)

chain = chain.cache()
_, materialise = timed("chain.count() after cache()", chain.count)
_, cached = timed("chain.count() from cache", chain.count)

saved, surcharge = cold - cached, materialise - cold
print(f"\ncache surcharge {surcharge:.0f}s, saves {saved:.0f}s per action, "
      f"breaks even after ~{max(1, round(surcharge / saved))} uses")

banner("2. select")
(chain
 .select(
     F.col("species"),
     F.col("countrycode").alias("country"),
     F.col("year"),
     F.upper(F.col("basisofrecord")).alias("basis"),
     F.round(F.col("decimallatitude"), 2).alias("lat"),
     F.coalesce(F.size("issue"), F.lit(0)).alias("n_issues"),
 )
 .show(8, truncate=28))

banner("3. filter")
total = chain.count()
us = chain.where(F.col("countrycode") == "US").count()
not_us = chain.where(F.col("countrycode") != "US").count()
null_cc = chain.where(F.col("countrycode").isNull()).count()
print(f"total               : {total:,}")
print(f"countrycode == 'US' : {us:,}")
print(f"countrycode != 'US' : {not_us:,}")
print(f"countrycode IS NULL : {null_cc:,}")
print(f"== plus != is {us + not_us:,}, which is {total - us - not_us:,} short of {total:,}")

fixed = chain.where((F.col("countrycode") != "US") | F.col("countrycode").isNull()).count()
print(f"(!= 'US') | isNull(): {fixed:,}   -> {us:,} + {fixed:,} = {us + fixed:,}")

geo = chain.where(
    F.col("decimallatitude").isNotNull()
    & F.col("decimallongitude").isNotNull()
    & ~((F.col("decimallatitude") == 0) & (F.col("decimallongitude") == 0))
    & F.col("decimallatitude").between(-90, 90)
)
print(f"\nrows with usable coordinates: {geo.count():,}")

banner("4. withColumn")
enriched = (geo
            .withColumn("n_issues", F.coalesce(F.size("issue"), F.lit(0)))
            .withColumn("clean", F.col("n_issues") == 0)
            .withColumn("hemisphere",
                        F.when(F.col("decimallatitude") >= 0, "N").otherwise("S"))
            .withColumn("cell_lat", F.floor(F.col("decimallatitude")).cast("int"))
            .withColumn("cell_lon", F.floor(F.col("decimallongitude")).cast("int")))
enriched.select("species", "decade", "hemisphere", "cell_lat", "cell_lon",
                "n_issues", "clean").show(8, truncate=24)

banner("5. groupBy + agg")
by_country = (enriched
              .groupBy("countrycode")
              .agg(F.count("*").alias("records"),
                   F.approx_count_distinct("species").alias("species"),
                   F.avg("decimallatitude").alias("mean_lat"),
                   F.avg(F.col("clean").cast("int")).alias("clean_frac"),
                   F.min("year").alias("first_year"),
                   F.max("year").alias("last_year"))
              .orderBy(F.desc("records")))
for r in by_country.limit(15).collect():
    print(f"  {str(r['countrycode']):<4}{r['records']:>12,}{r['species']:>9,} spp"
          f"{r['mean_lat']:>9.2f} lat   clean {100 * r['clean_frac']:5.1f}%"
          f"   {r['first_year']}-{r['last_year']}")

one = enriched.agg(F.count("*").alias("records"),
                   F.approx_count_distinct("species").alias("species"),
                   F.avg("n_issues").alias("avg_issues")).collect()[0]
print(f"\nno groupBy -> one row: {one['records']:,} records, "
      f"~{one['species']:,} species, {one['avg_issues']:.2f} issues/record")

banner("6. pivot")
BASES = ["HUMAN_OBSERVATION", "PRESERVED_SPECIMEN", "OCCURRENCE",
         "MACHINE_OBSERVATION", "MATERIAL_SAMPLE"]
(enriched.groupBy("decade").pivot("basisofrecord", BASES).count()
         .orderBy("decade").show(20, truncate=20))

banner("7. transformations vs actions")
t0 = time.perf_counter()
lazy = enriched.where(F.col("year") > 2000).select("species").distinct()
print(f"  where + select + distinct (no action)         {(time.perf_counter() - t0) * 1000:6.1f} ms")
timed("count()", lazy.count)
timed("take(3)", lambda: lazy.take(3))
timed("first()", lazy.first)

banner("8. notes")
print("""findings
  - building a chain reads nothing; the first build is slower only because it
    resolves the schema and lists files once
  - count() twice costs the same twice - the DataFrame holds the recipe
  - cache() has a real surcharge and only pays off after several reuses
  - != drops nulls, so == and != do not add back up to the total
  - take/first beat count because they stop as soon as they have enough rows
  - the clean column (zero issues) does not mean good data: most countries sit
    near 0% and SE/NL sit high, which just reflects who trips the one
    informational flag that 82% of records carry
  - HUMAN_OBSERVATION goes from ~34k in the 1950s to ~10.2M in the 2020s while
    PRESERVED_SPECIMEN peaks in the 2010s - the dataset changes character, so
    any trend read off it has to account for that

questions
  - is caching worth it here at all, given each run is a fresh session?
  - approx_count_distinct for species per country - exact enough to report?""")

spark.stop()
print("\ndone.")
