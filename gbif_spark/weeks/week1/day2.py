"""Day 2 - what is in the GBIF occurrence snapshot.

2 GB slice: 74 of the 9,898 shards in the 2026-09-01 snapshot (~266 GB).

Run: uv run python -m gbif_spark day 2
     SLICE_GB=0.2 uv run python -m gbif_spark day 2
"""
import os

from pyspark.sql import functions as F

from gbif_spark.helpers import gbif

SLICE_GB = float(os.environ.get("SLICE_GB", 2.0))
TOP_N = 15


def banner(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


def pct(part, whole):
    return f"{100.0 * part / whole:5.1f}%" if whole else "    -"


spark = gbif.spark_session(app="gbif-day2")
spark.sparkContext.setLogLevel("ERROR")

paths = gbif.pick_slice(target_gb=SLICE_GB)
df = spark.read.parquet(*paths)

banner("1. shape")
n = df.count()
print(f"files        : {len(paths)}")
print(f"rows         : {n:,}")
print(f"columns      : {len(df.columns)}")
print(f"rows per file: {n // len(paths):,} avg")

types = {}
for _, t in df.dtypes:
    head = t.split("<")[0]
    types[head] = types.get(head, 0) + 1
print("\ntypes: " + ", ".join(f"{t} x{c}" for t, c in
                              sorted(types.items(), key=lambda kv: -kv[1])))

banner("2. grouping the 50 columns")
FAMILIES = {
    "what (taxonomy)": ["kingdom", "phylum", "class", "order", "family", "genus",
                        "species", "infraspecificepithet", "taxonrank",
                        "scientificname", "verbatimscientificname", "taxonkey",
                        "specieskey"],
    "where (space)": ["countrycode", "stateprovince", "locality",
                      "decimallatitude", "decimallongitude",
                      "coordinateuncertaintyinmeters", "coordinateprecision",
                      "elevation", "elevationaccuracy", "depth", "depthaccuracy"],
    "when (time)": ["eventdate", "day", "month", "year", "dateidentified",
                    "lastinterpreted"],
    "who (provenance)": ["datasetkey", "publishingorgkey", "institutioncode",
                         "collectioncode", "catalognumber", "recordnumber",
                         "recordedby", "identifiedby", "license", "rightsholder"],
    "how (record kind)": ["basisofrecord", "occurrencestatus", "individualcount",
                          "establishmentmeans", "typestatus", "mediatype",
                          "occurrenceid", "gbifid"],
    "quality flags": ["issue"],
}
seen = set()
for fam, cols in FAMILIES.items():
    present = [c for c in cols if c in df.columns]
    seen.update(present)
    print(f"\n{fam}  ({len(present)})")
    print("  " + ", ".join(present))
missed = [c for c in df.columns if c not in seen]
if missed:
    print(f"\nnot classified ({len(missed)}): " + ", ".join(missed))

banner("3. nulls per column")
nonnull = df.agg(*[F.count(F.col(c)).alias(c) for c in df.columns]).collect()[0]
rows = sorted(((c, n - nonnull[c]) for c in df.columns), key=lambda kv: -kv[1])
print(f"{'column':<34}{'nulls':>14}  {'null %':>7}")
for c, nulls in rows:
    bar = "#" * int(round(20 * nulls / n)) if n else ""
    print(f"{c:<34}{nulls:>14,}  {pct(nulls, n):>7}  {bar}")

banner("4. distinct values")
CARD = ["gbifid", "datasetkey", "publishingorgkey", "institutioncode", "species",
        "genus", "family", "order", "class", "phylum", "kingdom", "countrycode",
        "stateprovince", "basisofrecord", "license", "occurrencestatus",
        "establishmentmeans", "taxonrank", "year"]
card_cols = [c for c in CARD if c in df.columns]
card = df.agg(*[F.approx_count_distinct(c).alias(c) for c in card_cols]).collect()[0]
print(f"{'column':<24}{'distinct':>14}   {'per row':>8}")
for c in sorted(card_cols, key=lambda c: -card[c]):
    print(f"{c:<24}{card[c]:>14,}   {card[c] / n:>8.4f}")

banner("5. common values")


def top_values(col, k=TOP_N):
    if col not in df.columns:
        return
    print(f"\ntop {k} {col}")
    for r in (df.groupBy(col).count()
                .orderBy(F.desc("count")).limit(k).collect()):
        v = r[col] if r[col] is not None else "<null>"
        print(f"  {str(v)[:40]:<42}{r['count']:>12,}  {pct(r['count'], n)}")


for c in ["basisofrecord", "kingdom", "countrycode", "license", "taxonrank"]:
    top_values(c)

banner("6. time coverage")
yr = df.select("year").where(F.col("year").isNotNull())
stats = yr.agg(F.min("year").alias("lo"), F.max("year").alias("hi"),
               F.count("year").alias("n")).collect()[0]
print(f"has a year : {stats['n']:,}  ({pct(stats['n'], n)})")
print(f"year range : {stats['lo']} .. {stats['hi']}")
print("\nper decade (1900+):")
dec = (yr.where(F.col("year") >= 1900)
         .withColumn("decade", (F.col("year") / 10).cast("int") * 10)
         .groupBy("decade").count().orderBy("decade").collect())
mx = max((r["count"] for r in dec), default=1)
for r in dec:
    print(f"  {r['decade']}s {r['count']:>12,}  {'#' * int(round(40 * r['count'] / mx))}")

banner("7. spatial coverage")
geo = df.agg(
    F.count(F.when(F.col("decimallatitude").isNotNull() &
                   F.col("decimallongitude").isNotNull(), 1)).alias("has_xy"),
    F.count(F.when((F.col("decimallatitude") == 0) &
                   (F.col("decimallongitude") == 0), 1)).alias("null_island"),
    F.min("decimallatitude").alias("lat_lo"), F.max("decimallatitude").alias("lat_hi"),
    F.min("decimallongitude").alias("lon_lo"), F.max("decimallongitude").alias("lon_hi"),
    F.expr("percentile_approx(coordinateuncertaintyinmeters, 0.5)").alias("unc_p50"),
    F.expr("percentile_approx(coordinateuncertaintyinmeters, 0.9)").alias("unc_p90"),
).collect()[0]
print(f"has lat+lon    : {geo['has_xy']:,}  ({pct(geo['has_xy'], n)})")
print(f"exactly (0, 0) : {geo['null_island']:,}")
print(f"lat range      : {geo['lat_lo']} .. {geo['lat_hi']}")
print(f"lon range      : {geo['lon_lo']} .. {geo['lon_hi']}")
print(f"uncertainty    : p50 {geo['unc_p50']} m, p90 {geo['unc_p90']} m")

banner("8. array columns")
if "issue" in df.columns:
    df.select("issue").where(F.size("issue") > 0).show(3, truncate=60)
    iss = (df.withColumn("k", F.coalesce(F.size("issue"), F.lit(0)))
             .agg(F.count(F.when(F.col("k") > 0, 1)).alias("flagged"),
                  F.avg("k").alias("avg"), F.max("k").alias("max"))
             .collect()[0])
    print(f"records with >=1 issue : {iss['flagged']:,}  ({pct(iss['flagged'], n)})")
    print(f"issues per record      : avg {iss['avg']:.2f}, max {iss['max']}")
    print(f"\ntop {TOP_N} issues:")
    for r in (df.select(F.explode("issue").alias("i"))
                .select(F.col("i.array_element").alias("issue"))
                .groupBy("issue").count()
                .orderBy(F.desc("count")).limit(TOP_N).collect()):
        print(f"  {r['issue'][:44]:<46}{r['count']:>12,}  {pct(r['count'], n)}")

banner("9. notes")
print("""findings
  - core fields are well populated (species 8% null, year 4%, lat/lon 3.5%);
    optional measurements are not (depth 99.5%, coordinateprecision 99.8%)
  - 84% HUMAN_OBSERVATION, 76% Animalia, 33% of records from the US
  - volume explodes after 2000 - recording effort, not biology
  - 6,829 records at exactly (0,0), null island, drop them
  - 93% of records carry an issue flag, and the top one (82%,
    CONTINENT_DERIVED_FROM_COORDINATES) is informational, so a flag does not
    mean the record is bad - they have to be read individually
  - approx_count_distinct returned more distinct gbifids than rows, which is
    the ~5% sketch error showing

questions
  - is ~5% sketch error acceptable for anything we would report?
  - are day 2 numbers OK off a 2 GB slice, or should they be off the full 266 GB?
  - are the issue flags documented anywhere beyond the enum names?

project angle  [my pick - happy to be redirected]
  Whether the issue flags are informative or just noise: which publishers and
  datasets generate which flags, and does the mix change over time.
  Why: the data is about itself, so I am not fighting the sampling bias that
  affects every ecological reading of this dataset. Needs explode + groupBy +
  a join, so it covers the Spark I am meant to be learning.
  How it could be wrong: flags are not independent (one bad record trips
  several), and a publisher's flag rate may say more about which GBIF pipeline
  version last touched their records than about the records themselves.
  Runners-up: sampling bias in space/time; the rise of machine observation.""")

spark.stop()
print("\ndone.")
