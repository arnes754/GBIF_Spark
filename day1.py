import gbif

SLICE_GB = 2.0

print("=" * 70)
print("ENVIRONMENT")
print("=" * 70)

shards = gbif.list_shards()
print(f"snapshot            : {gbif.SNAPSHOT}")
print(f"bucket              : s3://{gbif.BUCKET}  ({gbif.REGION})")
print(f"shards in snapshot  : {len(shards):,}")
print(f"snapshot size       : {gbif.snapshot_size_gb():.1f} GB")
print(f"hadoop (detected)   : {gbif._hadoop_version()}")

spark = gbif.spark_session(app="gbif-day1")
spark.sparkContext.setLogLevel("ERROR")
sc = spark.sparkContext
print(f"spark version       : {spark.version}")
print(f"master              : {sc.master}")
print(f"parallelism         : {sc.defaultParallelism}")
print(f"driver memory       : {sc.getConf().get('spark.driver.memory')}")

print()
print("=" * 70)
print("READ")
print("=" * 70)
df = gbif.read_slice(spark, target_gb=SLICE_GB)

print()
print("=" * 70)
print("1. COUNT")
print("=" * 70)
n = df.count()
print(f"rows in slice : {n:,}")
print(f"columns       : {len(df.columns)}")

print()
print("=" * 70)
print("2. SCHEMA")
print("=" * 70)
df.printSchema()

print()
print("=" * 70)
print("3. SAMPLE ROWS")
print("=" * 70)
cols = ["gbifid", "species", "countrycode", "year", "basisofrecord",
        "decimallatitude", "decimallongitude"]
df.select(*[c for c in cols if c in df.columns]).show(10, truncate=22)

print("full first row:")
from pyspark.sql import functions as F
row = (df.limit(1)
         .select(*[F.col(c).cast("string").alias(c) for c in df.columns])
         .collect()[0].asDict())
for k, v in row.items():
    print(f"  {k:<34} {v}")

print()
print("=" * 70)
print("4. NOTE: out-of-range timestamps")
print("=" * 70)
tcols = [c for c, t in df.dtypes if t == "timestamp"]
aggs = []
for c in tcols:
    aggs += [F.min(F.col(c).cast("string")).alias(f"{c}__min"),
             F.max(F.col(c).cast("string")).alias(f"{c}__max")]
r = df.select(*aggs).collect()[0].asDict()
print(f"{'column':<18}{'min':<28}{'max'}")
for c in tcols:
    print(f"{c:<18}{str(r[f'{c}__min']):<28}{r[f'{c}__max']}")

spark.stop()
print("\ndone.")
