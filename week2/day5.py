"""Day 5 - reading and writing at scale.

Days 1-4 read a slice and looked at plans. This day is about the two ends of
the job: getting bytes off disk without reading bytes you do not need, and
putting them back down in a shape the next reader can skip through.

Everything runs against the local curated table (data/curated/occurrence_slim),
so it is repeatable and costs no S3. The numbers that matter are ratios -
bytes read vs bytes on disk, files opened vs files present - and those hold at
any scale.

    uv run python -m week2.day5
    WRITE=0 uv run python -m week2.day5      # skip section 5-6 (they write ~GBs)
    HOLD_FOR_UI=1 uv run python -m week2.day5
"""
import os
import pathlib
import re
import shutil
import time

from pyspark.sql import functions as F, types as T

import bench
import gbif
from bench import banner, gb, measure, show

TABLE = os.environ.get("TABLE", "data/curated/occurrence_slim")
SCRATCH = pathlib.Path(os.environ.get("SCRATCH", "data/scratch/day5"))
DO_WRITE = os.environ.get("WRITE", "1") == "1"
HOLD_FOR_UI = os.environ.get("HOLD_FOR_UI") == "1"

# the six columns a species-distribution map actually needs, out of 47
MAP_COLUMNS = ["specieskey", "cell_lat", "cell_lon", "decade",
               "country", "usable_for_mapping"]


def dir_bytes(p):
    p = pathlib.Path(p)
    return sum(f.stat().st_size for f in p.rglob("*.parquet")) if p.exists() else 0


def dir_files(p):
    p = pathlib.Path(p)
    return sum(1 for _ in p.rglob("*.parquet")) if p.exists() else 0


spark = gbif.spark_session(app="gbif-day5", driver_memory="4g")
spark.sparkContext.setLogLevel("ERROR")
conf = spark.conf

ON_DISK = dir_bytes(TABLE)
N_FILES = dir_files(TABLE)

# ---------------------------------------------------------------------------
banner("1. schema on read")
# Parquet carries its own schema, so `read.parquet` never scans data to infer
# one - but it does have to pick ONE schema for a directory whose files may
# disagree. Each curate() run can add columns; that is the disagreement.
# Warm up first. The very first read of a path pays for the recursive file
# listing and for Hadoop/JVM class loading, and that cost lands on whichever
# variant happens to run first. Timing them cold "proved" mergeSchema was 3x
# FASTER than a plain read, which is nonsense - it was just second.
spark.read.parquet(TABLE).schema

def time_read(fn, reps=3):
    return min(_one(fn) for _ in range(reps))


def _one(fn):
    t0 = time.perf_counter()
    fn()
    return time.perf_counter() - t0


plain = spark.read.parquet(TABLE)
t_plain = time_read(lambda: spark.read.parquet(TABLE).schema)
merged = spark.read.option("mergeSchema", "true").parquet(TABLE)
t_merged = time_read(
    lambda: spark.read.option("mergeSchema", "true").parquet(TABLE).schema)

print(f"table                       : {TABLE}")
print(f"files                       : {N_FILES}   on disk {gb(ON_DISK)}")
print(f"read.parquet()              : {len(plain.columns)} columns, "
      f"schema resolved in {t_plain:.3f}s   (best of 3, listing warm)")
print(f"read.parquet(mergeSchema)   : {len(merged.columns)} columns, "
      f"schema resolved in {t_merged:.3f}s   "
      f"({t_merged / max(t_plain, 1e-9):.1f}x the plain read)")
extra = [c for c in merged.columns if c not in plain.columns]
print(f"columns only mergeSchema sees: {extra or 'none - all batches agree'}")
print(f"""
  mergeSchema reads the FOOTER of every file and unions the schemas; a plain
  read takes one file's schema and assumes the rest match.

  At {N_FILES} files the difference is not measurable ({t_merged / max(t_plain, 1e-9):.1f}x) - so the
  statement is not "mergeSchema is slow", it is that the cost scales with
  FILE COUNT, and this table has {N_FILES} files. The snapshot has 9,898, and
  section 5's partitionBy-without-repartition layout produces 1,152 from a
  single decade. That is when it shows up, and it shows up at PLAN time,
  before a single row is read.

  The correctness argument is the stronger one anyway: without mergeSchema, a
  column added by a later curate() run is silently ABSENT rather than an
  error. Here all batches agree, so nothing is lost - but nothing warned us
  either way.""")

# An explicit schema is the third option: no footer reads at all, and a
# mismatch is your problem rather than Spark's.
explicit = T.StructType([f for f in merged.schema.fields if f.name in MAP_COLUMNS])
t0 = time.perf_counter()
typed = spark.read.schema(explicit).parquet(TABLE)
t_typed = time_read(lambda: spark.read.schema(explicit).parquet(TABLE).schema)
print(f"\nread.schema(explicit)       : {len(typed.columns)} columns, "
      f"{t_typed:.3f}s  <- no footer opened at planning time at all")

df = merged

# ---------------------------------------------------------------------------
banner("2. column pruning, in bytes")
# count() is the wrong benchmark: it needs no column values at all, so Spark
# answers it from parquet row-group metadata and every variant looks the same.
# Show that first, because it is the trap.
runs = []
for label, d in [("count(), all 47 columns", df),
                 ("count(), 6 columns", df.select(*MAP_COLUMNS)),
                 ("count(), 1 column", df.select("specieskey"))]:
    with measure(spark, label) as m:
        d.where(F.col("decade") == 2010).count()
    runs.append(m)
show(runs)
print("""
  Identical, and not because pruning failed - count() needs no column VALUES
  at all, so there is nothing to prune. It is the worst possible benchmark for
  this. Pruning only shows up when values must actually be decoded:""")

runs = []
def checksum(d, cols):
    """Touch every requested column so none can be pruned away as unused.

    xxhash64 over all of them at once, and max() rather than sum(): adding 47
    hashes overflows a bigint, and Spark 4 runs with ANSI mode ON by default,
    so an overflow is an ERROR rather than the silent wraparound Spark 3 gave.
    A benchmark that dies is better than one that quietly wraps, but it is a
    real behaviour change to know about."""
    expr = F.xxhash64(*[F.col(c) for c in cols]).alias("x")
    return d.where(F.col("decade") == 2010).select(expr).agg(F.max("x"))

for label, cols in [("hash all 47 columns", df.columns),
                    ("hash 6 columns", MAP_COLUMNS),
                    ("hash 1 column", ["specieskey"])]:
    with measure(spark, label) as m:
        checksum(df, cols).collect()
    runs.append(m)
show(runs)
wide, narrow_c = runs[0], runs[-1]
print(f"""
  {wide['seconds']:.1f}s for 47 columns, {runs[1]['seconds']:.1f}s for 6, {narrow_c['seconds']:.1f}s for 1 - a {wide['seconds'] / max(narrow_c['seconds'], 1e-9):.0f}x spread.
  Column pruning is unambiguously real and it is the single biggest lever on
  this table.

  And now the thing that cost an hour: look at the `read` column. It is
  IDENTICAL on all three rows. Spark's "size of files read" metric is the
  total size of the files the scan OPENED, not the bytes it decoded out of
  them. Parquet is columnar, so the reader seeks to the column chunks it needs
  and skips the rest - but no metric Spark publishes counts those skipped
  bytes, because the file was still "read".

  So: partition pruning is visible in bytes (section 3 - fewer files opened).
  Column pruning is NOT visible in bytes at all, only in time. Anyone
  measuring column pruning by watching a byte counter will conclude it does
  not work. It works; the counter is answering a different question.

  For extrapolating to the full 266 GB snapshot this matters directly: the
  has to be built from file sizes and column WIDTH, because the run itself will
  not report the number.""")

# ---------------------------------------------------------------------------
banner("3. partition pruning: files opened, not bytes guessed")
cases = [
    ("no filter", df),
    ("decade = 2010", df.where(F.col("decade") == 2010)),
    ("decade >= 2000", df.where(F.col("decade") >= 2000)),
    ("decade = 2010 and batch b0000",
     df.where((F.col("decade") == 2010) & (F.col("ingest_batch") == "b0000"))),
    # country is NOT a partition column - this one cannot prune directories
    ("country = 'SE'", df.where(F.col("country") == "SE")),
]
# The numbers come from the Scan operator's own metrics, not from scraping
# the plan text - the plan's wording changes between Spark versions and the
# regex that read it silently returned None on Spark 4.
print(f"  {'filter':<34}{'files':>7}{'dirs':>6}{'bytes read':>12}{'skipped':>10}")
for label, d in cases:
    m = bench.scan_of(spark, d)
    print(f"  {label:<34}{int(m['files_read']):>7}{int(m['partitions_read']):>6}"
          f"{gb(m['input_bytes']):>12}"
          f"{100 * (1 - m['files_read'] / N_FILES):>9.0f}%")

s = bench.scan_stats(df.select(*MAP_COLUMNS)
                       .where((F.col("decade") == 2010)
                              & (F.col("usable_for_mapping"))))
print(f"""
  PartitionFilters : {s['PartitionFilters']}
  PushedFilters    : {s['PushedFilters']}
  ReadSchema       : {s['ReadSchema']}

  Three different mechanisms, and the plan names them separately:
    PartitionFilters - directories eliminated from the listing. Free.
    PushedFilters    - predicates handed to the parquet reader, which uses
                       row-group min/max statistics to skip row groups. Cheap.
    ReadSchema       - the columns actually decoded. This is column pruning.
  A predicate that appears in neither list is applied by Spark AFTER the read,
  which means the bytes were paid for.""")

# ---------------------------------------------------------------------------
banner("4. row-group skipping: the payoff for sorting at write time")
# curate.py sorts each partition by datasetkey before writing, which tightens
# each row group's min/max on that column. A predicate on datasetkey should
# therefore skip row groups; the same predicate on an UNSORTED column cannot,
# because every row group's range covers everything.
one_key = df.select("datasetkey").where(F.col("decade") == 2010) \
            .limit(1).collect()[0][0]
# Both predicates match (almost) nothing; the question is how much has to be
# READ to establish that. A column the file is sorted on has tight row-group
# min/max, so most groups can be excluded from the footer alone.
runs = []
base = df.where(F.col("decade") == 2010)
for label, d in [
        ("no predicate", base),
        ("sorted col: datasetkey = x",
         base.where(F.col("datasetkey") == one_key)),
        ("unsorted col: gbifid = 1", base.where(F.col("gbifid") == 1)),
        ("unsorted col: species = x",
         base.where(F.col("species") == "Vulpes vulpes"))]:
    with measure(spark, label) as m:
        d.select(F.sum(F.hash("gbifid"))).collect()
    runs.append(m)
show(runs)
base_rows = runs[0]["scan_rows"]
srt, uns = runs[1], runs[2]
print(f"""
  Read `rows scanned`, not `read` - section 2 explained why bytes cannot show
  this. Rows scanned is what the reader actually emitted after row-group
  statistics were consulted:

    no predicate        {int(base_rows):>12,}
    datasetkey (sorted) {int(srt['scan_rows']):>12,}   {100 * (1 - srt['scan_rows'] / base_rows):>5.1f}% of row groups skipped
    gbifid (unsorted)   {int(uns['scan_rows']):>12,}   {100 * (1 - uns['scan_rows'] / base_rows):>5.1f}% skipped
    species (unsorted)  {int(runs[3]['scan_rows']):>12,}   {100 * (1 - runs[3]['scan_rows'] / base_rows):>5.1f}% skipped

  The sorted column skips the most, which is the mechanism working - but the
  effect is {100 * (1 - srt['scan_rows'] / base_rows):.0f}%, not the order of magnitude the idea promises. Two
  reasons:

    - curate.py sorts WITHIN each decade partition. The ordering is local, so
      every file's datasetkey range still overlaps every other file's.
    - a row group is ~1M rows here. A predicate matching one dataset still
      touches any row group that dataset appears in, and eBird-sized datasets
      appear in all of them.

  A global sort would skip far more and would cost a full shuffle at write
  time. That is a real trade, and for THIS table the write-time cost is not
  obviously worth it - which is a more useful conclusion than "sorting makes
  reads fast".""")

# ---------------------------------------------------------------------------
if DO_WRITE:
    banner("5. write layouts: the same data, four ways")
    shutil.rmtree(SCRATCH, ignore_errors=True)
    SCRATCH.mkdir(parents=True, exist_ok=True)

    # one decade, so each layout writes ~seconds rather than ~minutes
    src = df.select(*MAP_COLUMNS, "datasetkey", "species", "gbifid") \
            .where(F.col("decade") == 2000).cache()
    n = src.count()
    print(f"source: decade 2000, {n:,} rows, {len(src.columns)} columns\n")

    layouts = [
        ("as-is (one file per task)", lambda d: d, {}),
        ("coalesce(1)",             lambda d: d.coalesce(1), {}),
        ("partitionBy(country)",    lambda d: d, {"partitionBy": ["country"]}),
        ("repartition+partitionBy", lambda d: d.repartition(F.col("country")),
         {"partitionBy": ["country"]}),
        ("partitionBy(country,decade) - the mistake",
         lambda d: d, {"partitionBy": ["country", "decade"]}),
    ]
    results = []
    for label, shape, opts in layouts:
        out = SCRATCH / re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")[:28]
        w = shape(src).write.mode("overwrite").option("compression", "snappy")
        if "partitionBy" in opts:
            w = w.partitionBy(*opts["partitionBy"])
        with measure(spark, label) as m:
            w.parquet(str(out))
        m["files"] = dir_files(out)
        m["size"] = dir_bytes(out)
        m["dirs"] = sum(1 for p in out.rglob("*") if p.is_dir())
        results.append((label, m, out))

    bench.show([m for _, m, _ in results],
               [("layout", "label", bench.TXT), ("secs", "seconds", bench.SEC),
                ("files", "files", bench.NUM), ("dirs", "dirs", bench.NUM),
                ("size", "size", bench.BYTES)])
    print("""
  partitionBy without a matching repartition is the classic file explosion:
  every input task can produce a row for every country, so you get
  tasks x countries files. One shuffle before the write collapses that to
  ~one file per country. Adding a second partition column multiplies the
  directory count again for no read benefit here - decade is constant in this
  slice, so it buys nothing and costs a directory level.

  Small files are not just an inode problem: section 1 showed schema
  resolution cost scaling with file count, and every file is a separate task,
  a separate footer read and a separate S3 request.""")

    print("\n  reading each layout back, filtered to one country:")
    reads = []
    for label, _, out in results:
        d = spark.read.parquet(str(out))
        with measure(spark, label) as m:
            d.where(F.col("country") == "US").agg(F.count("*")).collect()
        reads.append(m)
    bench.show(reads)
    print("""
  The partitioned layouts read a fraction of the bytes: the filter became a
  directory listing. That is the whole argument for partitionBy - not tidiness,
  but turning a predicate into a path.""")

    # -----------------------------------------------------------------------
    banner("6. compression codecs")
    codecs = []
    for codec in ["snappy", "zstd", "gzip", "uncompressed"]:
        out = SCRATCH / f"codec_{codec}"
        with measure(spark, f"write {codec}") as m:
            src.coalesce(4).write.mode("overwrite") \
               .option("compression", codec).parquet(str(out))
        m["size"] = dir_bytes(out)
        with measure(spark, f"read {codec}") as r:
            spark.read.parquet(str(out)).agg(F.sum("gbifid")).collect()
        m["read_s"] = r["seconds"]
        m["read_bytes"] = r["input_bytes"]
        codecs.append(m)
    bench.show(codecs, [("codec", "label", bench.TXT),
                        ("write s", "seconds", bench.SEC),
                        ("size", "size", bench.BYTES),
                        ("read s", "read_s", bench.SEC),
                        ("bytes read", "read_bytes", bench.BYTES)])
    print("""
  zstd is usually 20-40% smaller than snappy at a similar read speed, gzip is
  smaller still and slow, and uncompressed is only faster if you are not
  I/O-bound - which, reading from S3, you always are. The number that decides
  it is not the write time, it is `size`: on a 266 GB snapshot every percent
  is 2.6 GB you do not transfer.""")
    src.unpersist()
else:
    print("\n(WRITE=0: sections 5-6 skipped)")

# ---------------------------------------------------------------------------
banner("7. notes")
print(f"""findings
  - parquet is schema-on-read in the sense that the schema comes from the file,
    not from you - but "the file" is one arbitrary file unless you pay for
    mergeSchema, and that cost scales with FILE COUNT, not data size
  - pruning has three separate mechanisms and the plan names all three
    (PartitionFilters / PushedFilters / ReadSchema). Conflating them is how
    people conclude "predicate pushdown doesn't work"
  - count() is the worst possible benchmark for column pruning: it needs no
    column values, so every variant looks identical
  - "size of files read" is files OPENED, not bytes decoded. Partition pruning
    moves that number; column pruning does not move it at all, and is only
    visible as time. This is the most misleading metric in the SQL tab
  - sorting at write time does skip row groups, but sortWithinPartitions gives
    a LOCAL ordering, so the effect here is percent, not orders of magnitude.
    Measured before believed
  - partitionBy without repartition = tasks x distinct-values files
  - a partition column you never filter on costs a directory level and buys
    nothing

gotchas hit
  - bench.measure() around a lazy DataFrame measures zero. The block has to
    contain an action or you are timing plan construction
  - the SQL listener is ASYNCHRONOUS. Reading metrics the instant an action
    returns attributed every block's work to the NEXT block - a table that was
    entirely wrong while looking entirely plausible. bench._settle() waits for
    the executions to land and stop changing
  - the first read of a path pays for the recursive file listing. Timing it
    cold "proved" mergeSchema was faster than a plain read. Warm up, then take
    the best of several
  - Spark 4 has ANSI mode ON by default, so an integer overflow that Spark 3
    wrapped silently is now a job-killing error

questions
  - is 2M maxRecordsPerFile right? section 5 says file COUNT drives planning
    cost, so bigger files are better until a single task gets too big
  - would partitioning by country instead of decade serve the queries better?
    Every question groups by time, but the skew section of day 8 will say
    whether country is even usable as a partition key

to try
  - the same six sections against s3a:// instead of local disk; the ratios
    should hold and the wall times should not""")

if HOLD_FOR_UI:
    input("\nSpark UI on http://localhost:4040 - Enter to stop...")
spark.stop()
print("\ndone.")
