"""Curated occurrence table: materialise a slice once, append more later.

The point is to stop paying S3 for every run. `build` pulls shards that are not
already in the table, transforms them, and appends. Run it again with a bigger
--gb and you get MORE data, not the same data twice.

    uv run python curate.py build --gb 2      # first 2 GB
    uv run python curate.py status            # what is in the table
    uv run python curate.py build --gb 4      # +4 GB of DIFFERENT shards
    uv run python curate.py preview           # read it back, sanity numbers
    uv run python curate.py compact           # fix the small-file problem
    uv run python curate.py drop-batch b0003  # undo one append

Layout: data/curated/occurrence_slim/ingest_batch=bNNNN/decade=YYYY/*.parquet

  - `ingest_batch` first so an append is pure file creation: nothing existing is
    touched, and a half-finished run is undone with `drop-batch`. That is the
    whole crash-safety story - no table format, no transaction log.
  - `decade` second because every question in SCOPE.md groups or filters by
    time, so it is the partition that earns its keep at read.
  - Cost: batches x decades directories. `compact` collapses them into one
    batch, and trades away per-batch rollback to do it.

State lives in data/manifests/<table>.json, not in the table. It is local-only,
which is exactly the limitation a real table format (Iceberg, Delta) removes.
"""
import argparse
import datetime
import json
import pathlib
import shutil
import time

from pyspark.sql import functions as F

import gbif

HERE = pathlib.Path(__file__).parent
DEFAULT_OUT = str(HERE / "data" / "curated" / "occurrence_slim")
MANIFEST_DIR = HERE / "data" / "manifests"

# --- transformation policy, all in one place so day 16 can move them ---------
UNCERTAINTY_MAX_M = 10_000.0   # 1-degree cell is ~111 km; 10 km is generous
MIN_PLAUSIBLE_YEAR = 1600      # below this, `year` is a typo more often than not
MAX_UNCERTAINTY_SANE = 20_000_000.0   # > Earth's circumference/2, clearly junk

# Issue flags that make a coordinate unusable for mapping. Deliberately NOT
# "any flag": day 2 found 93% of records flagged and the top flag (82%,
# CONTINENT_DERIVED_FROM_COORDINATES) is informational.
GEO_FATAL = [
    "ZERO_COORDINATE",
    "COORDINATE_INVALID",
    "COORDINATE_OUT_OF_RANGE",
    "COORDINATE_REPROJECTION_FAILED",
    "COUNTRY_COORDINATE_MISMATCH",
    "PRESUMED_SWAPPED_COORDINATE",
    "PRESUMED_NEGATED_LATITUDE",
    "PRESUMED_NEGATED_LONGITUDE",
]

# 50 columns in, these out. Everything dropped is either >99% null (day 2) or
# irrelevant to the one question.
SOURCE_COLUMNS = [
    "gbifid", "datasetkey", "publishingorgkey", "institutioncode", "license",
    "kingdom", "phylum", "class", "order", "family", "genus", "species",
    "specieskey", "taxonrank", "scientificname",
    "countrycode", "decimallatitude", "decimallongitude",
    "coordinateuncertaintyinmeters",
    "eventdate", "year", "month", "lastinterpreted",
    "basisofrecord", "occurrencestatus", "individualcount", "issue",
]


def banner(title):
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


# --- shard selection --------------------------------------------------------
def pick_new_shards(done, target_gb):
    """~target_gb of shards not already ingested, spread evenly over what is
    left. Spread matters: shards are sorted by key, which correlates with how
    GBIF wrote them, so taking the first N in a row is not a random sample.
    Spreading over the REMAINDER keeps every increment representative instead
    of drifting into whatever was left at the end."""
    remaining = [(k, s) for k, s in gbif.list_shards() if k not in done]
    if not remaining:
        return [], 0
    budget = target_gb * 1024**3
    avg = sum(s for _, s in remaining) / len(remaining)
    want = max(1, min(len(remaining), round(budget / avg)))
    step = len(remaining) / want
    keys = sorted({remaining[min(int(i * step), len(remaining) - 1)][0]
                   for i in range(want)})
    size = sum(dict(remaining)[k] for k in keys)
    return keys, size


# --- the transformations ----------------------------------------------------
def curate(df):
    """Raw occurrence rows -> curated rows. Pure: no I/O, no session state, so
    it can be tested against a five-row DataFrame built by hand.

    Rule: this NEVER drops a row. The question in SCOPE.md is *what fraction*
    of GBIF is usable, so the filter has to be a column you can group by, not
    a `where` that deletes the evidence.
    """
    cols = [c for c in SOURCE_COLUMNS if c in df.columns]
    d = df.select(*cols)

    # 1. issue: array<struct<array_element:string>> -> array<string>, sorted.
    #    transform() on a NULL array returns NULL, so coalesce to empty - then
    #    size() is 0 instead of null and array_contains works everywhere.
    #    Sorting makes two records with the same flags compare equal, which is
    #    what the per-publisher flag-vector work needs.
    d = d.withColumn("issues", F.expr(
        "coalesce(array_sort(transform(issue, x -> x.array_element)),"
        " cast(array() as array<string>))"))
    geo = F.array(*[F.lit(x) for x in GEO_FATAL])
    d = (d
         .withColumn("n_issues", F.size("issues"))
         .withColumn("n_geo_issues", F.size(F.array_intersect("issues", geo)))
         .withColumn("has_geo_issue", F.col("n_geo_issues") > 0)
         .drop("issue"))

    # 2. categoricals: trim, upcase, and turn "" into NULL. GBIF ships both;
    #    without this, "" and NULL are two different groups in every groupBy.
    for c, new in [("basisofrecord", "basis"), ("occurrencestatus", "status"),
                   ("countrycode", "country"), ("taxonrank", "taxon_rank")]:
        if c in d.columns:
            t = F.upper(F.trim(F.col(c)))
            d = d.withColumn(new, F.when(t == "", None).otherwise(t)).drop(c)
    d = d.withColumn("is_present", F.col("status") == "PRESENT")

    # `class` and `order` are SQL keywords - renaming here saves backticks in
    # every query written from now on.
    for c, new in [("class", "taxon_class"), ("order", "taxon_order")]:
        if c in d.columns:
            d = d.withColumnRenamed(c, new)

    # 3. time. decade is the partition column, so it must never be NULL:
    #    unknown year becomes 0, an explicit bucket you can count.
    plausible = F.col("year").isNotNull() & (F.col("year") >= MIN_PLAUSIBLE_YEAR)
    d = (d
         .withColumn("year_known", plausible)
         .withColumn("decade", F.when(plausible,
                                      (F.col("year") / 10).cast("int") * 10)
                               .otherwise(F.lit(0)))
         # when GBIF last reprocessed the record - the claim in SCOPE.md says
         # this predicts flags better than the observation does, so it has to
         # be a first-class grouping key, not a timestamp nobody can group on
         .withColumn("interpreted_month",
                     F.date_format(F.to_timestamp("lastinterpreted"), "yyyy-MM"))
         .withColumn("interpreted_year",
                     F.year(F.to_timestamp("lastinterpreted"))))

    # 4. space. Three separate booleans, not one: "no coordinate" and "a
    #    coordinate that is a lie" are different findings and get counted apart.
    lat, lon = F.col("decimallatitude"), F.col("decimallongitude")
    unc = F.col("coordinateuncertaintyinmeters")
    d = (d
         .withColumn("has_coords", lat.isNotNull() & lon.isNotNull())
         .withColumn("null_island", (lat == 0) & (lon == 0))
         .withColumn("coord_valid",
                     lat.isNotNull() & lon.isNotNull()
                     & lat.between(-90, 90) & lon.between(-180, 180)
                     & ~((lat == 0) & (lon == 0)))
         # negative and absurd uncertainties exist; they are not information
         .withColumn("uncertainty_m",
                     F.when((unc >= 0) & (unc <= MAX_UNCERTAINTY_SANE), unc))
         # 1-degree grid: floor, not round, so a cell is [n, n+1) and the cell
         # id names its own south-west corner
         .withColumn("cell_lat", F.when(F.col("coord_valid"),
                                        F.floor(lat).cast("int")))
         .withColumn("cell_lon", F.when(F.col("coord_valid"),
                                        F.floor(lon).cast("int")))
         .withColumnRenamed("decimallatitude", "lat")
         .withColumnRenamed("decimallongitude", "lon")
         .drop("coordinateuncertaintyinmeters"))
    d = (d
         .withColumn("cell_id", F.when(F.col("coord_valid"),
                                       F.concat_ws("_", "cell_lat", "cell_lon")))
         .withColumn("uncertainty_known", F.col("uncertainty_m").isNotNull())
         .withColumn("uncertainty_ok",
                     F.col("uncertainty_m").isNull()
                     | (F.col("uncertainty_m") <= UNCERTAINTY_MAX_M)))

    # 5. the gate. One boolean that encodes the downstream use from SCOPE.md
    #    section 1, so the headline number is `avg(usable_for_mapping)` and
    #    every breakdown is a groupBy away.
    #    uncertainty_ok passes NULL uncertainty on purpose - most records have
    #    none, and dropping them would answer a different question. Keeping
    #    `uncertainty_known` as its own column means the strict variant
    #    (usable_for_mapping AND uncertainty_known) needs no rebuild.
    d = (d
         .withColumn("has_species", F.col("species").isNotNull()
                     & (F.trim("species") != ""))
         .withColumn("usable_for_mapping",
                     F.col("has_species") & F.col("coord_valid")
                     & ~F.col("has_geo_issue") & F.col("year_known")
                     & F.col("is_present") & F.col("uncertainty_ok")))

    # 6. provenance. Which shard a row came from, so a suspicious number can be
    #    traced back to a file instead of to the whole slice.
    return d.withColumn("src_shard",
                        F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1))


# --- reading it back --------------------------------------------------------
def read_table(spark, path=DEFAULT_OUT, batches=None):
    """Read the curated table, or only some of its ingest batches.

    Week 3 is measurement, and a measurement you only run once is a guess. The
    full table is 90 GB / 2.2 billion rows, so every experiment that needs to
    run ten times runs on a named subset instead, and the subset is named in
    the output so nobody compares two numbers from different amounts of data.

        read_table(spark)                      # all 33 batches, 90 GB
        read_table(spark, batches=["b0004"])   # one batch, 5.7 GB

    `basePath` is the part that is easy to get wrong: point Spark at
    `.../ingest_batch=b0004` and it reads that directory as the root, so
    `ingest_batch` stops being a column and `decade` becomes the outer
    partition. Giving it the table root as basePath keeps both columns, which
    keeps partition pruning available on both.
    """
    if not batches:
        return spark.read.parquet(path)
    paths = [f"{path.rstrip('/')}/ingest_batch={b}" for b in batches]
    missing = [p for p in paths if not pathlib.Path(p).is_dir()]
    if missing:
        raise SystemExit(f"no such batch: {', '.join(missing)}")
    return spark.read.option("basePath", path).parquet(*paths)


def batch_names(path=DEFAULT_OUT):
    """Batch directory names, sorted, straight off disk."""
    root = pathlib.Path(path)
    return sorted(d.name.split("=", 1)[1] for d in root.glob("ingest_batch=*"))


def table_bytes(path=DEFAULT_OUT, batches=None):
    """On-disk size of what read_table would read. Reported next to every
    timing, because a time without a data size is not a measurement."""
    root = pathlib.Path(path)
    dirs = ([root / f"ingest_batch={b}" for b in batches] if batches
            else [root])
    return sum(f.stat().st_size for d in dirs for f in d.rglob("*.parquet"))


# --- manifest ---------------------------------------------------------------
def manifest_path(out):
    MANIFEST_DIR.mkdir(parents=True, exist_ok=True)
    name = out.rstrip("/").split("/")[-1]
    return MANIFEST_DIR / f"{name}.json"


def load_manifest(out):
    p = manifest_path(out)
    if p.exists():
        return json.loads(p.read_text())
    return {"table": out, "snapshot": gbif.SNAPSHOT, "batches": []}


def save_manifest(out, m):
    manifest_path(out).write_text(json.dumps(m, indent=2))


def done_shards(m):
    return {k for b in m["batches"] for k in b["shards"]}


# --- commands ---------------------------------------------------------------
def cmd_build(args):
    out = args.out
    m = load_manifest(out)
    if m.get("snapshot") != gbif.SNAPSHOT:
        raise SystemExit(
            f"table was built from snapshot {m['snapshot']}, gbif.py now says "
            f"{gbif.SNAPSHOT}. Appending would mix two snapshots - use a new --out.")

    done = done_shards(m)
    keys, size = pick_new_shards(done, args.gb)
    if not keys:
        print(f"all {len(gbif.list_shards()):,} shards already ingested, nothing to do")
        return

    # "b" prefix on purpose: a bare "0000" directory name gets turned into
    # the integer 0 by Spark's partition type inference, so the column no
    # longer matches the string in the manifest. The prefix keeps it a string.
    batch = f"b{len(m['batches']):04d}"
    print(f"batch {batch}: {len(keys)} new shards, {size / 1024**3:.2f} GB "
          f"({len(done)} shards already in the table)")

    spark = gbif.spark_session(app=f"gbif-curate-{batch}",
                               driver_memory=args.driver_memory)
    spark.sparkContext.setLogLevel("ERROR")
    if args.minio:
        gbif.use_minio(spark)

    t0 = time.perf_counter()
    raw = spark.read.parquet(*[f"s3a://{gbif.BUCKET}/{k}" for k in keys])
    d = curate(raw).withColumn("ingest_batch", F.lit(batch))

    # repartition by decade before the write, or every input task writes into
    # every decade directory: tasks x decades files per batch. One shuffle now
    # buys ~one file per decade. maxRecordsPerFile then splits the two fat
    # decades (2010s, 2020s) back into readable chunks.
    # sortWithinPartitions is not cosmetic: it gives each parquet row group a
    # tight min/max on datasetkey, which is what makes a later predicate on
    # datasetkey skip row groups instead of reading them.
    (d.repartition(F.col("decade"))
      .sortWithinPartitions("datasetkey", "specieskey")
      .write
      .mode("append")
      .partitionBy("ingest_batch", "decade")
      .option("maxRecordsPerFile", args.max_records_per_file)
      .option("compression", args.compression)
      .parquet(out))
    wrote = time.perf_counter() - t0

    rows = spark.read.parquet(out).where(F.col("ingest_batch") == batch).count()
    m["batches"].append({
        "batch": batch,
        "shards": keys,
        "source_bytes": size,
        "rows": rows,
        "seconds": round(wrote, 1),
        "at": datetime.datetime.now().isoformat(timespec="seconds"),
    })
    save_manifest(out, m)
    print(f"\nwrote {rows:,} rows in {wrote:.0f}s -> {out}/ingest_batch={batch}")
    spark.stop()


def cmd_status(args):
    m = load_manifest(args.out)
    all_shards = gbif.list_shards()
    done = done_shards(m)
    banner(f"{args.out}")
    if not m["batches"]:
        print("empty - run:  uv run python curate.py build --gb 2")
        return
    print(f"{'batch':>6}{'shards':>9}{'source GB':>12}{'rows':>14}{'secs':>8}  when")
    for b in m["batches"]:
        print(f"{b['batch']:>6}{len(b['shards']):>9}"
              f"{b['source_bytes'] / 1024**3:>12.2f}{b['rows']:>14,}"
              f"{b['seconds']:>8.0f}  {b['at']}")
    gb = sum(b["source_bytes"] for b in m["batches"]) / 1024**3
    rows = sum(b["rows"] for b in m["batches"])
    total_gb = sum(s for _, s in all_shards) / 1024**3
    print(f"\n{len(done):,} of {len(all_shards):,} shards"
          f"   {gb:.2f} of {total_gb:.0f} GB   ({100 * gb / total_gb:.2f}%)"
          f"   {rows:,} rows")
    on_disk = sum(f.stat().st_size for f in pathlib.Path(args.out).rglob("*.parquet")) \
        if pathlib.Path(args.out).exists() else 0
    if on_disk:
        files = sum(1 for _ in pathlib.Path(args.out).rglob("*.parquet"))
        print(f"on disk: {on_disk / 1024**3:.2f} GB in {files} files"
              f"   ({on_disk / (gb * 1024**3):.0%} of source - column pruning)")


def cmd_preview(args):
    spark = gbif.spark_session(app="gbif-curate-preview")
    spark.sparkContext.setLogLevel("ERROR")
    if args.minio:
        gbif.use_minio(spark)
    d = spark.read.option("mergeSchema", "true").parquet(args.out)

    banner("schema")
    d.printSchema()

    banner("sample")
    d.select("species", "country", "decade", "cell_id", "n_issues",
             "n_geo_issues", "usable_for_mapping").show(10, truncate=22)

    banner("the gate")
    n = d.count()
    s = d.agg(*[F.avg(F.col(c).cast("int")).alias(c) for c in
                ["has_species", "has_coords", "coord_valid", "year_known",
                 "is_present", "uncertainty_known", "uncertainty_ok",
                 "has_geo_issue", "usable_for_mapping"]]).collect()[0]
    print(f"rows: {n:,}\n")
    for c in s.asDict():
        print(f"  {c:<22}{100 * (s[c] or 0):6.2f}%")
    strict = d.where(F.col("usable_for_mapping")
                     & F.col("uncertainty_known")).count()
    print(f"\n  {'usable + unc. known':<22}{100 * strict / n:6.2f}%"
          f"   <- the strict reading of the same gate")

    banner("usable share by decade")
    (d.groupBy("decade")
      .agg(F.count("*").alias("records"),
           F.avg(F.col("usable_for_mapping").cast("int")).alias("usable"))
      .orderBy("decade").show(30))
    spark.stop()


def cmd_compact(args):
    """Rewrite every batch into one. Fixes the file count that repeated appends
    create; costs you per-batch rollback, which is the trade."""
    out = pathlib.Path(args.out)
    if args.minio or str(out).startswith("s3a://"):
        raise SystemExit("compact is local-only for now")
    m = load_manifest(args.out)
    if len(m["batches"]) < 2:
        print("fewer than 2 batches, nothing to compact")
        return

    staging = out.with_name(out.name + "__compacting")
    shutil.rmtree(staging, ignore_errors=True)
    spark = gbif.spark_session(app="gbif-curate-compact")
    spark.sparkContext.setLogLevel("ERROR")
    before = sum(1 for _ in out.rglob("*.parquet"))

    # mergeSchema: batches written before a curate() change have fewer columns
    d = spark.read.option("mergeSchema", "true").parquet(str(out))
    (d.withColumn("ingest_batch", F.lit("b0000"))
      .repartition(F.col("decade"))
      .sortWithinPartitions("datasetkey", "specieskey")
      .write.mode("overwrite").partitionBy("ingest_batch", "decade")
      .option("maxRecordsPerFile", args.max_records_per_file)
      .option("compression", args.compression)
      .parquet(str(staging)))
    spark.stop()

    shutil.rmtree(out)
    staging.rename(out)
    after = sum(1 for _ in out.rglob("*.parquet"))
    m["batches"] = [{
        "batch": "b0000",
        "shards": sorted(done_shards(m)),
        "source_bytes": sum(b["source_bytes"] for b in m["batches"]),
        "rows": sum(b["rows"] for b in m["batches"]),
        "seconds": 0.0,
        "at": datetime.datetime.now().isoformat(timespec="seconds"),
    }]
    save_manifest(args.out, m)
    print(f"compacted {before} files -> {after}")


def cmd_drop_batch(args):
    out = pathlib.Path(args.out)
    d = out / f"ingest_batch={args.batch}"
    m = load_manifest(args.out)
    keep = [b for b in m["batches"] if b["batch"] != args.batch]
    if len(keep) == len(m["batches"]) and not d.exists():
        raise SystemExit(f"no batch {args.batch}")
    shutil.rmtree(d, ignore_errors=True)
    m["batches"] = keep
    save_manifest(args.out, m)
    print(f"dropped batch {args.batch}; its shards are selectable again")


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--minio", action="store_true",
                   help="write via the docker-compose MinIO instead of local disk")
    p.add_argument("--max-records-per-file", type=int, default=2_000_000)
    # The build is network-bound, not heap-bound - it streams shards through a
    # narrow transform and writes them straight back out. A large heap buys
    # nothing here and, on a 16 GB laptop, competes with everything else for
    # hours. Lower it when the machine is busy.
    p.add_argument("--driver-memory", default="4g")
    p.add_argument("--compression", default="snappy",
                   choices=["snappy", "zstd", "gzip", "none"])
    sub = p.add_subparsers(dest="cmd", required=True)

    b = sub.add_parser("build", help="append ~N GB of not-yet-ingested shards")
    b.add_argument("--gb", type=float, default=2.0)
    b.set_defaults(fn=cmd_build)

    sub.add_parser("status", help="what is in the table").set_defaults(fn=cmd_status)
    sub.add_parser("preview", help="read it back and print the headline numbers"
                   ).set_defaults(fn=cmd_preview)
    sub.add_parser("compact", help="collapse all batches into one"
                   ).set_defaults(fn=cmd_compact)

    d = sub.add_parser("drop-batch", help="delete one append")
    d.add_argument("batch")
    d.set_defaults(fn=cmd_drop_batch)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
