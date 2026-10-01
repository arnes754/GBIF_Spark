"""The dataset registry: GBIF's dimension table, fetched once and cached.

The occurrence snapshot knows `datasetkey` and `publishingorgkey` and nothing
else. Who published a record, from which country, under what licence, lives in
the GBIF registry API. SCOPE.md's claim is about publishers, so without this
join there is no claim to test.

    uv run python registry.py keys       # distinct datasetkeys in the table
    uv run python registry.py fetch      # /dataset/{key} + /organization/{key}
    uv run python registry.py build      # jsonl -> parquet dimension table
    uv run python registry.py status
    uv run python registry.py show

Why key-directed fetch and not pagination - this cost an afternoon:

  The obvious approach is to page /dataset/search?type=OCCURRENCE, 54 pages of
  1000. The first pages return in ~1 s. By offset 30,000 a single page had not
  returned after two minutes. The endpoint is Elasticsearch-backed and deep
  pagination makes the engine sort and discard `offset` documents on every
  shard for every request, so cost grows with offset, not with page size. Deep
  paging a search API is a known anti-pattern; a search_after cursor is the
  supported way, and GBIF does not expose one here.

  The fix is to stop asking for data we do not need. The occurrence slice
  references ~1,500 distinct datasets, not 54,000. Fetching /dataset/{key} for
  exactly those keys is ~0.25 s each, embarrassingly parallel, cacheable,
  incremental, and has no offset in it at all. It is also the shape that scales:
  at 266 GB the key list grows, the method does not change.

  This is the dimension-table lesson in general - build the dimension for the
  keys the fact table actually has.
"""
import argparse
import concurrent.futures
import json
import pathlib
import threading
import time
import urllib.error
import urllib.request

from pyspark.sql import functions as F, types as T

import bench
import gbif

HERE = pathlib.Path(__file__).parent
REG = HERE / "data" / "registry"
KEYS = REG / "keys.json"
DATASETS = REG / "datasets.jsonl"
ORGS = REG / "organizations.jsonl"
DEFAULT_TABLE = str(HERE / "data" / "curated" / "occurrence_slim")
DEFAULT_OUT = str(HERE / "data" / "curated" / "dataset_dim")

API = "https://api.gbif.org/v1"
WORKERS = 12

SCHEMA = T.StructType([
    T.StructField("datasetkey", T.StringType()),
    T.StructField("dataset_title", T.StringType()),
    T.StructField("dataset_type", T.StringType()),
    T.StructField("license_url", T.StringType()),
    T.StructField("publisher_key", T.StringType()),
    T.StructField("installation_key", T.StringType()),
    T.StructField("dataset_created", T.StringType()),
    T.StructField("dataset_modified", T.StringType()),
    T.StructField("publisher_title", T.StringType()),
    T.StructField("publisher_country", T.StringType()),
    T.StructField("publisher_city", T.StringType()),
    T.StructField("endorsing_node_key", T.StringType()),
    T.StructField("publisher_datasets", T.LongType()),
])


def _get(path, tries=4):
    """One GET with backoff. 404 is a real answer - a dataset key can be in the
    snapshot and deleted from the registry - so it returns None rather than
    raising, and `build` keeps it as a null-filled dimension row."""
    for attempt in range(tries):
        try:
            with urllib.request.urlopen(f"{API}/{path}", timeout=60) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return None
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)
        except Exception:
            if attempt == tries - 1:
                raise
            time.sleep(2 ** attempt)


def _fetch_many(kind, keys, slim, label):
    """Fetch /{kind}/{key} for every key, WORKERS at a time, and slim each
    response down before it is kept. The full /dataset response is ~33 KB of
    contacts, endpoints and coverage descriptions; the six fields we join on
    are ~200 bytes. Slimming at the boundary is why the cache is megabytes
    instead of gigabytes."""
    out, failed = [], []
    t0 = time.perf_counter()
    lock = threading.Lock()
    done = [0]

    def one(key):
        try:
            rec = _get(f"{kind}/{key}")
        except Exception as e:
            with lock:
                failed.append((key, repr(e)))
            return None
        with lock:
            done[0] += 1
            if done[0] % 200 == 0 or done[0] == len(keys):
                rate = done[0] / max(time.perf_counter() - t0, 1e-9)
                print(f"  {label}: {done[0]}/{len(keys)}  {rate:.0f}/s  "
                      f"{time.perf_counter() - t0:.0f}s", flush=True)
        return slim(key, rec)

    with concurrent.futures.ThreadPoolExecutor(WORKERS) as pool:
        for r in pool.map(one, keys):
            if r is not None:
                out.append(r)
    if failed:
        print(f"  {len(failed)} {kind} keys failed, first: {failed[0]}")
    return out


def _slim_dataset(key, r):
    r = r or {}
    return {
        "datasetkey": key,
        "dataset_title": (r.get("title") or "").strip() or None,
        "dataset_type": r.get("type"),
        "license_url": r.get("license"),
        "publisher_key": r.get("publishingOrganizationKey"),
        "installation_key": r.get("installationKey"),
        "dataset_created": r.get("created"),
        "dataset_modified": r.get("modified"),
        "found": bool(r),
    }


def _slim_org(key, r):
    r = r or {}
    return {
        "publisher_key": key,
        "publisher_title": (r.get("title") or "").strip() or None,
        "publisher_country": r.get("country"),
        "publisher_city": (r.get("city") or "").strip() or None,
        "endorsing_node_key": r.get("endorsingNodeKey"),
        "publisher_datasets": r.get("numPublishedDatasets"),
        "found": bool(r),
    }


def _read_jsonl(p):
    return [json.loads(l) for l in p.open()] if p.exists() else []


def _write_jsonl(p, rows):
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w") as f:
        for r in rows:
            f.write(json.dumps(r) + "\n")


# --- commands ---------------------------------------------------------------
def cmd_keys(args):
    """The distinct join keys the fact table actually contains. This is a
    shuffle over the whole table, so it is its own command and its own cache -
    nobody should pay for it inside the fetch."""
    spark = gbif.spark_session(app="gbif-registry-keys", driver_memory="4g")
    spark.sparkContext.setLogLevel("ERROR")
    d = spark.read.option("mergeSchema", "true").parquet(args.table)
    keys = sorted(r[0] for r in d.select("datasetkey").distinct().collect()
                  if r[0])
    spark.stop()
    KEYS.parent.mkdir(parents=True, exist_ok=True)
    KEYS.write_text(json.dumps({"table": args.table, "keys": keys}, indent=0))
    print(f"{len(keys):,} distinct datasetkeys in {args.table} -> {KEYS}")


def cmd_fetch(args):
    if not KEYS.exists():
        raise SystemExit("no key list - run: uv run python registry.py keys")
    keys = json.loads(KEYS.read_text())["keys"]

    have = {r["datasetkey"] for r in _read_jsonl(DATASETS)} if not args.refresh else set()
    todo = [k for k in keys if k not in have]
    print(f"datasets: {len(keys):,} wanted, {len(have):,} cached, "
          f"{len(todo):,} to fetch ({WORKERS} at a time)")
    rows = _read_jsonl(DATASETS) if not args.refresh else []
    if todo:
        rows += _fetch_many("dataset", todo, _slim_dataset, "datasets")
        _write_jsonl(DATASETS, rows)
    missing = sum(1 for r in rows if not r.get("found"))
    print(f"  {len(rows):,} datasets cached in {DATASETS} "
          f"({DATASETS.stat().st_size / 1024**2:.1f} MB, {missing} not in registry)")

    org_keys = sorted({r["publisher_key"] for r in rows if r.get("publisher_key")})
    have_o = {r["publisher_key"] for r in _read_jsonl(ORGS)} if not args.refresh else set()
    todo_o = [k for k in org_keys if k not in have_o]
    print(f"\norganizations: {len(org_keys):,} referenced, {len(have_o):,} cached, "
          f"{len(todo_o):,} to fetch")
    orows = _read_jsonl(ORGS) if not args.refresh else []
    if todo_o:
        orows += _fetch_many("organization", todo_o, _slim_org, "orgs")
        _write_jsonl(ORGS, orows)
    print(f"  {len(orows):,} organizations cached in {ORGS}")


def dimension(spark):
    """jsonl -> DataFrame. Separate from the write so day 7 can call it.

    The two caches are joined IN PYTHON, not in Spark: they are thousands of
    rows, the driver already holds them, and starting a shuffle to join two
    dicts would be the kind of thing this project is supposed to teach you not
    to do.
    """
    ds = _read_jsonl(DATASETS)
    orgs = {r["publisher_key"]: r for r in _read_jsonl(ORGS)}
    if not ds:
        raise SystemExit("no registry cache - run: uv run python registry.py fetch")

    rows = []
    for r in ds:
        o = orgs.get(r.get("publisher_key") or "", {})
        rows.append((
            r["datasetkey"], r["dataset_title"], r["dataset_type"],
            r["license_url"], r.get("publisher_key"), r.get("installation_key"),
            r.get("dataset_created"), r.get("dataset_modified"),
            o.get("publisher_title"), o.get("publisher_country"),
            o.get("publisher_city"), o.get("endorsing_node_key"),
            o.get("publisher_datasets"),
        ))

    d = spark.createDataFrame(rows, SCHEMA)
    # Licence URLs are unreadable in a groupBy. Two families exist and only
    # one of them says "licenses":
    #   .../licenses/by/4.0/legalcode      -> CC_BY_4_0
    #   .../publicdomain/zero/1.0/legalcode -> CC0_1_0
    # A regex written for the first family silently bucketed every CC0 dataset
    # (42% of them) into a group called LEGALCODE, because the fallback took
    # the last path segment. The wrong answer looked like a real category,
    # which is the dangerous kind of wrong.
    lic = F.regexp_extract("license_url", r"licenses/([^/]+)/([^/]+)", 0)
    pdm = F.regexp_extract("license_url", r"publicdomain/(zero|mark)/([^/]+)", 0)
    d = d.withColumn(
        "license",
        F.when(F.col("license_url").isNull() | (F.trim("license_url") == ""),
               "UNSPECIFIED")
         .when(lic != "", F.upper(F.regexp_replace(
             F.regexp_replace(F.concat(F.lit("CC_"), lic), "licenses/", ""),
             "[/.]", "_")))
         .when(pdm != "", F.upper(F.regexp_replace(F.regexp_replace(
             F.regexp_replace(pdm, "publicdomain/zero", "CC0"),
             "publicdomain/mark", "PDM"), "[/.]", "_")))
         .otherwise(F.lit("OTHER")))
    for c in ("publisher_country", "dataset_type"):
        d = d.withColumn(c, F.when(F.trim(F.col(c)) == "", None)
                          .otherwise(F.upper(F.trim(F.col(c)))))
    d = d.withColumn("dataset_year", F.year(F.to_timestamp(
        F.substring("dataset_created", 1, 19))))
    return d.withColumn("publisher_key",
                        F.coalesce("publisher_key", F.lit("UNKNOWN")))


def cmd_build(args):
    spark = gbif.spark_session(app="gbif-registry", driver_memory="2g")
    spark.sparkContext.setLogLevel("ERROR")
    d = dimension(spark)
    n = d.count()
    # one file on purpose: ~1 MB. Splitting it only makes Spark open more
    # handles, and a single small file keeps the parquet footer's size
    # statistic well under autoBroadcastJoinThreshold - which is what makes
    # day 7's broadcast happen without a hint.
    d.coalesce(1).write.mode("overwrite").option("compression", "snappy") \
     .parquet(args.out)
    size = sum(f.stat().st_size for f in pathlib.Path(args.out).rglob("*.parquet"))
    thr = bench.conf_bytes(spark, "spark.sql.autoBroadcastJoinThreshold")
    print(f"wrote {n:,} datasets -> {args.out}  ({size / 1024**2:.2f} MB)")
    print(f"autoBroadcastJoinThreshold = {thr / 1024**2:.0f} MB "
          f"-> Spark {'WILL' if size < thr else 'will NOT'} broadcast this unprompted")
    spark.stop()


def cmd_status(args):
    for p, what in [(KEYS, "key list"), (DATASETS, "datasets"), (ORGS, "organizations")]:
        if p.exists():
            n = (len(json.loads(p.read_text())["keys"]) if p is KEYS
                 else sum(1 for _ in p.open()))
            print(f"  {what:<16}{n:>8,} rows   {p.stat().st_size / 1024**2:>7.2f} MB  {p}")
        else:
            print(f"  {what:<16}{'absent':>8}")
    o = pathlib.Path(args.out)
    if o.exists():
        size = sum(f.stat().st_size for f in o.rglob("*.parquet"))
        print(f"  {'dimension':<16}{'':>8}   {size / 1024**2:>7.2f} MB  {args.out}")
    else:
        print(f"  {'dimension':<16}{'absent':>8}")


def cmd_show(args):
    spark = gbif.spark_session(app="gbif-registry-show", driver_memory="2g")
    spark.sparkContext.setLogLevel("ERROR")
    d = spark.read.parquet(args.out)
    print(f"\n{d.count():,} datasets   "
          f"{d.select('publisher_key').distinct().count():,} publishers   "
          f"{d.select('publisher_country').distinct().count():,} publisher countries\n")
    d.groupBy("publisher_title", "publisher_country") \
     .agg(F.count("*").alias("datasets")) \
     .orderBy(F.desc("datasets")).show(15, truncate=44)
    d.groupBy("license").count().orderBy(F.desc("count")).show(10, truncate=30)
    d.groupBy("dataset_type").count().orderBy(F.desc("count")).show(10)
    spark.stop()


def main():
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--table", default=DEFAULT_TABLE)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("keys", help="distinct datasetkeys in the fact table"
                   ).set_defaults(fn=cmd_keys)
    f = sub.add_parser("fetch", help="fetch those keys from the registry API")
    f.add_argument("--refresh", action="store_true")
    f.set_defaults(fn=cmd_fetch)
    sub.add_parser("build", help="jsonl -> parquet dimension").set_defaults(fn=cmd_build)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    sub.add_parser("show").set_defaults(fn=cmd_show)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
