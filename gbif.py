import json
import os
import pathlib
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

os.environ.setdefault(
    "JAVA_HOME", "/usr/local/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
)

REGION = "eu-central-1"
BUCKET = f"gbif-open-data-{REGION}"
SNAPSHOT = "2026-09-01"
PREFIX = f"occurrence/{SNAPSHOT}/occurrence.parquet/"

_CACHE = pathlib.Path(__file__).parent / ".shard_cache.json"
_S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def list_shards(refresh=False):
    if _CACHE.exists() and not refresh:
        cached = json.loads(_CACHE.read_text())
        if cached.get("prefix") == PREFIX:
            return [(k, s) for k, s in cached["shards"]]

    shards, token = [], None
    while True:
        params = {"list-type": "2", "prefix": PREFIX, "max-keys": "1000"}
        if token:
            params["continuation-token"] = token
        url = f"https://{BUCKET}.s3.amazonaws.com/?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(url, timeout=60) as resp:
            root = ET.fromstring(resp.read())
        for c in root.findall(f"{_S3_NS}Contents"):
            shards.append((c.findtext(f"{_S3_NS}Key"),
                           int(c.findtext(f"{_S3_NS}Size"))))
        if root.findtext(f"{_S3_NS}IsTruncated") != "true":
            break
        token = root.findtext(f"{_S3_NS}NextContinuationToken")

    shards.sort()
    _CACHE.write_text(json.dumps({"prefix": PREFIX, "shards": shards}))
    return shards


def snapshot_size_gb():
    return sum(s for _, s in list_shards()) / 1024**3


def pick_slice(target_gb=10.0, spread=True, refresh=False):
    shards = list_shards(refresh=refresh)
    budget = target_gb * 1024**3

    if not spread:
        chosen, total = [], 0
        for key, size in shards:
            if total + size > budget and chosen:
                break
            chosen.append(key)
            total += size
    else:
        avg = sum(s for _, s in shards) / len(shards)
        want = max(1, min(len(shards), round(budget / avg)))
        step = len(shards) / want
        chosen = [shards[min(int(i * step), len(shards) - 1)][0] for i in range(want)]
        chosen = sorted(set(chosen))
        total = sum(dict(shards)[k] for k in chosen)

    print(f"slice: {len(chosen)} of {len(shards)} shards, {total/1024**3:.1f} GB")
    return [f"s3a://{BUCKET}/{k}" for k in chosen]


def _hadoop_version():
    import pyspark
    jars = pathlib.Path(pyspark.__file__).parent / "jars"
    for jar in jars.glob("hadoop-client-api-*.jar"):
        return jar.stem.replace("hadoop-client-api-", "")
    raise RuntimeError(f"no hadoop-client-api jar under {jars}")


AWS_SDK_VERSION = "2.35.4"


def spark_session(app="gbif", driver_memory="4g", shuffle_partitions=24,
                  max_task_failures=4, cores=None):
    """Every script goes through here, so the two settings that decide how much
    of the machine Spark takes are overridable from the environment without
    editing any script:

        SPARK_CORES=4  uv run python day5.py     # 4 threads instead of all 12
        SPARK_MEM=3g   uv run python day6.py     # smaller JVM heap

    Defaults stay as the caller asked for, so nothing changes unless you set
    them.
    """
    from pyspark.sql import SparkSession

    cores = os.environ.get("SPARK_CORES", cores if cores is not None else "*")
    driver_memory = os.environ.get("SPARK_MEM", driver_memory)

    return (
        SparkSession.builder.appName(app)
        # local[*] means maxFailures = 1: ONE task failure aborts the whole
        # job, with no retry. A single transient DNS blip killed a 10 GB
        # ingest two minutes in ("Task 9 in stage 2.0 failed 1 times;
        # aborting job"). local[*, F] sets maxFailures to F, which is what
        # every cluster mode gives you by default. Reading from S3 over a home
        # connection, transient failures are not exceptional - they are the
        # normal case, and the job has to survive them.
        # local[C,F]: C worker threads, F attempts per task before the job
        # aborts.
        #
        # C ("*" = every core) is how many tasks run AT ONCE, and therefore how
        # many S3 connections are open at once. More is not always better: the
        # link to eu-central-1 tops out around 4 MB/s no matter how many
        # streams share it, so 12 readers get ~0.34 MB/s each and a multi-MB
        # range request starts crossing the S3 client's 60 s timeout. Fewer,
        # fatter readers finish inside the timeout. Parallelism past the
        # bandwidth ceiling buys nothing and costs failures.
        .master(f"local[{cores},{max_task_failures}]")
        .config("spark.driver.memory", driver_memory)
        .config(
            "spark.jars.packages",
            f"org.apache.hadoop:hadoop-aws:{_hadoop_version()},"
            f"software.amazon.awssdk:bundle:{AWS_SDK_VERSION}",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.AnonymousAWSCredentialsProvider",
        )
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{REGION}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        # --- S3 resilience over a home connection -------------------------
        # 318 ApiCallTimeoutExceptions and 65 UnknownHostExceptions killed two
        # 10 GB ingests. The defaults assume datacentre bandwidth and a
        # datacentre's DNS; neither holds here.
        #
        # The 60 s timeouts came from the "analytics accelerator" reader that
        # hadoop-aws 3.5 enables by default. It issues many concurrent range
        # requests, which is exactly wrong when the bottleneck is a shared
        # 4 MB/s pipe - each request gets a sliver of bandwidth and crosses the
        # deadline. Turning it off restores the classic S3A stream, whose
        # timeouts these settings actually control.
        .config("spark.hadoop.fs.s3a.analytics.accelerator.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5m")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "5m")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60s")
        # retries: transient DNS and connection resets are the NORMAL case
        # here, not the exception, so retry far more than the default and back
        # off between attempts rather than hammering.
        .config("spark.hadoop.fs.s3a.attempts.maximum", "20")
        .config("spark.hadoop.fs.s3a.retry.limit", "20")
        .config("spark.hadoop.fs.s3a.retry.interval", "2s")
        .config("spark.hadoop.fs.s3a.retry.throttle.limit", "20")
        # fewer, fatter readers: past the bandwidth ceiling extra concurrency
        # buys nothing and costs timeouts
        .config("spark.hadoop.fs.s3a.connection.maximum", "32")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.adaptive.enabled", "true")
        # The console progress bar writes carriage returns to stdout, so any
        # print() that lands mid-stage is overwritten and any grep over the
        # captured log silently loses it. Measurements are the output of these
        # scripts; they do not get to be eaten by a progress indicator.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_slice(spark, target_gb=10.0, spread=True):
    return spark.read.parquet(*pick_slice(target_gb, spread))


def snapshot_path():
    return f"s3a://{BUCKET}/{PREFIX}"


def read_snapshot(spark):
    """The whole snapshot. Hand Spark the directory, not 9898 paths - it lists
    and bin-packs the files itself, and the plan is built in one shot."""
    return spark.read.parquet(snapshot_path())


def use_minio(spark, endpoint="http://localhost:9000",
              key="minio", secret="minio12345"):
    hc = spark.sparkContext._jsc.hadoopConfiguration()
    hc.set("fs.s3a.endpoint", endpoint)
    hc.set("fs.s3a.access.key", key)
    hc.set("fs.s3a.secret.key", secret)
    hc.set("fs.s3a.path.style.access", "true")
    hc.set("fs.s3a.aws.credentials.provider",
           "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
