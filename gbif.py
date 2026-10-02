"""Java lookup, S3 shard listing and the Spark session builder."""
import json
import os
import pathlib
import shutil
import subprocess
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

# PySpark 4.2 needs Java 17 or 21.
SUPPORTED_JAVA = (17, 21)

INSTALL_HINT = """Spark needs a Java 17 or 21 JDK and none was found.

  macOS     brew install openjdk@17
  Debian    sudo apt install openjdk-17-jdk
  Fedora    sudo dnf install java-17-openjdk-devel

Or point JAVA_HOME at one:

  export JAVA_HOME=/path/to/jdk-17"""


def _java_version(home):
    """Major version of the JDK at `home`, or None if there is no java there."""
    java = pathlib.Path(home) / "bin" / "java"
    if not java.is_file():
        return None
    try:
        out = subprocess.run([str(java), "-version"], capture_output=True,
                             text=True, timeout=30).stderr
    except (OSError, subprocess.SubprocessError):
        return None
    # 'openjdk version "17.0.20.1"' -> 17, '"1.8.0_392"' -> 8
    for token in out.split('"'):
        parts = token.split(".")
        if parts and parts[0].isdigit():
            major = int(parts[0])
            return int(parts[1]) if major == 1 and len(parts) > 1 else major
    return None


def _candidate_java_homes():
    """Places a JDK might be, best guess first."""
    if os.environ.get("JAVA_HOME"):
        yield pathlib.Path(os.environ["JAVA_HOME"])

    # macOS keeps a registry of installed JDKs.
    if pathlib.Path("/usr/libexec/java_home").exists():
        for version in SUPPORTED_JAVA:
            try:
                out = subprocess.run(["/usr/libexec/java_home", "-v", str(version)],
                                     capture_output=True, text=True, timeout=30)
            except (OSError, subprocess.SubprocessError):
                continue
            if out.returncode == 0 and out.stdout.strip():
                yield pathlib.Path(out.stdout.strip())

    # Homebrew JDKs are not in that registry. /usr/local is Intel,
    # /opt/homebrew is Apple Silicon.
    for brew in ("/usr/local/opt", "/opt/homebrew/opt"):
        for version in SUPPORTED_JAVA:
            yield pathlib.Path(brew) / f"openjdk@{version}" / "libexec" / \
                "openjdk.jdk" / "Contents" / "Home"

    for root in ("/usr/lib/jvm", "/usr/java",
                 os.path.expanduser("~/.sdkman/candidates/java")):
        directory = pathlib.Path(root)
        if directory.is_dir():
            yield from sorted(directory.iterdir())

    # Fall back to whatever java is on PATH: bin/java -> home is two up.
    found = shutil.which("java")
    if found:
        yield pathlib.Path(found).resolve().parent.parent


def find_java_home():
    """Path of a Java 17 or 21 JDK, or raise with install instructions."""
    seen, wrong_version = set(), []
    for home in _candidate_java_homes():
        if home in seen:
            continue
        seen.add(home)
        version = _java_version(home)
        if version in SUPPORTED_JAVA:
            return str(home)
        if version is not None:
            wrong_version.append((version, home))

    message = INSTALL_HINT
    if wrong_version:
        found = ", ".join(f"Java {v} at {h}" for v, h in wrong_version[:3])
        message += f"\n\nFound but unusable: {found}"
    raise RuntimeError(message)


# PySpark reads JAVA_HOME when it starts the JVM, so set it on import.
# A JAVA_HOME that is already set is tried first and wins if it is valid.
os.environ["JAVA_HOME"] = find_java_home()

REGION = "eu-central-1"
BUCKET = f"gbif-open-data-{REGION}"
SNAPSHOT = "2026-09-01"
PREFIX = f"occurrence/{SNAPSHOT}/occurrence.parquet/"

_CACHE = pathlib.Path(__file__).parent / ".shard_cache.json"
_S3_NS = "{http://s3.amazonaws.com/doc/2006-03-01/}"


def list_shards(refresh=False):
    """(key, size) for every parquet shard in the snapshot. Cached to disk."""
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
    """Pick shard paths up to target_gb. spread=True takes them evenly across
    the snapshot instead of the first N, so the slice stays representative."""
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
    """Match hadoop-aws to the hadoop-client jar PySpark ships with."""
    import pyspark
    jars = pathlib.Path(pyspark.__file__).parent / "jars"
    for jar in jars.glob("hadoop-client-api-*.jar"):
        return jar.stem.replace("hadoop-client-api-", "")
    raise RuntimeError(f"no hadoop-client-api jar under {jars}")


AWS_SDK_VERSION = "2.35.4"


def spark_session(app="gbif", driver_memory="4g", shuffle_partitions=24,
                  max_task_failures=4, cores=None):
    """Build the session. SPARK_CORES and SPARK_MEM override cores and heap
    from the environment, so a busy machine does not need a code change:

        SPARK_CORES=4 SPARK_MEM=3g uv run python -m week2.day5
    """
    from pyspark.sql import SparkSession

    cores = os.environ.get("SPARK_CORES", cores if cores is not None else "*")
    driver_memory = os.environ.get("SPARK_MEM", driver_memory)

    return (
        SparkSession.builder.appName(app)
        # local[C,F]: C worker threads, F attempts per task. Plain local[*]
        # means F=1, so one transient S3 failure aborts the whole job.
        # C also caps how many S3 connections are open at once; past the
        # ~4 MB/s link ceiling more readers just means more timeouts.
        .master(f"local[{cores},{max_task_failures}]")
        .config("spark.driver.memory", driver_memory)
        # Keep the driver's own connections on loopback. By default it binds
        # to the LAN address, and anything else answering on that port kills
        # startup with "Too large frame".
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
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
        # S3 over a home connection. hadoop-aws 3.5 defaults to the analytics
        # accelerator reader, which issues many concurrent range requests and
        # times out when they share one slow link. The classic S3A stream plus
        # long timeouts and many retries is what survives here.
        .config("spark.hadoop.fs.s3a.analytics.accelerator.enabled", "false")
        .config("spark.hadoop.fs.s3a.connection.timeout", "5m")
        .config("spark.hadoop.fs.s3a.connection.request.timeout", "5m")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "60s")
        .config("spark.hadoop.fs.s3a.attempts.maximum", "20")
        .config("spark.hadoop.fs.s3a.retry.limit", "20")
        .config("spark.hadoop.fs.s3a.retry.interval", "2s")
        .config("spark.hadoop.fs.s3a.retry.throttle.limit", "20")
        .config("spark.hadoop.fs.s3a.connection.maximum", "32")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.parquet.filterPushdown", "true")
        .config("spark.sql.adaptive.enabled", "true")
        # The progress bar overwrites stdout with carriage returns, which eats
        # printed measurements and breaks grep over captured logs.
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def read_slice(spark, target_gb=10.0, spread=True):
    return spark.read.parquet(*pick_slice(target_gb, spread))


def snapshot_path():
    return f"s3a://{BUCKET}/{PREFIX}"


def read_snapshot(spark):
    """Whole snapshot. Pass the directory, not 9898 paths: Spark lists and
    bin-packs the files itself and builds the plan in one go."""
    return spark.read.parquet(snapshot_path())
