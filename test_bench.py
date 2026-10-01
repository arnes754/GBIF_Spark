"""Tests for the measurement harness and the registry's parsing.

Run: uv run python test_bench.py

Most of this file tests string parsing, which sounds unworthy of a test until
you remember that both of the worst bugs in this project so far were parsing:
a metric value read wrong, and a licence URL bucketed into a category that did
not exist. Neither crashed. Both produced plausible tables.
"""
import bench

FAILS = []


def check(name, got, want):
    ok = got == want
    if not ok:
        FAILS.append(name)
    print(f"  {'ok  ' if ok else 'FAIL'} {name:<52} {got!r}"
          + ("" if ok else f"   want {want!r}"))


def close(name, got, want, tol=0.51):
    ok = abs(got - want) <= tol
    if not ok:
        FAILS.append(name)
    print(f"  {'ok  ' if ok else 'FAIL'} {name:<52} {got!r}"
          + ("" if ok else f"   want ~{want!r}"))


print("\nparse_metric: counts")
check("plain integer", bench.parse_metric("11"), 11.0)
check("thousands separators", bench.parse_metric("18,175,670"), 18175670.0)
check("zero", bench.parse_metric("0"), 0.0)

print("\nparse_metric: sizes (SQL metrics use IEC units)")
check("bytes", bench.parse_metric("0.0 B"), 0.0)
close("KiB is 1024", bench.parse_metric("55.1 KiB"), 55.1 * 1024)
close("MiB is 1024^2", bench.parse_metric("701.0 MiB"), 701.0 * 1024**2)
close("GiB is 1024^3", bench.parse_metric("2.3 GiB"), 2.3 * 1024**3)

print("\nparse_metric: aggregated timings are two-line strings")
agg = ("total (min, med, max (stageId: taskId))\n"
       "9.5 s (115 ms, 847 ms, 876 ms (stage 3.0: task 92))")
check("takes the total from line 2, in ms", bench.parse_metric(agg), 9500.0)
agg_bytes = ("total (min, med, max (stageId: taskId))\n"
             "76.6 KiB (536.0 B, 7.6 KiB, 7.7 KiB (stage 3.0: task 94))")
close("aggregated size, not the min", bench.parse_metric(agg_bytes), 76.6 * 1024)
check("ms stays ms", bench.parse_metric("11 ms"), 11.0)
check("unparseable is 0, never a crash", bench.parse_metric("n/a"), 0.0)
check("empty is 0", bench.parse_metric(""), 0.0)

print("\ngb() formatting")
check("bytes", bench.gb(512), "512 B")
check("KB", bench.gb(2048), "2.00 KB")
check("MB", bench.gb(5 * 1024**2), "5.00 MB")
check("GB", bench.gb(3 * 1024**3), "3.00 GB")
check("None is not a crash", bench.gb(None), "0 B")


class FakeConf:
    """conf_bytes reads through spark.conf.get, so a stub is enough - and it
    lets us test the Spark 3 form, which this machine no longer produces."""
    def __init__(self, v):
        self.v = v

    def get(self, _key):
        return self.v


class FakeSpark:
    def __init__(self, v):
        self.conf = FakeConf(v)


print("\nconf_bytes: Spark 4 suffixes a unit, Spark 3 did not")
check("spark 3 bare integer", bench.conf_bytes(FakeSpark("10485760"), "k"), 10485760)
check("spark 4 'b' suffix", bench.conf_bytes(FakeSpark("10485760b"), "k"), 10485760)
check("'k' suffix", bench.conf_bytes(FakeSpark("200k"), "k"), 200 * 1024)
check("'m' suffix", bench.conf_bytes(FakeSpark("64m"), "k"), 64 * 1024**2)
check("'g' suffix", bench.conf_bytes(FakeSpark("1g"), "k"), 1024**3)
check("-1 disables broadcasting", bench.conf_bytes(FakeSpark("-1"), "k"), -1)

# --- registry parsing, which needs a session but no data ---------------------
print("\nregistry: licence URLs -> groupable categories")
import gbif
import registry

spark = gbif.spark_session(app="bench-tests", driver_memory="1g")
spark.sparkContext.setLogLevel("ERROR")

from pyspark.sql import functions as F, types as T

URLS = [
    ("http://creativecommons.org/licenses/by/4.0/legalcode", "CC_BY_4_0"),
    ("http://creativecommons.org/licenses/by-nc/4.0/legalcode", "CC_BY-NC_4_0"),
    # the one that silently became "LEGALCODE" - a different URL family
    ("http://creativecommons.org/publicdomain/zero/1.0/legalcode", "CC0_1_0"),
    ("https://creativecommons.org/publicdomain/mark/1.0/", "PDM_1_0"),
    (None, "UNSPECIFIED"),
    ("", "UNSPECIFIED"),
    ("http://example.com/some-bespoke-licence", "OTHER"),
]

rows = [(f"ds{i}", None, "OCCURRENCE", url, "org1", None, None, None)
        for i, (url, _) in enumerate(URLS)]
schema = T.StructType([
    T.StructField("datasetkey", T.StringType()),
    T.StructField("dataset_title", T.StringType()),
    T.StructField("dataset_type", T.StringType()),
    T.StructField("license_url", T.StringType()),
    T.StructField("publisher_key", T.StringType()),
    T.StructField("installation_key", T.StringType()),
    T.StructField("dataset_created", T.StringType()),
    T.StructField("dataset_modified", T.StringType()),
])
raw = spark.createDataFrame(rows, schema)

# reuse the exact expression dimension() builds, applied to a hand-made frame
lic = F.regexp_extract("license_url", r"licenses/([^/]+)/([^/]+)", 0)
pdm = F.regexp_extract("license_url", r"publicdomain/(zero|mark)/([^/]+)", 0)
got = (raw.withColumn(
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
    .select("license_url", "license").collect())

for (url, want), row in zip(URLS, got):
    check(f"{str(url)[-34:]:<34}", row["license"], want)

print("\nregistry: a bespoke URL must land in OTHER, not a fake category")
check("no plausible-looking fallback",
      sum(1 for r in got if r["license"] == "LEGALCODE"), 0)

spark.stop()
print(f"\n{len(FAILS)} failures" + ("" if not FAILS else ": " + ", ".join(FAILS)))
raise SystemExit(1 if FAILS else 0)
