"""Tests for curate() - no S3, no cluster, five rows built by hand.

Run: uv run python -m gbif_spark.tests.test_curate
     uv run python -m gbif_spark test               # every test file

curate() is a pure DataFrame -> DataFrame function precisely so this is
possible. Anything that needed a real read would not be testable at all.
"""
from pyspark.sql import Row, types as T

from gbif_spark.helpers import gbif
from gbif_spark.pipeline import curate

ISSUE_T = T.ArrayType(T.StructType([T.StructField("array_element", T.StringType())]))

SCHEMA = T.StructType([
    T.StructField("gbifid", T.LongType()),
    T.StructField("datasetkey", T.StringType()),
    T.StructField("publishingorgkey", T.StringType()),
    T.StructField("species", T.StringType()),
    T.StructField("specieskey", T.IntegerType()),
    T.StructField("class", T.StringType()),
    T.StructField("order", T.StringType()),
    T.StructField("countrycode", T.StringType()),
    T.StructField("decimallatitude", T.DoubleType()),
    T.StructField("decimallongitude", T.DoubleType()),
    T.StructField("coordinateuncertaintyinmeters", T.DoubleType()),
    T.StructField("year", T.IntegerType()),
    T.StructField("lastinterpreted", T.TimestampType()),
    T.StructField("basisofrecord", T.StringType()),
    T.StructField("occurrencestatus", T.StringType()),
    T.StructField("taxonrank", T.StringType()),
    T.StructField("issue", ISSUE_T),
])


def issues(*names):
    return [Row(array_element=n) for n in names]


import datetime
TS = datetime.datetime(2025, 3, 14, 10, 0, 0)

ROWS = [
    (1, "ds-a", "org-a", "Vulpes vulpes", 5219243, "Mammalia", "Carnivora",
     "se", 59.3, 18.1, 50.0, 2019, TS, "human_observation", "PRESENT",
     "species", issues("CONTINENT_DERIVED_FROM_COORDINATES")),
    (2, "ds-a", "org-a", "Vulpes vulpes", 5219243, "Mammalia", "Carnivora",
     "SE", 0.0, 0.0, 10.0, 2019, TS, "HUMAN_OBSERVATION", "PRESENT",
     "SPECIES", issues("ZERO_COORDINATE", "COUNTRY_COORDINATE_MISMATCH")),
    (3, "ds-b", "org-b", None, None, "Insecta", "Diptera",
     "  ", 10.0, -70.0, None, None, TS, "PRESERVED_SPECIMEN", "PRESENT",
     "GENUS", None),
    (4, "ds-b", "org-b", "Bufo bufo", 2422459, "Amphibia", "Anura",
     "DE", -33.9, 151.2, 500000.0, 12, TS, "MATERIAL_SAMPLE", "ABSENT",
     "SPECIES", issues()),
    (5, "ds-c", "org-c", "Quercus robur", 2878688, "Magnoliopsida", "Fagales",
     "FR", 91.0, 2.3, -1.0, 1953, TS, "OCCURRENCE", "PRESENT",
     "SPECIES", issues("COORDINATE_OUT_OF_RANGE")),
]

FAILS = []


def check(name, got, want):
    ok = got == want
    FAILS.append(name) if not ok else None
    print(f"  {'ok  ' if ok else 'FAIL'} {name:<44} {got!r}"
          + ("" if ok else f"   want {want!r}"))


spark = gbif.spark_session(app="curate-tests", driver_memory="1g")
spark.sparkContext.setLogLevel("ERROR")

raw = spark.createDataFrame(ROWS, SCHEMA)
out = curate.curate(raw).orderBy("gbifid").collect()
r = {row["gbifid"]: row for row in out}

print("\nissue flattening")
check("array<struct> -> array<string>", r[1]["issues"],
      ["CONTINENT_DERIVED_FROM_COORDINATES"])
check("sorted", r[2]["issues"], ["COUNTRY_COORDINATE_MISMATCH", "ZERO_COORDINATE"])
check("NULL array -> empty, not null", r[3]["issues"], [])
check("n_issues on NULL array is 0", r[3]["n_issues"], 0)
check("informational flag is not a geo issue", r[1]["n_geo_issues"], 0)
check("two fatal flags counted", r[2]["n_geo_issues"], 2)
check("has_geo_issue", r[2]["has_geo_issue"], True)

print("\ncategoricals")
check("basis upcased", r[1]["basis"], "HUMAN_OBSERVATION")
check("country upcased", r[1]["country"], "SE")
check("blank country -> NULL", r[3]["country"], None)
check("class -> taxon_class", r[1]["taxon_class"], "Mammalia")
check("order -> taxon_order", r[1]["taxon_order"], "Carnivora")
check("is_present", r[4]["is_present"], False)

print("\ntime")
check("decade", r[1]["decade"], 2010)
check("1953 -> 1950", r[5]["decade"], 1950)
check("NULL year -> decade 0, never NULL", r[3]["decade"], 0)
check("year 12 is implausible -> decade 0", r[4]["decade"], 0)
check("year_known false for typo", r[4]["year_known"], False)
check("interpreted_month", r[1]["interpreted_month"], "2025-03")

print("\nspace")
check("coord_valid", r[1]["coord_valid"], True)
check("null island is not valid", r[2]["coord_valid"], False)
check("null island flagged separately", r[2]["null_island"], True)
check("lat 91 is not valid", r[5]["coord_valid"], False)
check("has_coords true even when invalid", r[5]["has_coords"], True)
check("cell floors toward -inf", (r[3]["cell_lat"], r[3]["cell_lon"]), (10, -70))
check("cell_id", r[1]["cell_id"], "59_18")
check("no cell for invalid coords", r[5]["cell_id"], None)
check("negative uncertainty -> NULL", r[5]["uncertainty_m"], None)
check("uncertainty_known false on NULL", r[3]["uncertainty_known"], False)
check("NULL uncertainty passes the gate", r[3]["uncertainty_ok"], True)
check("500 km uncertainty fails", r[4]["uncertainty_ok"], False)

print("\nthe gate")
check("clean record is usable", r[1]["usable_for_mapping"], True)
check("null island is not", r[2]["usable_for_mapping"], False)
check("no species is not", r[3]["usable_for_mapping"], False)
check("ABSENT is not", r[4]["usable_for_mapping"], False)
check("out of range is not", r[5]["usable_for_mapping"], False)

print("\ninvariants")
check("no rows dropped", len(out), len(ROWS))
check("decade never NULL", sum(1 for x in out if x["decade"] is None), 0)
check("usable never NULL", sum(1 for x in out if x["usable_for_mapping"] is None), 0)

print("\nshard picker")
picked, _ = curate.pick_new_shards(set(), 0.2)
again, _ = curate.pick_new_shards(set(picked), 0.2)
check("second pick overlaps nothing", set(picked) & set(again), set())
check("second pick is non-empty", len(again) > 0, True)

spark.stop()
print(f"\n{len(FAILS)} failures" + ("" if not FAILS else ": " + ", ".join(FAILS)))
raise SystemExit(1 if FAILS else 0)
