"""Wall time, bytes read, bytes shuffled and spill for a block of Spark work.

    with bench.measure(spark, "count") as m:
        df.count()
    print(m)      # count  12.4s  read 2.30 GB  files 140/140  shuffle 55 KB

Numbers come from the driver UI REST API, from /SQL rather than /stages.
/stages.inputBytes is Hadoop FileSystem.Statistics, which Spark's vectorised
parquet reader mostly bypasses - it reported 1.60 MB for a 2.3 GB scan. The
SQL endpoint carries per-operator metrics emitted by the operators themselves.
"""
import contextlib
import json
import re
import time
import urllib.request

UI_FALLBACK = "http://localhost:4040"

SQL_METRICS = {
    "size of files read":        "input_bytes",
    "number of files read":      "files_read",
    "number of partitions read": "partitions_read",
    "shuffle bytes written":     "shuffle_write_bytes",
    "local bytes read":          "shuffle_read_bytes",
    "remote bytes read":         "shuffle_read_bytes",
    "spill size":                "disk_spill_bytes",
    "scan time":                 "scan_ms",
    "peak memory":               "peak_memory_bytes",
    "number of output rows":     None,
}

_UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4,
          "kb": 1000, "mb": 1000**2, "gb": 1000**3,
          "ns": 1e-6, "ms": 1.0, "s": 1000.0, "m": 60_000.0, "h": 3_600_000.0}
_NUM = re.compile(r"^([\d,]+(?:\.\d+)?)\s*([A-Za-z]+)?")


def parse_metric(value):
    """Parse a SQL metric value into a float.

    Three shapes: a count ("18,175,670"), a size ("701.0 MiB"), or a two-line
    aggregated timing whose second line starts with the total.
    """
    s = str(value)
    if "\n" in s:
        s = s.split("\n", 1)[1]
    s = s.strip().lstrip("(")
    m = _NUM.match(s)
    if not m:
        return 0.0
    n = float(m.group(1).replace(",", ""))
    unit = (m.group(2) or "").lower()
    return n * _UNITS.get(unit, 1)


def ui_base(spark):
    """This session's own UI root."""
    return (spark.sparkContext.uiWebUrl or UI_FALLBACK).rstrip("/")


def _ui(spark, path):
    with urllib.request.urlopen(f"{ui_base(spark)}/api/v1{path}", timeout=60) as r:
        return json.loads(r.read())


def _app(spark):
    return spark.sparkContext.applicationId


def _sql(spark, after=-1):
    """SQL executions with an id above `after`.

    planDescription is dropped because it is the whole plan as a string.
    `offset` indexes the list rather than matching ids, so filtering on the
    id is the only safe way to say "everything after N".
    """
    try:
        execs = _ui(spark, f"/applications/{_app(spark)}/sql"
                    f"?offset=0&length=100000&planDescription=false")
    except Exception:
        return []
    return [q for q in execs if q["id"] > after]


def _stages(spark, after=-1):
    """Stage metrics. Used only for task counts and executor time."""
    try:
        return [s for s in _ui(spark, f"/applications/{_app(spark)}/stages?status=COMPLETE")
                if s["stageId"] > after]
    except Exception:
        return []


def ui_json(spark, path):
    """Any UI endpoint for this application, as parsed JSON."""
    return _ui(spark, f"/applications/{_app(spark)}{path}")


def stage_list(spark, summaries=False):
    """Completed stages. summaries=True adds taskMetricsDistributions, the
    per-task quantiles that show skew inside a stage."""
    q = "?status=COMPLETE" + ("&withSummaries=true&quantiles=0,0.25,0.5,0.75,1.0"
                              if summaries else "")
    try:
        return ui_json(spark, f"/stages{q}")
    except Exception:
        return []


def sql_list(spark):
    """SQL executions with their operator trees and metrics."""
    try:
        return ui_json(spark, "/sql?offset=0&length=100000&planDescription=false")
    except Exception:
        return []


def _high_water(spark):
    sql = _sql(spark)
    st = _stages(spark)
    return (max((q["id"] for q in sql), default=-1),
            max((s["stageId"] for s in st), default=-1))


def _settle(spark, sql_hw, timeout=20.0):
    """Wait until executions created since `sql_hw` exist and stop changing.

    An execution is registered as RUNNING first and only gets its metrics when
    it completes, so both conditions are needed.
    """
    deadline = time.perf_counter() + timeout
    last, stable = None, 0
    while time.perf_counter() < deadline:
        execs = _sql(spark, sql_hw)
        sig = (len(execs), sum(1 for q in execs if q.get("status") == "RUNNING"),
               sum(len(n.get("metrics", [])) for q in execs
                   for n in q.get("nodes", [])))
        if execs and sig[1] == 0 and sig == last:
            stable += 1
            if stable >= 2:
                return
        else:
            stable = 0
        last = sig
        time.sleep(0.05)


class Measurement(dict):
    def __str__(self):
        return (f"{self['label']:<34} {self['seconds']:7.1f}s"
                f"  read {gb(self['input_bytes'])}"
                f"  files {self['files_read']}"
                f"  shuffle {gb(self['shuffle_write_bytes'])}"
                f"  spill {gb(self['disk_spill_bytes'])}"
                f"  tasks {self['tasks']}")


def gb(n):
    n = float(n or 0)
    for unit, div in (("GB", 1024**3), ("MB", 1024**2), ("KB", 1024)):
        if n >= div:
            return f"{n / div:.2f} {unit}"
    return f"{n:.0f} B"


FIELDS = ("input_bytes", "files_read", "partitions_read", "shuffle_write_bytes",
          "shuffle_read_bytes", "disk_spill_bytes", "scan_ms",
          "peak_memory_bytes", "scan_rows", "tasks", "task_ms", "stages",
          "executions")


@contextlib.contextmanager
def measure(spark, label):
    """Time a block and attribute the SQL executions it created to it.

    Ids only increase, so "created by this block" means "id above the
    high-water mark taken before it". The block must contain an action: a
    lazy DataFrame creates no execution and measures zero.
    """
    sql_hw, stage_hw = _high_water(spark)
    m = Measurement(label=label, seconds=0.0, **{f: 0 for f in FIELDS})
    m["exec_ids"], m["stage_ids"] = [], []
    t0 = time.perf_counter()
    try:
        yield m
    finally:
        m["seconds"] = time.perf_counter() - t0
        _settle(spark, sql_hw)
        for q in _sql(spark, sql_hw):
            m["executions"] += 1
            m["exec_ids"].append(q["id"])
            for node in q.get("nodes", []):
                is_scan = "Scan" in node.get("nodeName", "")
                for met in node.get("metrics", []):
                    name = met.get("name")
                    if name == "number of output rows":
                        if is_scan:
                            m["scan_rows"] += int(parse_metric(met["value"]))
                        continue
                    field = SQL_METRICS.get(name)
                    if field:
                        m[field] += parse_metric(met["value"])
        for s in _stages(spark, stage_hw):
            m["stages"] += 1
            m["stage_ids"].append(s["stageId"])
            m["tasks"] += s.get("numCompleteTasks", 0)
            m["task_ms"] += s.get("executorRunTime", 0)


def conf_bytes(spark, key):
    """Read a byte-valued config. Spark 4 returns these as strings with a unit
    suffix ("10485760b", "200k") where Spark 3 returned a bare number."""
    v = str(spark.conf.get(key)).strip().lower()
    if v.lstrip("-").isdigit():
        return int(v)
    unit = {"b": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    return int(float(v[:-1]) * unit.get(v[-1], 1))


TXT = str
SEC = lambda v: f"{v:.1f}"
BYTES = gb
NUM = lambda v: f"{int(v):,}"
PCT = lambda v: f"{v:.0f}"

STANDARD = [("", "label", TXT), ("secs", "seconds", SEC),
            ("read", "input_bytes", BYTES), ("files", "files_read", NUM),
            ("rows scanned", "scan_rows", NUM),
            ("shuffle w", "shuffle_write_bytes", BYTES),
            ("spill", "disk_spill_bytes", BYTES),
            ("tasks", "tasks", NUM)]


def table(rows, cols):
    widths = [max(len(h), *(len(fmt(r.get(k, 0))) for r in rows))
              for h, k, fmt in cols]
    print("  " + "  ".join(h.rjust(w) for (h, _, _), w in zip(cols, widths)))
    print("  " + "  ".join("-" * w for w in widths))
    for r in rows:
        print("  " + "  ".join(fmt(r.get(k, 0)).rjust(w)
                               for (_, k, fmt), w in zip(cols, widths)))


def show(rows, cols=None):
    table(rows, cols or STANDARD)


def banner(title):
    print()
    print("=" * 78)
    print(title)
    print("=" * 78)


def scan_of(spark, df, action=None):
    """Run `action` (default count) and return files, partitions and bytes
    read, taken from the Scan operator rather than from the plan string."""
    with measure(spark, "scan") as m:
        (action or (lambda: df.count()))()
    return m


def scan_stats(df):
    """PartitionFilters, PushedFilters and ReadSchema off the executed plan.
    Three separate mechanisms, so they are reported separately."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    out = {}
    for key in ("PartitionFilters", "PushedFilters", "ReadSchema"):
        m = re.search(rf"{key}: (\[[^\]]*\]|struct<.*?>)", plan)
        out[key] = m.group(1) if m else "-"
    return out
