"""Measuring a Spark job instead of having opinions about it.

Every "is this faster" question from here to day 9 needs the same numbers:
wall time, bytes read, bytes shuffled, bytes spilled. Wall time alone lies - a
query can be faster because the page cache is warm and slower because it read
40x more data. The others say which.

WHERE THE NUMBERS COME FROM, and why it is not the obvious place:

  The driver serves its own UI data as JSON on :4040/api/v1. The obvious
  endpoint is /stages, which has `inputBytes` per stage. It is wrong. Summing
  a full scan of the 2.3 GB curated table, /stages reports 1.60 MB.

  `inputBytes` comes from Hadoop's FileSystem.Statistics, which are collected
  per-thread and only for filesystems that bother to update them. Reading local
  parquet through Spark's vectorised reader mostly bypasses that accounting, so
  the counter stays near zero. It is not a bug you can fix - it is a metric
  that does not mean what its name suggests.

  /SQL/<execution> is the real source: the same data the SQL tab draws, with
  per-OPERATOR metrics that the operators themselves emit. The Scan node
  publishes `size of files read`, `number of files read` and `number of
  partitions read`. Those are exact, and they are what this module reads.

  This is SCOPE.md day 11's "read the Spark UI SQL tab properly", as code.

    with bench.measure(spark, "count") as m:
        df.count()
    print(m)      # count  12.4s  read 2.30 GB  files 140/140  shuffle 55 KB
"""
import contextlib
import json
import re
import time
import urllib.request

# The UI port is NOT always 4040. Spark takes the next free port if 4040 is
# taken, so a second session on the same machine lands on 4041 - and a
# hardcoded 4040 then queries the WRONG application. It does not error: the
# app id is not found there, every request 404s, and every metric comes back
# zero. A table full of honest-looking zeroes. Ask the context where its own
# UI is instead.
UI_FALLBACK = "http://localhost:4040"

# operator metric name -> our field. Several operators can emit the same metric
# name in one execution (two Exchanges, three Scans), so everything sums.
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
    "number of output rows":     None,      # handled separately - see below
}

_UNITS = {"b": 1, "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4,
          "kb": 1000, "mb": 1000**2, "gb": 1000**3,
          "ns": 1e-6, "ms": 1.0, "s": 1000.0, "m": 60_000.0, "h": 3_600_000.0}
_NUM = re.compile(r"^([\d,]+(?:\.\d+)?)\s*([A-Za-z]+)?")


def parse_metric(value):
    """SQL metric values come in three shapes:
         "18,175,670"                      a plain count
         "701.0 MiB"                       a size
         "total (min, med, max ...)\n9.5 s (115 ms, ...)"   an aggregated timing
    The third is a two-line string whose second line starts with the total.
    Returns a float in bytes, milliseconds, or units - the caller knows which.
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
    """This session's own UI root, straight from the context."""
    return (spark.sparkContext.uiWebUrl or UI_FALLBACK).rstrip("/")


def _ui(spark, path):
    with urllib.request.urlopen(f"{ui_base(spark)}/api/v1{path}", timeout=60) as r:
        return json.loads(r.read())


def _app(spark):
    return spark.sparkContext.applicationId


def _sql(spark, after=-1):
    """SQL executions with an id above `after`. planDescription is suppressed:
    it is the whole formatted plan as a string and dwarfs the metrics.

    `offset` is an index into the list, not an execution id, so it is NOT a
    safe way to say "everything after id N" - filtering on the id is."""
    try:
        execs = _ui(spark, f"/applications/{_app(spark)}/sql"
                    f"?offset=0&length=100000&planDescription=false")
    except Exception:
        return []
    return [q for q in execs if q["id"] > after]


def _stages(spark, after=-1):
    """Stage metrics, used ONLY for task counts and executor time - the two
    things /stages reports honestly."""
    try:
        return [s for s in _ui(spark, f"/applications/{_app(spark)}/stages?status=COMPLETE")
                if s["stageId"] > after]
    except Exception:
        return []


# --- public UI accessors (day 10 reads the UI as data) -----------------------
def ui_json(spark, path):
    """Any UI endpoint for THIS application, as parsed JSON.

    The browser at :4040 and this function read the same store. Day 10's point
    is that every number on those pages is a REST call away, so "what did the
    UI say" can be a committed table instead of a screenshot.
    """
    return _ui(spark, f"/applications/{_app(spark)}{path}")


def stage_list(spark, summaries=False):
    """Completed stages. With summaries=True each stage carries
    `taskMetricsDistributions` - the per-task quantiles, which is the only
    place skew within a stage is visible as a number."""
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
    """Block until the SQL executions created since `sql_hw` have appeared AND
    stopped changing. Two conditions, because an execution is registered as
    RUNNING first and gains its metrics only when it completes."""
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
    """Time a block and attribute every SQL execution it created to it.

    Execution ids and stage ids only ever go up, so "created by this block" is
    "id above the high-water mark taken before it". Which means the block MUST
    contain an action: a lazy DataFrame creates no execution and measures zero,
    the single easiest way to fool yourself with this harness.
    """
    sql_hw, stage_hw = _high_water(spark)
    m = Measurement(label=label, seconds=0.0, **{f: 0 for f in FIELDS})
    # Which executions and stages this block owns. Day 10 needs it: a stage
    # timing says WHERE the time went, and the only way back to WHY is the
    # operator tree and the task distribution behind that exact id.
    m["exec_ids"], m["stage_ids"] = [], []
    t0 = time.perf_counter()
    try:
        yield m
    finally:
        m["seconds"] = time.perf_counter() - t0
        # The SQL listener is asynchronous: the action returns before the
        # execution and its final metrics are posted to the UI store. Reading
        # immediately attributes this block's work to the NEXT block, which
        # looks exactly like a correct-but-shifted table and is very hard to
        # spot. Wait for the executions to land and settle.
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
    """Spark 4 returns byte-valued configs as strings with a unit suffix
    ("10485760b", "200k"); Spark 3 returned a bare number. int() on the Spark 4
    form raises - a one-character bug that only appears on a version bump."""
    v = str(spark.conf.get(key)).strip().lower()
    if v.lstrip("-").isdigit():
        return int(v)
    unit = {"b": 1, "k": 1024, "m": 1024**2, "g": 1024**3, "t": 1024**4}
    return int(float(v[:-1]) * unit.get(v[-1], 1))


# --- printing ---------------------------------------------------------------
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
    """Run `action` (default: count) and return the Scan-node facts for it:
    files read, partitions read, bytes read. This is the honest way to prove
    partition pruning - the numbers come from the operator, not from a plan
    string that changes format between Spark versions."""
    with measure(spark, "scan") as m:
        (action or (lambda: df.count()))()
    return m


def scan_stats(df):
    """PartitionFilters / PushedFilters / ReadSchema, off the executed plan.
    The three lines that answer "did the predicate reach the files" - each one
    a different mechanism, which is why they are reported separately."""
    plan = df._jdf.queryExecution().executedPlan().toString()
    out = {}
    for key in ("PartitionFilters", "PushedFilters", "ReadSchema"):
        m = re.search(rf"{key}: (\[[^\]]*\]|struct<.*?>)", plan)
        out[key] = m.group(1) if m else "-"
    return out
