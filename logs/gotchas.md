# Gotchas

One entry per problem that cost more than ~20 minutes, written while still
annoyed. Format is fixed on purpose - the "how I found out" line is the
transferable part and it is the one you skip if the format lets you.

---

## day 5 — every benchmark table was correct, and shifted by one row

**What I saw:** the first row of every `bench.measure()` table read `0 B, 0
files`, and every later row carried the *previous* row's numbers. The tables
looked completely plausible. I only noticed because a `count()` that should
read footers reported 701 MB, and the row above it reported nothing at all.

**What I assumed:** that `count()` on parquet was reading more than I thought,
and that my understanding of metadata-only counts was wrong.

**What it actually was:** Spark's SQL listener is asynchronous. An action
returns to the driver *before* the execution and its final metrics are posted
to the UI's store. My harness read `/api/v1/.../sql` the instant the `with`
block exited, saw nothing new, and the work landed in the next block's window.

**How I found out:** printed the raw execution ids inside the block instead of
the parsed table. The ids were one behind the actions that produced them, which
is only possible if the read happened too early.

**What I'd check first next time:** any metrics API backed by a listener bus is
eventually consistent. Before trusting it, assert that the number of new
executions equals the number of actions you ran. `bench._settle()` now waits
for the executions to appear *and* stop changing - two conditions, because an
execution is registered as RUNNING before it has any metrics.

---

## day 5 — `inputBytes` said 1.60 MB for a 2.3 GB scan

**What I saw:** summing one column over the whole curated table, the `/stages`
endpoint reported `inputBytes: 1.60 MB`. The table is 2.3 GB on disk.

**What I assumed:** column pruning was working spectacularly well.

**What it actually was:** `inputBytes` comes from Hadoop's
`FileSystem.Statistics`, which are thread-local counters that only get updated
by filesystems that bother. Reading local parquet through Spark's vectorised
reader largely bypasses that accounting, so the counter stays near zero. It is
not broken - it is measuring something else.

**How I found out:** compared it against the SQL tab, which said `size of files
read: 2.3 GiB` for the same query. Two numbers from the same UI, three orders
of magnitude apart, and only one of them matched `du`.

**What I'd check first next time:** cross-check any single metric against a
number you can get from outside Spark (`du`, the file listing, the row count)
before building anything on top of it. `bench.py` now takes bytes from the SQL
operator metrics and only takes task counts from `/stages`.

---

## day 5 — "size of files read" is not bytes read

**What I saw:** having switched to the SQL metrics, reading 1 column and
reading 47 columns both reported exactly `701.00 MB`. The wall times were 0.6 s
and 10.8 s.

**What I assumed:** the new metric was broken too.

**What it actually was:** `size of files read` is the total size of the files
the scan *opened*. Parquet is columnar, so the reader seeks to the column
chunks it needs and never touches the rest - but the file was still opened, so
the metric counts all of it. No metric Spark publishes counts the bytes
actually decoded.

**How I found out:** the 19x time difference with zero byte difference is not a
measurement error, it is a definition mismatch. Reading the metric's
description in the source settled it.

**What I'd check first next time:** partition pruning is visible in bytes
(fewer files opened). Column pruning is visible only in time. Anyone measuring
column pruning with a byte counter will conclude it does not work. This is the
most misleading number in the SQL tab and it has an entirely reasonable name.

---

## day 5 — mergeSchema "proved" 3x faster than a plain read

**What I saw:** `read.parquet()` took 2.30 s, `read.parquet(mergeSchema=true)`
took 0.68 s. The expensive option was three times faster than the cheap one.

**What I assumed:** briefly, that mergeSchema had some fast path. It does not.

**What it actually was:** the first read of a path pays for the recursive file
listing and for JVM class loading. Whichever variant ran first absorbed that
cost. Reversing the order reversed the result.

**How I found out:** swapped the two lines.

**What I'd check first next time:** warm up, then take the best of several
runs. An A/B where A always runs first is not an A/B. (The real answer: at 140
files there is no measurable difference at all - the cost scales with file
count, and the argument for mergeSchema is correctness, not speed.)

---

## day 5 — the registry API stopped answering at offset 30,000

**What I saw:** paging `/dataset/search?type=OCCURRENCE` in pages of 1000. The
first pages returned in ~1 s each. By offset 30,000 a single page had not
returned after two minutes.

**What I assumed:** rate limiting, and added backoff and retries. This made it
slower and no more successful.

**What it actually was:** deep pagination against an Elasticsearch-backed
search endpoint. To serve `offset=30000` the engine sorts and discards 30,000
documents *per shard, per request*. Cost grows with offset, not page size. No
amount of retrying fixes an algorithm.

**How I found out:** timed a single `curl` at offset 0 and at offset 30,000 -
1 s versus >120 s, with identical page size. That is not a rate limiter.

**What I'd check first next time:** before paging an API deeply, time the last
page, not the first. And ask whether you need all of it: the occurrence slice
references 1,473 datasets, not 53,651. Fetching `/dataset/{key}` for exactly
the keys the fact table contains has no offset in it at all and finished in
29 seconds. **Build the dimension for the keys the fact table actually has.**

---

## day 5-7 — two Spark 4 behaviour changes that look like code bugs

**What I saw:** (a) `int(spark.conf.get("spark.sql.autoBroadcastJoinThreshold"))`
raised `invalid literal for int(): '10485760b'`. (b) A benchmark that summed 47
column hashes died with `ARITHMETIC_OVERFLOW`.

**What it actually was:** (a) Spark 4 returns byte-valued configs as strings
with a unit suffix; Spark 3 returned a bare number. (b) Spark 4 has ANSI mode
ON by default, so an integer overflow that Spark 3 wrapped around silently is
now a job-killing error.

**How I found out:** both were immediate, loud failures - which is the point.

**What I'd check first next time:** these are the good kind of breakage. The
ANSI one in particular means Spark 3 code that silently produced wrong numbers
now refuses to run. Worth saying out loud in the talk: a job that crashes on
upgrade may have been lying to you before.

---

## day 7 — 42% of datasets were licensed "LEGALCODE"

**What I saw:** `groupBy("license")` returned `CC_BY_4_0` (740), `LEGALCODE`
(623), `CC_BY-NC_4_0` (110). LEGALCODE is not a licence.

**What I assumed:** missing data for those 623 datasets.

**What it actually was:** my regex looked for `licenses/<x>/<y>` in the licence
URL. Creative Commons uses two URL families and only one says "licenses":
`.../licenses/by/4.0/legalcode` but `.../publicdomain/zero/1.0/legalcode`.
Every CC0 dataset missed the regex and fell through to a fallback that took the
last path segment - `legalcode`.

**How I found out:** counted the distinct raw `license_url` values. Three
values, and the second one did not contain the word "licenses".

**What I'd check first next time:** a parse fallback that produces a
*plausible-looking* category is worse than one that produces null. LEGALCODE
sorted neatly into the table and would have been reported as a finding. If a
regex has an `otherwise` branch, make it say `OTHER`, then go and look at how
big OTHER is.

---

## day 6 — the benchmark I thought was hanging had already finished

**What I saw:** `day6.py` sat at "5. the same transformation, three ways" for
ten minutes with no further output. Killed it, pinned a dependency, ran it
again, watched it sit in the same place. Killed it again.

**What I assumed:** the pandas UDF was deadlocking - plausible, since PySpark
4.2 warns that pandas >= 3 is not fully supported and that was the version
installed.

**What it actually was:** two output problems stacked on top of each other and
neither was Spark.

  1. Spark's console progress bar writes carriage returns to stdout. A
     `print()` that lands mid-stage is overwritten by the next progress
     repaint, and a `grep` over the captured log drops the line entirely
     because it now starts with `[Stage 3:...`.
  2. `grep` writes in 4 KB blocks when its output is a file rather than a
     terminal. So the log file was empty for minutes at a time regardless of
     what the program was doing.

The pandas UDF was fine: 8.5 s over 6.4 million rows, against 0.6 s for the
built-in expression and 11.9 s for the row-at-a-time python UDF - exactly the
ordering the section claims.

**How I found out:** ran the section standalone, wrote stdout straight to a
file with no pipe, and pushed it through `tr '\r' '\n'`. Both missing lines
were there, one of them behind a carriage return.

**What I'd check first next time:** before debugging a program that appears
hung, confirm you can see its output at all. `spark.ui.showConsoleProgress` is
now `false` in `gbif.py` - these scripts exist to print measurements, and those
do not get to be overwritten by a progress indicator - and pipelines that
capture output use `grep --line-buffered`.

**The wider lesson, which is the one for the talk:** I twice "fixed" a problem
that did not exist, and one of those fixes (pinning pandas) changed the
environment under a running job. Reproduce before you remediate.

---

## day 6 — a job that hangs forever with 12 active tasks and 0 completed

**What I saw:** `day6.py` stopped at the UDF benchmark. No error *that I could
see*, no progress. The JVM was alive and using 0% CPU. It stayed that way for
ten minutes, twice.

**What I assumed:** the pandas UDF was deadlocking. I had already convinced
myself of this once (see the entry above, where the real problem was a progress
bar) so I was primed to believe it.

**What it actually was:** the machine ran out of memory and the OS killed the
python workers.

Three facts, each of which alone means nothing:

  - the Spark UI REST API said stage 68 had **12 active tasks and 0 complete**
  - `ps` found **zero** `pyspark.daemon` processes
  - `vm_stat` reported **42 MB** of free pages

Twelve active tasks with no workers to run them is not a deadlock in the usual
sense - it is the JVM waiting on sockets to processes that no longer exist.

And Spark *did* say so, on stderr:

    ERROR Executor: Exception in task 6.0 in stage 68.0 (TID 616):
    Python worker exited unexpectedly (crashed).

I never saw it, because the pipeline capturing the run was
`... 2>&1 | grep -vE "WARN|..."` and I had been filtering aggressively to keep
the measurement tables readable. I filtered out the one line that explained
everything. **The second time I searched the raw file instead of the filtered
one, it was the third line from the end.**

The cause is arithmetic: a python UDF forks one worker per core, and each one
imports pandas and pyarrow at roughly 150 MB resident. Twelve of those is
~1.8 GB, next to an 8 GB JVM heap, on a 16 GB laptop. `spark.driver.memory` is
a claim on the JVM only; the python side is invisible to it.

**How I found out:** the stuck stage's line number. I had assumed the pandas
UDF was to blame and the traceback-free hang gave me nothing to correct that
with - but `/api/v1/.../stages?status=ACTIVE` names the source line, and it
pointed at the **python** UDF two lines below. The pandas one had already
finished. Everything I believed about which line was slow was wrong, and one
REST call settled it.

**What I'd check first next time:** read the unfiltered output before
theorising. Then ask the UI *what is running* rather than guessing which line
is slow. "Active tasks > 0, worker processes = 0" is a specific and
recognisable signature, and the stage's source line is one REST call away.

**Contributing factor worth naming:** Docker Desktop was running an unrelated
project, configured with a 7.65 GiB memory ceiling and 8 of the 12 CPUs. It was
only holding 0.52 GB resident at the time, so it was not the cause - the
arithmetic fails without it - but on a 16 GB laptop it is a standing claim on
headroom, and its CPU allocation contends with `local[*]` directly. Worth
stopping before the day-12 full run.

**What changed:** `day6.py` asks for a 4 GB driver, not 8 GB, and benchmarks
the UDFs over 1M rows rather than 6.4M. Sizing the driver has to leave room for
the python workers, and that is a sentence I had read many times without it
meaning anything.
