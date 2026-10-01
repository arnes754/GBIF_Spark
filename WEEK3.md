# Week 3 — optimising the job

Weeks 1 and 2 built a pipeline that answers one question about GBIF. Week 3
does not add anything to it. It takes the job that exists, finds out where its
time actually goes, changes the things the measurements justify, and writes
down the things they do not.

This document is the whole rundown: what each day did, how the code fits
together, what was found, and what finally changed. If you only read one
section, read [The result](#the-result).

- [How to run it](#how-to-run-it)
- [How the code fits together](#how-the-code-fits-together)
- [Day 1 — reading the Spark UI](#day-1--reading-the-spark-ui-day10py)
- [Day 2 — partitioning](#day-2--partitioning-day11py)
- [Day 3 — broadcast joins and skew](#day-3--broadcast-joins-and-skew-day12py)
- [Day 4 — caching and persistence](#day-4--caching-and-persistence-day13py)
- [Day 5 — before and after](#day-5--before-and-after-day14py)
- [The result](#the-result)
- [What I would do differently](#what-i-would-do-differently)

---

## How to run it

Everything runs on `local[*]` against the curated parquet table on disk. No S3,
no cluster. The table is 90 GB / 2.23 billion rows across 31 `ingest_batch`
directories, so every script takes `--batches` and runs on a named subset —
an experiment you can only afford to run once is a guess, not a measurement.

```bash
uv run python day10.py --batches b0000          # profile the job, find the bottleneck
uv run python day10.py --hold                   # ... and leave the Spark UI up
uv run python day11.py --batches b0000          # partitioning: in, shuffle, out
uv run python day12.py --batches b0000          # broadcast joins and skew
uv run python day13.py --batches b0003 --big b0000   # caching
uv run python day14.py --slices b0003,b0000,b0004    # before and after
uv run python day14.py --report                 # re-print without re-running
```

The slices used throughout:

| batch | on disk | rows | what it is good for |
|---|---|---|---|
| `b0003` | 19 MB | 679 k | a toy. Everything completes. Misleading, usefully so. |
| `b0000` | 1.04 GB | 26.3 M | the working slice. Big enough to break things. |
| `b0004` | 5.65 GB | 141.6 M | the realistic one. Minutes per run. |
| all 31 | 90 GB | 2.23 B | the real table. Not run this week. |

---

## How the code fits together

Week 3 begins with a refactor, because you cannot optimise something you can
only run by typing its filename.

```
gbif.py      Spark session + S3 config.           (day 1)
curate.py    build the curated table; read_table(). (week 1)
registry.py  the publisher dimension from the GBIF API. (week 2)
bench.py     the measurement harness.             (day 5, extended this week)
job.py       THE JOB: nine stages + a Config.     (new, week 3 day 1)
day9.py      CLI over job.py + the run log.
day10..14.py one script per week-3 day.
```

### `job.py` — the job as a function

Day 9's pipeline lived inside its `main()`. Week 3 needs to run those same nine
stages five times with one setting moved, so they moved into `job.py`:

```python
cfg = job.Config(batches=("b0000",), cache="none", write_in_place=True)
spark = job.session_for(cfg)
stages, results, summary = job.run(spark, cfg)
```

The important property is that **no stage function reads a global**. Everything
a run is allowed to differ by is a field on `Config`. That is what makes
`day14.py` able to print the difference between two runs as a table: the
difference *is* the difference between two `Config` objects, and nothing is
hiding in a module-level variable somewhere.

The nine stages, unchanged from day 9:

| | stage | what it does |
|---|---|---|
| 1 | read + schema resolve | open the curated table and the dimension |
| 2 | enrich | left-join the publisher dimension, project to 18 columns |
| 3 | headline aggregate | the one number the project exists to produce |
| 4 | breakdowns | five `groupBy`s (decade, country, basis, …) |
| 5 | publisher/dataset windows | per-dataset aggregate + windows over it |
| 6 | variance decomposition | between-publisher vs within-publisher variance |
| 7 | explode + flag aggregates | the `issues` array, flattened and counted |
| 8 | write aggregates | eleven result tables, ~300 KB total |
| 9 | read back + verify | re-read one result, check it sums to the input |

### `bench.py` — where the numbers come from

Already existed. It reads the driver's own UI over REST and takes its metrics
from the **SQL tab**, not `/stages`, because `/stages.inputBytes` is Hadoop
`FileSystem` statistics and reads near zero for local parquet. Week 3 added:

- `ui_base(spark)` — the session's own UI URL instead of a hardcoded `:4040`.
  See the day-10 gotcha; this one cost an hour.
- `ui_json`, `stage_list(summaries=True)`, `sql_list` — public accessors, so a
  day script can read the operator tree and the per-task quantiles.
- `exec_ids` / `stage_ids` on each measurement, so a stage timing can be traced
  back to the exact SQL execution and Spark stages behind it.

---

## Day 1 — reading the Spark UI (`day10.py`)

**The task:** introduce the Spark UI. Find the bottleneck in my own job. Which
stage takes longest? Where do the shuffles happen?

### What the Spark UI is

While a job runs, the driver serves a web UI (usually `http://localhost:4040`).
When the job ends the driver exits and the UI goes with it — which is why
`day10.py` takes `--hold`.

| tab | the question it answers |
|---|---|
| **Jobs** | one row per action. Mostly "how many actions did my code trigger" — usually more than you thought. |
| **Stages** | one row per stage. A stage is the span between two shuffles, so the stage list *is* the shuffle structure. Per-task min/median/max lives here. |
| **SQL/DataFrame** | the one that matters. The operator tree with per-operator metrics the operators themselves emit. |
| **Storage** | what is cached, and whether it actually fit. |
| **Executors** | per-executor task time, GC, memory. On `local[*]` there is one, so mostly "how much GC". |

Every number on those pages is also JSON at `/api/v1`. `day10.py` reads it that
way so the answer to "which stage is the bottleneck" is a committed table
rather than something remembered from a browser tab that no longer exists.

### What it found

**First: the job could not be profiled as shipped.** Day 9's default persists
the enriched table in `MEMORY_AND_DISK`, and on anything above the 19 MB toy
batch that OOMs the driver. The profile runs with `--cache none`; day 4 of this
week is where that gets dealt with properly.

**Second: where the time went**, on the 1.04 GB slice:

| stage | secs | % | bytes read | shuffle written |
|---|---|---|---|---|
| 1 read + schema resolve | 5.3 | 5% | 1.04 GB | 1 KB |
| 2 enrich | 0.3 | 0% | 0 | 0 |
| 3 headline aggregate | 11.2 | 10% | 1.04 GB | 91 KB |
| 4 breakdowns (5 groupBys) | 6.8 | 6% | 5.22 GB | 204 KB |
| 5 publisher/dataset windows | 4.0 | 4% | 1.04 GB | 183 KB |
| 6 variance decomposition | 7.2 | 7% | 2.09 GB | 400 KB |
| 7 explode + flag aggregates | 3.5 | 3% | 1.04 GB | 39 KB |
| **8 write aggregates** | **44.7** | **41%** | **11.49 GB** | 1.1 MB |
| 9 read back + verify | 0.2 | 0% | 0 | 0 |
| | **108.4 s** | | **22.98 GB** | **2.03 MB** |

The bottleneck is the stage that writes eleven tables totalling 300 KB.

That makes no sense until you divide bytes-read by input size. Stage 4 read
5.22 GB — exactly five times the 1.04 GB slice, one scan per `groupBy`. Stage 8
read 11.49 GB — exactly eleven times, one scan per result table. **Every
aggregate was being computed twice:** once in its own stage, where `.count()`
forces it so the timing is honest, and once again in stage 8, because a
DataFrame is a recipe and `.write` re-runs the recipe.

22 passes over the fact table to produce eleven outputs.

The fix is `Config.write_in_place`: each aggregate stage writes what it just
built, so the write *is* the forcing action and nothing runs twice. On the same
slice, bytes read went 22.98 GB → 13.58 GB and executor task time 614 s → 457 s.

**Third: the shape of the job.** 22.98 GB read against 2.03 MB shuffled is a
ratio of about 11,000 : 1. This job is **read-bound**, not shuffle-bound.
Everything the aggregates do reduces hard on the map side, so almost nothing
crosses an exchange. That single ratio is what makes days 2 and 3 of this week
come out the way they do, and it is worth computing before touching anything.

**Fourth: no meaningful skew.** The worst task max/median inside a stage was
13.3× — but in a stage whose median task is under 0.05 s, where it is scheduling
noise, not data skew. Every stage doing real work came out between 1.2× and
1.9×.

---

## Day 2 — partitioning (`day11.py`)

**The task:** revisit the partitioning of inputs and outputs. Repartition and
coalesce where it helps.

Three things get called "partitioning" and they are not the same:

| | what it is | what controls it |
|---|---|---|
| **input partitions** | how many tasks read the files | `spark.sql.files.maxPartitionBytes` (128 MB), and how big the files are |
| **shuffle partitions** | how many pieces a wide operation redistributes into | `spark.sql.shuffle.partitions`, then AQE |
| **output partitions** | how many files get written | `repartition` / `coalesce` before the write |

And a fourth thing that is *not* a partition in that sense: `partitionBy("decade")`
at write time makes **directories**. It is a storage layout, and it only pays
off if a later query filters on that column.

### Experiment 1 — input partitioning

Spark bin-packs files into read tasks up to `maxPartitionBytes`, adding
`spark.sql.files.openCostInBytes` (4 MB) per file as a charge for opening it.
That open cost is why a thousand tiny files are not free — 1000 × 4 MB of
imaginary bytes dominates the packing and you get tasks that are mostly
overhead.

The 1.04 GB slice is 54 files, and the shape is not what I expected:

| file size | files | bytes |
|---|---|---|
| < 1 MB | 29 (54%) | 3.07 MB |
| 1–16 MB | 8 | 31.39 MB |
| 16–64 MB | 7 | 201.94 MB |
| 64–128 MB | 10 (19%) | 833.13 MB |

Half the files are tiny, but **80% of the bytes are in ten big files**, so the
default packing is already right:

| maxPartitionBytes | input partitions | secs |
|---|---|---|
| 32 MB | 43 | 1.8 |
| 128 MB | 15 | 0.3 |
| 512 MB | 15 | 0.3 |

15 read tasks for 12 cores. Dropping to 32 MB buys 43 tasks that each do less,
and a worse wall time. **No change made.**

The small files are a real cost — thin `decade` directories, one per batch —
and `curate.py compact` is the fix that already exists. They are just not what
limits this job.

### Experiment 2 — partition pruning

Which filters reach the directory listing, measured by *files opened* from the
Scan operator's own metric:

| filter | rows | files opened | read |
|---|---|---|---|
| no filter | 26,346,151 | 54 | 1.04 GB |
| `decade = 2010` | 8,589,431 | **5** | **336 MB** |
| `decade >= 2000` | 21,978,679 | **13** | **898 MB** |
| `year = 2015` | 890,212 | 54 | 1.04 GB |
| `country = 'US'` | 8,789,460 | 54 | 1.04 GB |

- `decade` is a **PartitionFilter**. Directories are not listed, files are
  never opened. The big win, and the only one that moves `files`.
- `year` is a **PushedFilter**. The predicate reaches the parquet reader and
  skips row *groups* whose min/max cannot match. All 54 files are still
  opened: `files` does not move, time does.
- `country` is neither. Read everything, filter in Spark.

**The honest finding: the job filters on nothing.** It reads the whole table
every time. So the decade partitioning buys *this job* nothing at all — it was
written for the ad-hoc queries in days 5–8 and for the analysis in week 5.
That is a cost this job carries for someone else's benefit, and saying so is
better than claiming it as a win.

### Experiment 3 — shuffle partitions

12 / 48 / 200 / 800, with AQE on and off.

| AQE | shuffle.partitions | secs | tasks |
|---|---|---|---|
| off | 12 | 1.4 | 28 |
| off | 48 | 1.0 | 64 |
| off | 200 | 1.9 | 216 |
| off | 800 | 3.3 | 816 |
| on | 12 | 0.8 | 17 |
| on | 48 | 0.7 | 17 |
| on | 200 | 0.8 | 17 |
| on | 800 | 0.7 | **17** |

The arithmetic nobody does: this groupBy shuffles **170 KB**. At the ~128 MB
per shuffle partition everyone recommends, that is **one** partition. The job
is configured for 48. With AQE off, 800 partitions means 816 tasks and 3.3 s
against 12 partitions' 1.4 s. With AQE on, every setting collapses to the same
17 tasks and the same 0.7 s — Spark coalesces them back down and the setting
stops mattering.

**No change made.** A number in a config file that no measurement supports is
worse than the default, because the next person has to assume it was
deliberate.

### Experiment 4 — output partitioning

The same output written four ways, with the read-back cost measured too,
because a write that is fast because it made 500 tiny files charges the cost to
whoever reads it next:

| layout | what it really does |
|---|---|
| as-is | one file per input task, no shuffle. Fastest write. File count is whatever the input happened to be — this is how tables end up with thousands of small files nobody chose. |
| `coalesce(4)` | merges partitions **without** a shuffle. The catch: it reaches *backwards*. The whole upstream computation now runs on 4 tasks, not just the write. Free when the upstream is trivial, a serious regression when it is not. |
| `repartition(4)` | a real shuffle. Costs more, keeps upstream parallelism, gives evenly sized files — **and produces a 46% bigger file** (172 MB vs 118 MB). repartition distributes rows at random, which destroys the clustering parquet's encodings depend on. Even file sizes are not free; you pay in compression. |
| `repartition("decade")` + `partitionBy` | what the curated table uses. The repartition is not optional: without it every task writes into every decade directory and you get tasks × decades files. |

| layout | write s | write shuffle | files out | size | read-back files |
|---|---|---|---|---|---|
| as-is | 7.8 | 0 B | 15 | 118.75 MB | 15 |
| `coalesce(4)` | 8.6 | 0 B | 4 | 118.72 MB | 4 |
| `repartition(4)` | 21.2 | 538 MB | 4 | **172.59 MB** | 4 |
| `repartition("decade")` + `partitionBy` | 22.2 | 297 MB | 44 | 120.26 MB | **1** |

For this job the results are kilobytes, so `coalesce(1)` is correct — one small
file is the right number of files for a 6 KB table, and the small-file argument
does not apply. **No change made.**

---

## Day 3 — broadcast joins and skew (`day12.py`)

**The task:** apply broadcast joins where appropriate. Diagnose and handle data
skew if present.

These are always taught together and they are really one thing: both are about
a shuffle, and both stop mattering if you can avoid the shuffle.

### The two join strategies

- **SortMergeJoin** — shuffle *both* sides by the key, sort each partition,
  merge. Works at any size. Costs writing and re-reading both sides.
- **BroadcastHashJoin** — send the small side, whole, to every task, and stream
  the big side past it. The big side never moves.

Spark picks broadcast by itself when it *believes* one side is under
`spark.sql.autoBroadcastJoinThreshold` (10 MB). "Believes" is load-bearing, and
day 4 of this project got burned by it: a **13-row** lookup got a SortMergeJoin,
because it was RDD-backed and had no size *statistic*. The data was tiny; Spark
did not know it was tiny.

### Experiment 1 — does it broadcast on its own?

Yes.

```
autoBroadcastJoinThreshold   10.00 MB
dimension on disk           218.51 KB   (1,473 rows)
dimension sizeInBytes stat   84.28 KB
fact table sizeInBytes stat    1.04 GB

with no hint at all         -> BroadcastHashJoin
with F.broadcast(dim)       -> BroadcastHashJoin
with the threshold disabled -> SortMergeJoin
```

A parquet relation carries a real size statistic — Spark reads the file footers
without opening a row group — so it knows the dimension is 84 KB and broadcasts
it unprompted.

**So the fix for a missing broadcast is usually not `F.broadcast()` — it is
"write the small side to parquet so Spark can see how small it is".**
`F.broadcast()` overrides the estimate: useful when you know better than the
statistic, a liability when you do not, because broadcasting something that
turns out to be 2 GB kills the driver.

### Experiment 2 — broadcast vs sort-merge, measured

| join | plan chose | secs | shuffle written | spill |
|---|---|---|---|---|
| broadcast (what Spark picks) | BroadcastHashJoin | 3.0 | 39 KB | 0 B |
| sort-merge (threshold = −1) | SortMergeJoin | 10.9 | 10.09 MB | 328 MB |

The column to read is the shuffle column, not the seconds. Forcing sort-merge
makes the **whole fact table** cross a stage boundary so that rows with equal
`datasetkey` land together — and it spills 328 MB doing it. Broadcast sends
218 KB to every task instead and the fact table never moves.

### Experiment 3 — how skewed is `datasetkey`?

Severely. On the 1.04 GB slice, 26,346,151 rows over 748 datasetkeys:

| rank | rows | share | × mean |
|---|---|---|---|
| 1 | 12,608,325 | **47.86%** | 358× |
| 2 | 1,023,835 | 3.89% | 29× |
| 3 | 845,449 | 3.21% | 24× |

The top ten keys hold **69.5%** of the rows. One key is 358× the mean.

What that means for a shuffle: partitions are assigned by `hash(key) % n`. The
biggest key **cannot be split across partitions**, so one partition is at least
that many rows however many partitions you ask for. Raising
`shuffle.partitions` does nothing for this — it is the single most common wrong
fix.

### Experiments 4 and 5 — AQE skew join, and salting by hand

AQE's skew join works at *runtime*: after the shuffle it can see how big each
partition came out and splits the oversized ones, replicating the matching rows
from the other side. A static optimiser cannot do this, because the sizes are
not known until the shuffle has run.

On a deliberately forced sort-merge join over that skewed key:

| setting | secs | spill | worst task / median |
|---|---|---|---|
| AQE off | 9.2 | 96 MB | **31.3×** |
| AQE on, skewJoin off | 8.9 | 0 B | 4.7× |
| AQE on, skewJoin on | 8.2 | 184 MB | 4.6× |
| hand-salted ×16 | 7.6 | 1.12 GB | **1.5×** |

Two things in that table are worth being honest about. Most of the improvement
is AQE's **coalescing**, not its skew join — 4.7× vs 4.6× is nothing, because
the partitions never get big enough to trip the skew-join thresholds at this
slice size. And salting flattens it furthest while costing twice the shuffle,
1.12 GB of spill, an extra shuffle to build the salted side, and advance
knowledge of which key is hot. AQE needs no code and cannot be wrong about
which key is hot, because it looks.

Wall time is flat across all of them. On `local[*]` with twelve threads and a
warm page cache, one slow task gets absorbed. The task distribution is where
skew is visible *before* it becomes a problem — and it is what blows up first
on a real cluster.

### What changed about the job: nothing

The job's only join is fact × dimension on `datasetkey`, and it is a broadcast.
**No shuffle, no skew** — however skewed the column is. Experiment 3 measures
real skew in the data and experiment 2 shows the join does not care.

AQE's skew handling stays on, because it costs nothing when idle and it is the
thing that saves the job the day the dimension grows past 10 MB. `F.broadcast()`
stays out of the default path, because a hint that is currently redundant is a
hint that will be wrong later and nobody will remember to check.

---

## Day 4 — caching and persistence (`day13.py`)

**The task:** identify where caching earns its place. Understand the cost, not
just the benefit.

A DataFrame is a recipe, not a table. Every action re-runs the recipe from the
files. `persist()` says: keep the result the first time, serve later reads from
the kept copy. So the deal is

```
pay  : materialising and storing it, once
save : recomputing it, on every read after the first
```

and caching wins only when `save × (readers − 1) > pay`. That is arithmetic,
and it is the arithmetic nobody does.

### The storage levels

| level | what it really does |
|---|---|
| `MEMORY_ONLY` | columnar cache in the JVM heap. Fastest. A partition that does not fit is **silently not cached** and gets recomputed — a 40%-cached table is 60% of the original work *plus* all of the storage cost. |
| `MEMORY_AND_DISK` | the same, but partitions that do not fit go to local disk. The usual advice, and day 9's default. |
| `DISK_ONLY` | always to disk. Slower than memory, much faster than recomputing if the recipe is expensive. |

### The failure that started the day

```
Caused by: java.lang.RuntimeException: java.lang.OutOfMemoryError: Java heap space
  at ... job.py apply_cache -> df.count()
```

on `b0004` (5.7 GB). Then on `b0000` (1.04 GB). Only the 19 MB toy batch ever
completed — which is the only size day 9 had ever been run at.

I had assumed `MEMORY_AND_DISK` could not OOM by definition; that is what the
"and disk" is for. **The storage level decides where a finished block is put.
It does not decide where the block is built.** Each task unrolls its partition
into Spark's columnar format in the JVM heap first, and with twelve tasks
unrolling ~1 M rows × 18 columns each — including an `array<string>` — into the
same 4 GB heap the query is already using, the *unrolling* is what runs out,
before any storage level gets consulted.

### What it measures when it *does* fit

On the 19 MB toy batch, where all four storage levels complete — seven
consumers, each a separate pass if nothing is cached:

| storage level | build s | consume s | total s | vs no cache | bytes read | % cached |
|---|---|---|---|---|---|---|
| `none` | 0.0 | 3.2 | 3.2 | 1.00× | 137.29 MB | — |
| `MEMORY_ONLY` | 4.1 | 3.1 | 7.2 | **0.44×** | 0 B | 100 |
| `MEMORY_AND_DISK` | 1.8 | 1.8 | 3.6 | 0.87× | 0 B | 100 |
| `DISK_ONLY` | 1.7 | 1.6 | 3.3 | 0.95× | 0 B | 100 |

Caching drives bytes read to zero, exactly as advertised, and every level is
still a net loss once the build cost is counted.

**And the caveat, because that is weaker than it looks.** At 19 MB one pass
costs 0.2 s, which is the same order as measurement noise, so "saved per read"
is a difference the harness cannot really resolve. The obvious fix is to
measure on a bigger slice — and that is exactly what cannot be done, because on
a bigger slice the cache OOMs. **There is no slice on this machine where the
cache both fits and is big enough for the saving to be measurable.** That is
not a gap in the experiment, it *is* the finding.

### Why the cache was a bad trade here even when it fit

1. **The thing being cached is cheap.** It is a parquet scan plus a broadcast
   join. No shuffle, no UDF, nothing to amortise. Re-reading parquet with
   column pruning is close to the fastest thing this machine does.
2. **The cache cannot prune columns per reader.** Uncached, the headline query
   reads three columns; cached, every reader pays for all sixteen.
3. **It does not degrade, it cliffs.** An uncached job reading 90 GB is slow. A
   cached one is dead, at a size nobody writes down.

### What caching *is* for

Small and expensive to produce. `by_dataset` is ~1,400 rows produced by a
`groupBy` over the entire fact table, and three stages read it. That is the
right shape, and `--cache-results` persists it. The fact table is the opposite
shape on both counts.

**The rule:** cache what was expensive to compute, not what is read often. And
check the Storage tab's "% cached" afterwards, every time — a 40%-cached table
is the worst of all worlds and looks exactly like success.

---

## Day 5 — before and after (`day14.py`)

**The task:** measure job performance before and after. Document what worked
and what did not.

Each configuration is a **fresh `day9.py` subprocess**, not a loop inside one
session. A session carries cached blocks, a warm JIT and conf values set by the
previous experiment, so a loop measures the order you ran things in as much as
the settings. `--warm` reads each slice once first so no configuration pays for
a cold page cache.

Three configurations, not two, because "we changed several things and it got
faster" is not a measurement:

| | flags |
|---|---|
| `before` | `--cache memory_and_disk` — day 9 as shipped |
| `nocache` | `--cache none` — one change, nothing else |
| `after` | `--cache none --write-in-place --cache-results` |

### The matrix

Nine runs, three slices, each a fresh JVM, page cache warmed first.

| slice | config | rows | job s | bytes read | shuffled | task s | result |
|---|---|---|---|---|---|---|---|
| b0003 (19 MB) | before | 678,958 | **48.9** | 39.23 MB | 400 KB | 100 | verified |
| b0003 | nocache | 678,958 | 65.5 | 430.64 MB | 399 KB | 80 | verified |
| b0003 | after | 678,958 | 84.6 | 254.98 MB | 278 KB | 79 | verified |
| b0000 (1.04 GB) | before | — | 115.9 | — | — | — | **FAILED: OOM** |
| b0000 | nocache | 26,346,151 | 117.3 | 22.98 GB | 2.03 MB | 611 | verified |
| b0000 | after | 26,346,151 | 119.5 | **13.58 GB** | 1.24 MB | **465** | verified |
| b0004 (5.65 GB) | before | — | 89.6 | — | — | — | **FAILED: OOM** |
| b0004 | nocache | 141,613,091 | 296.3 | 125.40 GB | 6.89 MB | 2746 | verified |
| b0004 | after | 141,613,091 | **243.4** | **74.10 GB** | 4.02 MB | **1949** | verified |

Read it top to bottom and the week's whole argument is in it.

**On the 19 MB toy batch, `before` wins.** 48.9 s against 84.6 s. The cache
fits trivially, the job makes many passes over the enriched table, and
caching pays for itself exactly as the tutorials say. This is the only size
day 9 was ever run at, and it is the reason the wrong decision looked right
for a week.

**On 1.04 GB and 5.65 GB, `before` does not run at all.** Not slower — it
OOMs the driver in the stage that exists to make the job faster. There is no
percentage to report here. The job went from not running to running.

**Between `nocache` and `after`**, on the 5.65 GB slice — the one big enough
for the numbers to mean something:

| | nocache | after | |
|---|---|---|---|
| wall time | 296.3 s | **243.4 s** | −18% |
| bytes read | 125.40 GB | **74.10 GB** | −41% |
| executor task time | 2746 s | **1949 s** | −29% |
| `usable_for_mapping` | — | — | identical |

125 GB read for a 5.65 GB input is 22 passes. 74 GB is 13. That is the
double-compute, removed.

### Where the difference came from

Stage by stage on the 5.65 GB slice, `nocache` → `after`:

| stage | nocache | after | delta |
|---|---|---|---|
| 1 read + schema resolve | 5.6 s | 5.4 s | −0.2 s |
| 2 enrich | 0.3 s | 0.3 s | — |
| 3 headline aggregate | 28.7 s | 53.9 s | +25.2 s |
| 4 breakdowns | 21.1 s | 43.7 s | +22.6 s |
| 5 publisher/dataset windows | 12.8 s | 34.7 s | +21.9 s |
| 6 variance decomposition | 24.6 s | 4.5 s | **−20.1 s** |
| 7 explode + flag aggregates | 13.4 s | 45.4 s | +32.0 s |
| **8 write aggregates** | **154.6 s** | **0.0 s** | **−154.6 s** |
| 9 read back + verify | 0.2 s | 0.2 s | — |

Stages 3–7 got *slower* because they now do the writing. Stage 8 went to zero
because there is nothing left for it to do. The sum is 53 seconds better, and
the work moved rather than disappearing — which is what you want a stage table
to show, because a total that improves without any stage moving means
something was measured wrong.

Stage 6 is the other change: the variance decomposition reads `by_dataset`,
which `--cache-results` now persists. 1,400 rows, cached, and it takes 20
seconds off. That is what caching is for.


---

## The result

Two changes to the job, both forced by a measurement:

1. **The fact-table cache came out.** `--cache none` is the default. It was
   added on day 9 because seven stages read the enriched table — the right
   question, the wrong answer. The thing being cached is a parquet scan plus a
   broadcast join: cheap to recompute, expensive to store, impossible to
   column-prune once cached, and fatal above ~20 MB on this machine.
2. **Each aggregate is computed once, not twice.** `--write-in-place`. Every
   result DataFrame used to be forced with `count()` to time it and then
   recomputed by the write stage. 22 passes over the fact table for 11
   outputs; now 13.
   And the one thing worth caching is cached: `--cache-results` persists
   `by_dataset`, 1,400 rows produced by a groupBy over the whole fact table
   and read by three stages.

Net, on the 5.65 GB slice: **−18% wall, −41% bytes read, −29% CPU**, same
answer to seven decimal places. And on every slice above 20 MB, the job now
finishes, which it previously did not.

**Three days ended in no change at all**, each with the measurement that says
why:

| day | what was tried | why nothing changed |
|---|---|---|
| 2 | input partitioning | files already pack into 15 tasks for 12 cores; 32 MB makes it worse |
| 2 | shuffle partitions 12/48/200/800 | the shuffles are **kilobytes**; AQE coalesces every setting to the same 17 tasks |
| 2 | partition pruning | the job filters on nothing, so the decade layout cannot help it — it exists for other queries, and that is a cost this job carries |
| 3 | broadcast joins | already broadcasting, because the parquet dimension has a real size statistic |
| 3 | skew | `datasetkey` is 358× skewed and it does not matter: no shuffle on the join, so no skew on the join |

That is the actual output of a week of optimisation: two changes made, five
changes *not* made, and a reason for each. Every one of those five would have
been code to maintain, or a number in a config file that nobody could later
justify, and a plausible-sounding slide with nothing behind it.

The one-sentence version: **this job is read-bound, not shuffle-bound** —
22.98 GB read against 2.03 MB shuffled — and almost all the standard Spark
tuning advice is about shuffling.


---

## What I would do differently

- **Run the job at more than one size before declaring it finished.** Day 9
  only ever ran on the 19 MB batch, which is exactly the size at which the
  wrong caching decision looks right. One run on a 1 GB slice would have caught
  it a week earlier.
- **Compute bytes-read ÷ input-size as a reflex.** It is one division and it
  caught the double-compute immediately. Wall time hid it completely.
- **Do not wrap network calls in `except Exception: return []`.** "Nothing
  happened" and "I could not find out" are different answers, and conflating
  them produced a table of plausible zeroes that cost an hour.
- **Write the "no change made, here is why" paragraph at the time.** Three of
  the five days end in a decision not to change anything, and those paragraphs
  are the most useful thing in this document. They would have been impossible
  to reconstruct a week later.
