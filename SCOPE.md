# DataMonth3 — 3 week scope

Written after day 4. Days 1-4 covered: environment + read (1), profiling the
snapshot (2), transformations (3), plans and partitions (4).

Dataset: GBIF occurrence snapshot `2026-09-01`, 9,898 parquet shards, ~266 GB,
`s3://gbif-open-data-eu-central-1`. Everything so far ran on a 2 GB / 74 shard
slice on `local[*]`.

---

## 1. The one claim

A presentation needs one claim, not five. Everything below exists to support it.

> **Claim (to be proved or killed):** GBIF's issue flags describe the
> *publishing pipeline*, not the record. Which flags a record carries is
> predicted better by who published it and when it was last interpreted than by
> anything about the observation itself. So "93% of records are flagged" is not
> a statement about data quality, and any filter built on flags silently selects
> for publishers.

This is the day-2 angle, sharpened so it can be wrong. It is falsifiable: if
flag mix varies more within a publisher than between publishers, the claim dies
and the finding becomes the opposite one. Either outcome is presentable.

**Framing for the talk** — lead with a decision, not a dataset:

> If you wanted to use GBIF to map where a species lives, how much of the 266 GB
> is actually usable, and who decided that for you?

Define one concrete downstream use (say: records usable for 1-degree species
distribution mapping — has species, has coordinates, uncertainty under some
threshold, not null island, year known). Measure what survives. Break the
survivors down by country, decade and publisher. The headline is a percentage;
the interesting part is that the percentage is not evenly distributed.

**Runners-up, explicitly not being done:** sampling bias in space/time, the rise
of machine observation. Name them in the talk as roads not taken.

---

## 2. What "broaden" should mean

Four axes you could grow along. You cannot have all four in 3 weeks.

| Axis | What it means here | Verdict |
|---|---|---|
| **Scale** | 2 GB slice -> full 266 GB | **Yes** — the whole point of Spark |
| **Stack** | standalone cluster, MinIO, spark-submit, packaging | **Yes** — docker-compose.yml already exists and is unused |
| **Rigor** | pipeline as code, tests, reproducible runs, measured tuning | **Yes** — this is what makes it a project and not a notebook |
| **Analysis** | more questions, ML, forecasting | **No** — one question, answered properly |

So: go deep on engineering, narrow on the question. The talk is then "I built a
thing that answers one question at full scale, and here is everything that
fought me", which is a much better talk than five shallow findings.

Explicitly out of scope — say so on a slide so nobody asks:
streaming, Delta/Iceberg (mention as "what I'd reach for next"), Airflow/Dagster,
Kubernetes, any ML, any cloud spend.

---

## 3. Week 1 — from scripts to a pipeline (days 5-9)

Goal: an analysis-ready curated table, produced by a repeatable job, on the
2 GB slice. Still `local[*]`. Speed of iteration matters more than scale.

**Day 5 — joins that mean something.**
Pull the dataset registry from the GBIF API (`api.gbif.org/v1/dataset`,
`/organization`): datasetkey -> publisher, publisher country, dataset type,
license. Cache it to disk like `.shard_cache.json` already does. Join it onto
occurrences.
Covers: broadcast vs sort-merge with *real* statistics (day 4 left this open —
answer the question by writing the lookup to parquet and seeing if Spark
broadcasts it unprompted), skew on `datasetkey`, salting one hot key and
measuring whether it helped, join types and the null-key trap.

**Day 6 — windows and explode at scale.**
Per-record flag sets, per-dataset flag vectors, rank flags within publisher,
first/last year each dataset appears, `lag` on a dataset's yearly counts.
Covers: `Window.partitionBy/orderBy`, the "window with no partitionBy collapses
to one partition" trap, `collect_set`, rolling counts. Note that `issue` is
`array<struct<array_element:string>>`, which day 2 already found.

**Day 7 — writing data, and MinIO.**
Write `curated/occurrence_slim`: pruned columns, derived fields (decade, grid
cell, usable-for-mapping boolean, flag count), partitioned sensibly. Write it to
the MinIO in docker-compose, not to local disk — `gbif.use_minio()` exists and
has never run.
Covers: `partitionBy` and how it explodes file counts, small-file problem,
`repartition` before write, `maxRecordsPerFile`, overwrite vs
`partitionOverwriteMode=dynamic`, compression codecs, and reading it back to
measure pushdown.

**Day 8 — the pipeline as code.**
Collapse days 2-7 from one-off scripts into modules with a CLI
(`--slice-gb`, `--out`, `--stage`). Add tests: build tiny DataFrames by hand,
assert transformations do what you claim. This is where a python UDF vs built-in
benchmark belongs — write the UDF, measure it, then delete it.
Covers: testing Spark code, separating transformation from I/O so it is testable
at all, configuration, why UDFs cost what they cost.

**Day 9 — SQL, catalog, and proving pushdown.**
Same three queries in DataFrame API and SQL, both `explain`ed. Register temp
views. Then the measurement day 4 set up: read the curated table with 6 of 50
columns and compare bytes actually read against file size.
Covers: SQL API, column pruning and predicate pushdown as *numbers* not claims,
parquet row-group statistics.

**Done when:** `uv run python -m pipeline --slice-gb 2` rebuilds the curated
table from scratch, tests pass, and you can state the usable-record percentage
for the slice.

---

## 4. Week 2 — scale and the cluster (days 10-14)

> **Changed after day 9 — see [WEEK3.md](WEEK3.md).** Days 10-14 became an
> optimisation week instead: read the Spark UI (10), partitioning (11),
> broadcast joins and skew (12), caching (13), before and after (14). The
> reason is in this section's own first line - "the same job, on the full
> 266 GB, on a real cluster, **with numbers explaining what mattered**". The
> numbers came first, and they said the job did not survive its own caching
> at 1 GB, let alone 266 GB. Taking a job that OOMs on a laptop and giving it
> to a cluster of smaller workers would have measured nothing.
>
> What that week answered anyway: day 11's scale ladder (run at three sizes
> and record everything) is day 14's matrix, and day 13's "tune with evidence,
> one thing at a time, keep a before/after table" is literally the deliverable.
> What is still owed: `spark-submit` to the docker cluster, driver-vs-executor
> in a real distributed setting, and the full-snapshot run.



Goal: the same job, on the full 266 GB, on a real cluster, with numbers
explaining what mattered.

**Day 10 — off local[*].**
`spark-submit` to `spark://spark-master:7077`. Package the code so workers can
import it (`--py-files` or a wheel). Fix the things that only break in
distributed mode: driver-side state, closures capturing unserializable objects,
paths that only exist on your laptop.
Covers: driver vs executor, deploy modes, why `local[*]` hid all of this.

**Day 11 — the scale ladder.**
Do not jump to 266 GB. Run at 2, 10, 50 GB and record: wall time, bytes read,
shuffle read/write, peak memory, spill, task time distribution. Plot it.
Extrapolate. Then check the extrapolation against reality on day 12.
Key insight to test: column pruning means 6 of 50 columns is *not* 266 GB of
network. Measure the real number — it may be an order of magnitude less, and
that is a genuinely good slide.
Covers: benchmarking method, reading the Spark UI SQL tab properly, knowing
cost before paying it.

**Day 12 — the full run.**
Run it once, overnight if needed. **Rule: full data in, small aggregates out.**
Write the reduced results to parquet and never read 266 GB again. Log
everything, including failures.
Covers: long-run operations, checkpointing, what actually breaks at 100x, S3
throttling and retry, the difference between a job that works and a job that
finishes.

**Day 13 — tuning with evidence.**
Now that you have a baseline, change one thing at a time: shuffle partitions vs
data size (day 4's open question — 24 is almost certainly wrong at 266 GB), AQE
skew join handling, executor memory and cores, spill thresholds, broadcast
threshold. Keep a before/after table.
Covers: the answers to every "should I?" in days 3-4, with measurements instead
of opinions.

**Day 14 — failure.**
Kill a worker mid-job on purpose. Corrupt an input path. Watch retries,
speculative execution, stage recomputation. Time a job with and without
`cache()` across a failure — day 3's open caching question answers itself here.
Covers: lineage and recomputation, fault tolerance as a real mechanism rather
than a slide, why it is genuinely worth the overhead.

**Done when:** the full-snapshot aggregates exist as a small parquet file, and
you have a table of what each tuning change did.

---

## 5. Week 3 — answer, attack, present (days 15-19)

Goal: the claim in section 1 is proved or killed, and there is a talk.

**Day 15 — the analysis.**
Results are small now, so pandas/duckdb is the honest tool — say so in the talk,
"the Spark part ends here" is a mature thing to say. Flag mix per publisher,
per decade, per basis of record. Variance within publisher vs between.

**Day 16 — try to kill the finding.**
This is the most valuable day and the easiest to skip. Alternative explanations
(is it the publisher, or the GBIF interpretation version, or just record age?),
sensitivity to your filter thresholds, and: rerun the day-2/3 headline numbers
from the slice against the full snapshot. **Every number that moved is a finding
about sampling.** That comparison is a slide on its own and directly answers
day 2's "are slice numbers OK to report".

**Day 17 — charts.**
Four or five, no more. Each one supports a sentence.

**Day 18 — write the talk** from the learning log (section 6). Not from memory.

**Day 19 — dry run, cut 30%, rehearse.**

Days 20-21: buffer. You will need it. If you don't, use it on the stretch list.

---

## 6. Capturing "what I learned and how I solved it"

This is half the presentation and it cannot be reconstructed afterwards. You are
already doing the right thing — days 2, 3 and 4 end in `findings` / `gotchas
hit` / `questions` blocks, and day 4's two gotchas (explain() memoises the
physical plan; `Dataset.rdd` is a lazy val) are exactly the kind of thing that
makes a talk memorable. Formalise it.

Create `logs/` and write one entry per problem that cost more than ~20 minutes,
while you are still annoyed:

```markdown
## day N — one-line symptom
**What I saw:** the actual error or wrong number.
**What I assumed:** the first, wrong theory.
**What it actually was:** the mechanism.
**How I found out:** explain() / Spark UI / docs / bisecting the query — be
specific, this is the transferable part.
**What I'd check first next time:** one sentence.
```

Also keep `logs/decisions.md`: one line per fork in the road, with the option
you rejected. "Chose X over Y because Z" is the backbone of the talk's middle.

Keep the `questions` habit from days 2-4, but add a rule: every open question
gets answered, killed, or explicitly deferred within a week. Day 4's four open
questions (broadcast statistics, shuffle partition sizing, salting vs AQE, slice
vs full) are all scheduled above — that is what a scope is for.

Slide-count sanity: a 30-minute talk is ~6 slides of findings and ~6 of
war stories. Twelve log entries is plenty. Do not try to tell all of them.

---

## 7. Presentation outline (write it now, fill it in later)

1. **The setup** — 266 GB, 9,898 files, a laptop. One question.
2. **The question** — "how much of GBIF can you actually use, and who decided?"
3. **What I built** — one architecture slide: S3 -> Spark -> curated parquet ->
   aggregates. Thirty seconds, no more.
4. **The finding** — the headline number, then the distribution that makes it
   interesting.
5. **Why you should doubt it** — day 16's work. Confidence goes *up* when you
   present this, not down.
6. **What fought me** — 4-6 war stories from `logs/`. The best ones are where
   the code was correct and slow, or correct and lying. Day 4's silent
   SortMergeJoin on a 13-row lookup is already a perfect example: nothing was
   broken, and it would have been permanently slow.
7. **What I'd do differently** — scope, tooling, the slice-vs-full call.
8. **What I'd learn next** — Iceberg, streaming, orchestration. One slide.

---

## 8. Risks, and what to do about them

- **The full run is the schedule risk.** 266 GB over a home connection is hours
  before Spark does any work. Mitigation: day 11's scale ladder tells you the
  cost in advance; run overnight; write aggregates once. If it is genuinely
  infeasible, a 50 GB stratified slice with the sampling caveat stated openly is
  a perfectly defensible result — and is itself a finding.
- **16 GB RAM, 2x2 GB workers.** The docker cluster is smaller than your
  `local[*]` driver at 8 GB. It is for learning distribution mechanics, not for
  speed. Expect the cluster to be *slower* and say so — that is a good slide,
  not an embarrassment.
- **Scope creep into analysis.** Every new question costs a day. Park them in
  `logs/decisions.md` and move on.
- **Week 3 compression.** If week 2 overruns, cut day 14 (failure) before you
  cut day 16 (attacking the finding). A finding you didn't try to kill is not a
  finding.
- **The finding might be boring.** "Flags are informative after all" is still a
  result. Have the day-16 comparison (slice vs full) in your pocket as a
  fallback headline — it is interesting regardless of how the main claim lands.

---

## 9. Stretch, only if days 20-21 are free

- Iceberg or Delta on MinIO — time travel, schema evolution, compaction
- A second snapshot month, diffed — what changes between GBIF releases
- CI that runs the tests and the pipeline on a 0.2 GB slice
- The `issue` enum documented properly from
  `api.gbif.org/v1/enumeration/basic/OccurrenceIssue` (answers day 2's third
  open question, and probably takes 20 minutes)
