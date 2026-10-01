# DataMonth3

Spark over the GBIF occurrence snapshot `2026-09-01`
(`s3://gbif-open-data-eu-central-1`, 9,898 parquet shards, ~266 GB).
Scope and plan: [SCOPE.md](SCOPE.md).

## The curated table

`curate.py` materialises a slice of the snapshot locally so no analysis run has
to pay S3 again, and grows it by appending rather than rebuilding.

```bash
uv run python curate.py build --gb 2      # pull 2 GB, transform, write
uv run python curate.py status            # what is in the table
uv run python curate.py build --gb 8      # +8 GB of DIFFERENT shards
uv run python curate.py preview           # read back, headline numbers
uv run python curate.py compact           # collapse batches, fix file count
uv run python curate.py drop-batch b0002  # undo one append
uv run python test_curate.py              # 40 assertions, no S3, ~20s
```

Layout: `data/curated/occurrence_slim/ingest_batch=bNNNN/decade=YYYY/*.parquet`

`ingest_batch` is the outer partition so an append only ever creates
directories — nothing existing is read or rewritten, and a crashed run is
undone with `rm -rf`. `decade` is inner because every question in SCOPE.md
filters or groups by time. The trade is `batches x decades` directories;
`compact` collapses them and gives up per-batch rollback to do it.

Which shards are already ingested lives in `data/manifests/<table>.json`, not
in the table. `build` picks the next increment from the shards *not* in it,
spread evenly over the remainder so each increment stays representative.
That local-only state is the limitation a real table format removes.

Both `data/` directories are gitignored — the table is reproducible from
`curate.py` plus the manifest.

## Transformations

`curate()` is a pure `DataFrame -> DataFrame` function (hence the tests). It
**never drops a row**: the question is what *fraction* of GBIF is usable, so
every filter is a column you can group by, not a `where` that deletes the
evidence.

| | |
|---|---|
| `issues` | `issue` is `array<struct<array_element:string>>`; flattened to a sorted `array<string>`, `NULL` -> `[]` so `size()` and `array_contains` work |
| `n_geo_issues`, `has_geo_issue` | only the 8 flags that actually break a coordinate — 93% of records carry *some* flag and the top one (82%) is informational |
| `basis`, `status`, `country`, `taxon_rank` | trimmed, upcased, `""` -> `NULL` so blanks and nulls stop being two groups |
| `taxon_class`, `taxon_order` | `class` and `order` are SQL keywords; renamed once here instead of backticked forever |
| `decade` | partition column, so never `NULL`: unknown or implausible (`< 1600`) year -> explicit `0` bucket |
| `interpreted_month` | `lastinterpreted` as `yyyy-MM`. The claim says reprocessing date predicts flags better than the observation does — a timestamp can't be grouped on |
| `has_coords`, `coord_valid`, `null_island` | three booleans, not one: "no coordinate" and "a coordinate that is a lie" are different findings |
| `uncertainty_m` | negative and absurd (> 20,000 km) values -> `NULL`; they are not information |
| `cell_lat`, `cell_lon`, `cell_id` | 1-degree grid, floored so a cell is `[n, n+1)` and its id names its own south-west corner |
| `usable_for_mapping` | the SCOPE §1 gate as one boolean: headline is `avg(usable_for_mapping)`, every breakdown is one `groupBy` |
| `src_shard` | which file a row came from, so a suspicious number traces back to a shard |

Thresholds (`UNCERTAINTY_MAX_M`, `MIN_PLAUSIBLE_YEAR`, `GEO_FATAL`) are
constants at the top of `curate.py` so day 16 can move them.

`uncertainty_ok` passes `NULL` uncertainty deliberately — only 32% of records
have one, and dropping them answers a different question. `uncertainty_known`
ships beside it, so the strict variant needs no rebuild.

Written with `repartition("decade")` (or every input task writes into every
decade directory), `maxRecordsPerFile` to split the two fat decades, and
`sortWithinPartitions("datasetkey")` — which is not cosmetic: it tightens each
parquet row group's min/max on `datasetkey`, which is what lets a later
predicate skip row groups instead of reading them.

## The dimension table

The snapshot knows `datasetkey` and nothing about who published it. SCOPE.md's
claim is about publishers, so the registry has to be joined on.

```bash
uv run python registry.py keys     # distinct datasetkeys in the fact table
uv run python registry.py fetch    # /dataset/{key} + /organization/{key}
uv run python registry.py build    # -> data/curated/dataset_dim  (0.2 MB)
uv run python registry.py show
```

Fetched **per key, not by paging**. `/dataset/search` stalls past offset
~30,000 - deep pagination makes an Elasticsearch-backed endpoint sort and
discard `offset` documents per shard per request. The slice references 1,473
datasets, not 53,651; fetching exactly those keys takes 29 s, has no offset in
it, is incremental, and scales with the fact table rather than with GBIF.

At 0.2 MB the dimension is far under `autoBroadcastJoinThreshold`, so Spark
broadcasts it unprompted - which is the answer to day 4's open question. Day
4's SortMergeJoin on a 13-row lookup was a missing *statistic* (an RDD-backed
relation has none), not a missing `F.broadcast()` hint.

## Measuring

`bench.py` wraps a block and reports wall time, bytes read, files opened, rows
scanned, shuffle bytes and spill.

```python
with bench.measure(spark, "count") as m:
    df.count()
print(m)
```

Numbers come from the driver's own UI at `:4040/api/v1`, and **from the SQL
endpoint, not `/stages`**. `/stages.inputBytes` is Hadoop `FileSystem`
statistics, which local parquet reads largely bypass: it reports 1.60 MB for a
2.3 GB scan. The SQL operator metrics are emitted by the operators themselves
and are exact.

Two things that number cannot tell you, both documented in `logs/gotchas.md`:

- `size of files read` is files **opened**, not bytes decoded. Partition
  pruning moves it; column pruning does not move it at all and is visible only
  as time.
- the SQL listener is **asynchronous**. Reading metrics the instant an action
  returns attributes each block's work to the next block - a table that is
  entirely wrong while looking entirely plausible. `bench._settle()` waits.

## Days

`day1.py` environment + read · `day2.py` profiling · `day3.py` transformations ·
`day4.py` plans and partitions.

| | |
|---|---|
| `day5.py` | **reading and writing at scale** - schema on read (plain vs `mergeSchema` vs explicit), the three pruning mechanisms as separate numbers, row-group skipping as the payoff for sorting at write time, five write layouts compared on file count and read-back cost, four compression codecs |
| `day6.py` | **transformations at scale** - narrow fuses into one stage, wide costs an Exchange; groupBy cost tracks *groups* not rows; exact vs approximate distinct; built-in vs pandas UDF vs python UDF; shuffle partitions moved one at a time; when caching is a tax |
| `day7.py` | **joins** - does Spark broadcast the dimension by itself and how it decides, broadcast vs sort-merge measured, the null-key trap, the four join types, skew on `datasetkey` with AQE vs hand-salting |
| `day8.py` | **aggregations and windowing** - one groupBy with many aggregates vs many groupBys, `cube` as a cardinality explosion, windows replacing aggregate-then-join-back, the claim as a variance decomposition, lag/rolling/cumulative, why skew hurts a window more than a join |
| `day9.py` | **the end-to-end run** - nine stages, each timed, bytes and shuffle attributed; results written small; read back and verified; every run appended to `data/reports/runs.jsonl` |

```bash
uv run python day9.py --batches b0000 --tag baseline
uv run python day9.py --batches b0000 --cache none --write-in-place --tag tuned
uv run python day9.py --report          # compare runs, stage breakdown
```

Rule, from SCOPE.md section 4: **full data in, small aggregates out.** The
inputs are gigabytes, the outputs are kilobytes, and nothing downstream reads
the fact table again.

## Week 3 - optimising it

Full write-up with every measurement: **[WEEK3.md](WEEK3.md)**.

The job itself moved out of `day9.py` into `job.py` - nine stage functions and
a `Config` of everything a run is allowed to differ by. No stage reads a
global, so two runs differ by exactly the fields that differ in their Config,
which is what lets day 14 print the difference as a table. `day9.py` is now the
CLI and the run log.

| | |
|---|---|
| `day10.py` | **reading the Spark UI** - which tab answers which question; stages ranked by wall time; every Exchange with its bytes; per-task min/median/max so skew is distinguishable from slowness. `--hold` keeps the UI up |
| `day11.py` | **partitioning** - input / shuffle / output partitions kept apart; `maxPartitionBytes` and the open-cost charge; which filters reach the directory listing; 12/48/200/800 shuffle partitions with AQE on and off; repartition vs coalesce vs neither, with the read-back cost |
| `day12.py` | **broadcast joins and skew** - whether Spark broadcasts the dimension unprompted and how it decides; broadcast vs forced sort-merge; how skewed `datasetkey` really is; AQE skew join on and off; hand-salting measured against it |
| `day13.py` | **caching** - how many times the job really reads the enriched table; the three storage levels end to end; the break-even arithmetic; and the slice where the cache does not fit |
| `day14.py` | **before and after** - three configurations x three slices, each a fresh JVM, with the failures kept in the table |

```bash
uv run python day10.py --batches b0000 --hold
uv run python day14.py --slices b0003,b0000,b0004 --warm
uv run python day14.py --report
```

**What changed, and what did not.** Two changes, both from measurements:

- **the cache came out.** Day 9 persisted the enriched fact table because seven
  stages read it. That is the right question and the wrong answer - the thing
  being cached is a parquet scan plus a broadcast join, cheap to recompute and
  expensive to store, and it OOM'd the driver on every slice above 20 MB. The
  small, expensive intermediate (`by_dataset`, ~1,400 rows, read by three
  stages) is persisted instead.
- **aggregates are computed once, not twice.** Every result DataFrame was
  forced with `count()` to time it and then recomputed by the write stage: 22
  passes over the fact table for 11 outputs. Each stage now writes what it
  built.

Three days ended in *no change*, each with the measurement that says why:
shuffle partitions (the shuffles are kilobytes and AQE coalesces them anyway),
input partitioning (the files are already near `maxPartitionBytes`), and
broadcast/skew (the join is already a broadcast, so there is no shuffle and
therefore no skew). Those are in `logs/decisions.md` with the rejected option.

## Log

`logs/gotchas.md` - one entry per problem that cost more than ~20 minutes,
written while still annoyed. `logs/decisions.md` - one line per fork in the
road, with the option rejected.
