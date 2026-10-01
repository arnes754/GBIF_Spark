# Decisions

One line per fork in the road, with the option rejected. "Chose X over Y
because Z" is the backbone of the talk's middle section.

- **Dimension table fetched per-key, not by paging the search API.** Rejected
  paging `/dataset/search` (53,651 datasets, 54 pages) because deep pagination
  stalls past offset ~30,000. Fetching `/dataset/{key}` for the 1,473 keys the
  fact table actually contains takes 29 s and scales with the fact table, not
  with GBIF. See `logs/gotchas.md`.

- **Registry cache is two JSONL files joined in Python, not two Spark reads.**
  They are 1,473 and 240 rows. Starting a shuffle to join two dicts is exactly
  the instinct this project is meant to cure.

- **Benchmarks read the SQL-tab metrics, not `/stages`.** `/stages.inputBytes`
  is Hadoop FileSystem statistics and reads ~0 for local parquet. Rejected
  writing a SparkListener (more code, same numbers, has to cross into the JVM).

- **Broadcast the dimension rather than hint every join.** Rejected
  `F.broadcast()` everywhere once day 7 showed Spark broadcasts a
  parquet-backed dimension unprompted - the day-4 failure was a missing
  *statistic*, not a missing hint. `F.broadcast()` is kept only for forcing the
  comparison.

- **Left join + count the nulls, never inner join.** An inner join on
  `specieskey` silently deletes ~8% of rows, and those rows are exactly the
  unusable ones the project exists to count. Same rule as `curate()`: never
  drop a row, make the filter a column.

- **Aggregate first, window second.** Rejected windowing over the 58M-row fact
  table. Windows cannot reduce map-side and AQE will not split a skewed window
  partition, so every window in day 8 runs over the ~1.4k-row per-dataset
  aggregate instead.

- **AQE skew handling over hand-salting.** Implemented and measured both. Hand
  salting costs an extra shuffle to build the salted dimension and AQE needs no
  code and cannot be wrong about which key is hot. Kept the salting code as a
  measured comparison, not as the answer. (For this join both are moot -
  broadcasting removes the shuffle, and skew is a shuffle problem.)

- **approx_count_distinct for headline cardinalities.** 2-5% error on
  "how many species" is free; exact distinct shuffles in proportion to
  cardinality. Rejected exact counts except where the number is a join key.

- **zstd for result tables, snappy kept for the curated table.** zstd is ~28%
  smaller at comparable read speed. Not rebuilding the curated table for it -
  the saving is real but the rebuild costs an S3 pull, and that is a day-12
  decision, not a day-7 one.

- **Day-by-day scripts kept as scripts.** Rejected collapsing days 5-9 into the
  `pipeline` module now. The scripts are the lab notebook and they are what the
  talk is drawn from; `day9.py` is the thing that has to become a job, and it
  already takes `--out`/`--tag`/`--shuffle-partitions` in preparation.

- **pandas pinned below 3.0.** PySpark 4.2 warns that pandas >= 3 is not fully
  supported; the pandas-UDF benchmark is a teaching point and it has to run.

- **Driver memory 4 GB, not 8.** This laptop has 16 GB and shares it with
  Docker Desktop (7.65 GiB ceiling, 8 of 12 CPUs, an unrelated project). An
  oversized JVM heap does not fail loudly here - it starves the python workers
  and the OS kills them. Rejected "give Spark as much as possible": the curated
  table is 2.3 GB before column pruning, so the heap was never the constraint.

- **UDF benchmark runs over 500k rows on 4 partitions, not 6.4M on 12.** The
  point of the section is the ratio between three implementations, and the
  ratio is visible at any size. The full-size version crashed the driver.

- **The job extracted into `job.py`; day 9 kept as its CLI.** Rejected copying
  the pipeline into each week-3 script. Five scripts have to run the same nine
  stages with one setting moved, and five copies of a pipeline diverge by day
  three. The stage functions read no globals, so a run is fully described by
  its `Config` — which is what lets day 14 print the difference between two
  runs as a table instead of as prose.

- **No fact-table cache (`--cache none` is the default).** Rejected day 9's
  `MEMORY_AND_DISK` on the enriched table. It was added because seven stages
  read that table, which is the right question and the wrong answer: the thing
  being cached is a parquet scan plus a broadcast join — cheap to recompute,
  expensive to store, and impossible to column-prune once cached. It also
  OOM'd the driver on every slice above 20 MB. Kept as a flag, not a default.

- **Cache the small expensive intermediate instead.** `by_dataset` is ~1,400
  rows produced by a groupBy over the whole fact table, and three stages read
  it. That is the shape caching is actually for: small, and expensive to
  produce. The fact table is the opposite shape on both counts.

- **Each aggregate stage writes its own results (`--write-in-place`).**
  Rejected keeping the single write stage at the end. Forcing an aggregate
  with `count()` to time it and then writing the same lazy DataFrame computes
  it twice; on a 1.1 GB slice the job read 22.98 GB. Letting the write be the
  forcing action keeps the per-stage timings honest *and* computes each
  aggregate once.

- **Shuffle partitions left at 48.** Measured 12 / 48 / 200 / 800, with AQE on
  and off. The job's shuffles are kilobytes — every aggregate reduces hard on
  the map side — so the setting is choosing between 48 and 800 near-empty
  tasks, and AQE coalesces them away regardless. Rejected "tune it anyway":
  a number in a config file that no measurement supports is worse than the
  default, because the next person has to assume it was deliberate.

- **Input partitioning left at the 128 MB default.** The curated table's files
  are already close to that size, so the default gives roughly one task per
  file and saturates twelve cores. Nothing to win.

- **`F.broadcast()` stays out of the default join path.** Spark already
  broadcasts the 0.2 MB parquet dimension on its own, because parquet relations
  carry a real size statistic. Rejected hinting it anyway: a hint that is
  currently redundant is a hint that will be wrong after the dimension grows,
  and nobody will remember to check.

- **AQE skew handling stays on, and nothing else is done about skew.**
  `datasetkey` is genuinely skewed, but the job's only join is a broadcast, so
  there is no shuffle on it and therefore no skew to handle. Salting was
  implemented and measured on a forced sort-merge join purely as the
  comparison; it is not in the job.

- **Day 14 runs three configurations, not two.** "We changed several things
  and it got faster" is not a measurement. `nocache` sits between `before` and
  `after` so the table attributes the difference to a specific change.

- **Each day-14 run is a fresh JVM subprocess.** Rejected looping inside one
  session. A session carries cached blocks, a warm JIT and conf values set by
  the previous experiment, so a loop measures the order you ran things in.
