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
