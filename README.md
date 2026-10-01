# GBIF occurrence pipeline

A PySpark pipeline over the GBIF occurrence snapshot `2026-09-01`
(`s3://gbif-open-data-eu-central-1`, 9,898 parquet shards, about 266 GB).

It answers one question: how much of GBIF is actually usable for 1-degree
species distribution mapping, and how that breaks down by country, decade and
publisher.

The pipeline has three parts:

1. `curate.py` pulls shards from S3, transforms them and appends them to a
   local parquet table.
2. `registry.py` builds a publisher dimension from the GBIF registry API.
3. `job.py` reads both, joins them and writes about a dozen small aggregate
   tables. `day9.py` is its command line interface.

Everything runs on `local[*]`. There is no cluster.

## Requirements

- Python 3.13
- A Java 17 or 21 JDK (`gbif.py` locates it; set `JAVA_HOME` to override)
- [uv](https://docs.astral.sh/uv/)
- About 2 GB of free disk for the smallest useful table, and an internet
  connection for the first build

```bash
# macOS
brew install openjdk@17 uv

# Debian/Ubuntu
sudo apt install openjdk-17-jdk
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Setup

```bash
git clone <repo-url>
cd DataMonth3
uv sync          # creates .venv from uv.lock
uv run python -c "import gbif; print(gbif.find_java_home())"
```

That last line prints the JDK it will use, or an error telling you what to
install. Dependency versions are pinned in `uv.lock`, so `uv sync` gives the
same environment it was developed against.

## Build the data

`data/` is gitignored, so a fresh clone has none. Build it in three steps.
The first one downloads from S3 and is the slow part; 2 GB takes roughly ten
minutes on a home connection.

```bash
# 1. the fact table (repeat with a bigger --gb to append more shards)
uv run python curate.py build --gb 2
uv run python curate.py status

# 2. the publisher dimension (GBIF API, no credentials needed)
uv run python registry.py keys
uv run python registry.py fetch
uv run python registry.py build

# 3. check it
uv run python curate.py preview
uv run python registry.py show
```

`build` is incremental and append-only. Running it again picks shards that are
not already in the table, writes them to a new `ingest_batch=` directory and
records them in `data/manifests/`. Nothing existing is read or rewritten, so an
interrupted run is undone with `drop-batch`.

```bash
uv run python curate.py build --gb 8        # +8 GB of different shards
uv run python curate.py compact             # collapse batches, fewer files
uv run python curate.py drop-batch b0002    # undo one append
```

## Run the job

```bash
uv run python day9.py                       # whole table
uv run python day9.py --batches b0000       # one ingest batch
uv run python day9.py --report              # compare previous runs
```

It prints a per-stage table of wall time, bytes read, bytes shuffled and spill,
then the headline numbers. Results go to `data/results/` as parquet, and every
run appends a row to `data/reports/runs.jsonl`.

Useful flags:

```
--batches b0000,b0004      run on named ingest batches instead of everything
--shuffle-partitions N     spark.sql.shuffle.partitions
--cache LEVEL              none | memory | memory_and_disk | disk
--join MODE                auto | broadcast | sortmerge
--no-write-in-place        write all results in one stage at the end
--driver-memory 4g
--tag NAME                 label the run in runs.jsonl
```

`SPARK_CORES` and `SPARK_MEM` override the thread count and heap without
editing anything:

```bash
SPARK_CORES=4 SPARK_MEM=3g uv run python day9.py --batches b0000
```

## Tests

No Spark cluster, no S3, about 20 seconds each.

```bash
uv run python test_curate.py     # the transformations, on hand-built DataFrames
uv run python test_bench.py      # metric parsing and the registry's licence mapping
```

## Layout

```
gbif.py        Java lookup, S3 shard listing, Spark session builder
curate.py      build the curated fact table from the snapshot
registry.py    build the publisher dimension from the GBIF API
bench.py       measurement harness, reads the driver UI REST API
job.py         the pipeline: nine stages and a Config of every tunable setting
day9.py        command line interface over job.py, plus the run log
run_ingest.sh  loop curate.py build in fixed-size chunks, with one retry

day1.py        environment and first read
day2.py        profiling the snapshot
day3.py        transformations
day4.py        physical plans and partitions
day5.py        reading and writing at scale
day6.py        narrow vs wide transformations, UDF cost, shuffle partitions
day7.py        joins, broadcast vs sort-merge, skew
day8.py        aggregations and windows
day10.py       profiling the job, reading the Spark UI
day11.py       partitioning: input, shuffle, output
day12.py       broadcast joins and skew
day13.py       caching and persistence
day14.py       running the job under several configurations and comparing
```

`day1.py` to `day14.py` are the experiments the pipeline was built from. They
are standalone scripts and are not imported by anything. Each takes
`--batches` or reads `TABLE` from the environment, and each prints its own
measurements.

## Table layout

```
data/curated/occurrence_slim/ingest_batch=bNNNN/decade=YYYY/*.parquet
data/curated/dataset_dim/
data/manifests/occurrence_slim.json
data/results/
data/reports/runs.jsonl
```

`ingest_batch` is the outer partition so an append only ever creates files.
`decade` is inner because the queries filter and group by time. The cost is
`batches x decades` directories, which `compact` collapses.

## Transformations

`curate()` is a pure `DataFrame -> DataFrame` function, which is what makes it
testable without Spark reading anything. It never drops a row: the question is
what fraction of the data is usable, so every filter is a column you can group
by rather than a `where`.

| column | what it is |
|---|---|
| `issues` | `issue` is `array<struct<array_element:string>>`; flattened to a sorted `array<string>`, NULL to `[]` |
| `n_geo_issues`, `has_geo_issue` | only the 8 flags that actually break a coordinate |
| `basis`, `status`, `country`, `taxon_rank` | trimmed, upcased, `""` to NULL |
| `taxon_class`, `taxon_order` | `class` and `order` are SQL keywords, renamed once here |
| `decade` | partition column, so never NULL: unknown or implausible year becomes `0` |
| `interpreted_month` | `lastinterpreted` as `yyyy-MM`, so it can be grouped on |
| `has_coords`, `coord_valid`, `null_island` | three booleans, because "no coordinate" and "a wrong coordinate" are different |
| `uncertainty_m` | negative and absurd values become NULL |
| `cell_lat`, `cell_lon`, `cell_id` | 1-degree grid, floored so a cell is `[n, n+1)` |
| `usable_for_mapping` | the whole gate as one boolean |
| `src_shard` | which file a row came from |

Thresholds (`UNCERTAINTY_MAX_M`, `MIN_PLAUSIBLE_YEAR`, `GEO_FATAL`) are
constants at the top of `curate.py`.

## Measuring

`bench.py` wraps a block and reports wall time, bytes read, files opened, rows
scanned, shuffle bytes and spill.

```python
with bench.measure(spark, "count") as m:
    df.count()
print(m)
```

The numbers come from the driver UI REST API, from the SQL endpoint rather than
`/stages`. `/stages.inputBytes` is Hadoop `FileSystem` statistics, which local
parquet reads largely bypass: it reported 1.60 MB for a 2.3 GB scan. The SQL
operator metrics are emitted by the operators themselves.

Two things to know about those numbers:

- `size of files read` counts files opened, not bytes decoded. Partition
  pruning moves it; column pruning does not move it at all and shows up only
  as time.
- The SQL listener is asynchronous. Reading metrics the instant an action
  returns attributes each block's work to the next block. `bench._settle()`
  waits for them to land.

## Optimisation notes

Measured on a 1 GB and a 5.7 GB slice. Two changes made:

- **No cache on the fact table.** It was persisted because several stages read
  it, but it is a parquet scan plus a broadcast join, so recomputing it is
  cheap, and a cache cannot prune columns per reader. It also OOMed the driver
  on anything above about 20 MB. The small per-dataset aggregate is persisted
  instead, since three stages read it and it costs a full groupBy to produce.
- **Each aggregate written by the stage that builds it.** Every result
  DataFrame used to be forced with `count()` to time it and then recomputed by
  the write stage: 22 passes over the fact table for 11 outputs, now 13. On
  the 5.7 GB slice that is 125 GB read down to 74 GB, and wall time 296s down
  to 243s.

Four things were measured and left alone:

- Shuffle partitions. The shuffles are kilobytes, because every aggregate
  reduces on the map side. 48 against 800 is 48 against 800 near-empty tasks,
  and AQE coalesces them to the same 17 either way.
- Input partitioning. Half the files are under 1 MB but ten files of 64-128 MB
  hold 80% of the bytes, so the default 128 MB `maxPartitionBytes` already
  packs a 1 GB slice into 15 tasks for 12 cores.
- Broadcast joins. The dimension is 0.2 MB of parquet and carries a real size
  statistic, so Spark broadcasts it without a hint. Forcing sort-merge shuffles
  10 MB and spills 328 MB instead.
- Skew. One `datasetkey` holds 48% of the rows, but the only join is a
  broadcast, so there is no shuffle and therefore no skew on it. AQE skew join
  stays on because it costs nothing when idle.

`day10.py` through `day14.py` reproduce all of this.

## Not included

No cluster, no MinIO, no orchestration, no streaming, no table format
(Iceberg/Delta), no ML. The job runs on one machine.
