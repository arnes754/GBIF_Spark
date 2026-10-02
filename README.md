# GBIF occurrence pipeline

PySpark job over the GBIF occurrence snapshot `2026-09-01`
(`s3://gbif-open-data-eu-central-1`, 9,898 parquet shards, about 266 GB).
It measures how much of GBIF is usable for 1-degree species distribution
mapping, broken down by country, decade and publisher. Runs on `local[*]`.

## Requirements

- Python 3.13
- Java 17 or 21 (`gbif.py` finds it; set `JAVA_HOME` to override)
- [uv](https://docs.astral.sh/uv/)
- About 2 GB of free disk and an internet connection

```bash
brew install openjdk@17 uv                  # macOS
sudo apt install openjdk-17-jdk             # Debian/Ubuntu, then install uv
```

## Setup

```bash
git clone <repo-url> && cd DataMonth3
uv sync
uv run python -c "import gbif; print(gbif.find_java_home())"   # checks Java
```

## Build the data

`data/` is not in git. The first step downloads from S3 (no credentials) and
takes about ten minutes for 2 GB.

```bash
uv run python curate.py build --gb 2        # fact table, appends a new batch
uv run python registry.py keys              # publisher dimension from the GBIF API
uv run python registry.py fetch
uv run python registry.py build
uv run python curate.py status              # check what was built
```

## Run

```bash
uv run python -m week2.day9                     # whole table
uv run python -m week2.day9 --batches b0000     # one ingest batch
uv run python -m week2.day9 --report            # compare previous runs
```

Run everything from the repo root. The scripts in `week*/` import the modules
at the top level, so they are run with `-m`.

Prints wall time, bytes read and shuffle per stage. Results go to
`data/results/`, and each run is logged to `data/reports/runs.jsonl`.
`uv run python -m week2.day9 --help` lists the tuning flags.

## Tests

No S3 needed, about 20 seconds each.

```bash
uv run python test_curate.py
uv run python test_bench.py
```

## Layout

```
gbif.py        Java lookup, S3 shard listing, Spark session
curate.py      builds the curated fact table from the snapshot
registry.py    builds the publisher dimension from the GBIF API
job.py         the pipeline (nine stages) and its Config
bench.py       metrics from the Spark UI REST API
run_ingest.sh  runs curate.py build in chunks, for bigger tables

week1/         day1-day4: exploration, transformations, plans and partitions
week2/         day5-day9: reads and writes, shuffles, joins, aggregations;
               day9.py is the command line for job.py
week3/         day10-day14: Spark UI, partitioning, broadcast and skew,
               caching, before and after
```

Each `dayN.py` is a standalone experiment. Days are numbered straight through
the three weeks, so `week2/day5.py` is week 2, day 1.

## Optimisation decisions

Measured on 1 GB and 5.7 GB slices (`week3/`).

- Caching: the fact table is not cached. It is a cheap scan plus a broadcast
  join, and caching it ran the driver out of memory above about 20 MB. Only the
  small per-dataset aggregate is cached.
- Writes: each aggregate is written by the stage that computes it, instead of
  being computed twice. 296s to 243s and 125 GB to 74 GB read on 5.7 GB.
- Joins: the dimension is 0.2 MB, so Spark broadcasts it without a hint. Left
  as is.
- Partitioning: shuffles are kilobytes and the input already packs into 15
  tasks, so the shuffle and input partition settings were left unchanged.
- Skew: one `datasetkey` has 48% of rows, but the join is a broadcast, so there
  is no shuffle to skew.
