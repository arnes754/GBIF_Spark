# GBIF occurrence pipeline

PySpark pipeline over the GBIF occurrence snapshot `2026-09-01`: how much of
GBIF is usable for 1-degree species distribution mapping, by country, decade
and publisher.

## Requirements

- macOS or Linux (on Windows, use WSL)
- Python 3.13, Java 17 or 21, [uv](https://docs.astral.sh/uv/)
- About 2 GB of free disk and an internet connection

```bash
brew install openjdk@17 uv          # macOS
sudo apt install openjdk-17-jdk     # Debian/Ubuntu, then install uv
```

## Setup

```bash
git clone https://github.com/arnes754/GBIF_Spark.git && cd GBIF_Spark
uv sync
```

Run everything below from the repo root.

## Run everything

```bash
uv run python -m gbif_spark all       # build the data, run the job, print the answer
uv run python -m gbif_spark status    # what is built, and past runs
```

## Build the data

```bash
uv run python -m gbif_spark data
```

Or one step at a time:

```bash
uv run python -m gbif_spark curate build --gb 2
uv run python -m gbif_spark registry keys
uv run python -m gbif_spark registry fetch
uv run python -m gbif_spark registry build
```

## Run the job

```bash
uv run python -m gbif_spark job                    # whole table
uv run python -m gbif_spark job --batches b0000    # one ingest batch
uv run python -m gbif_spark job --report           # compare runs
```

## Tests

```bash
uv run python -m gbif_spark test
```

## Day-by-day experiments

```bash
uv run python -m gbif_spark day 5     # any day from 1 to 14
```
