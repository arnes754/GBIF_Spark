"""PySpark pipeline over the GBIF occurrence snapshot.

    __main__.py   the end-to-end command: uv run python -m gbif_spark all
    paths.py      where everything lives on disk, all of it under data/
    pipeline/     the steps that produce the result: curate.py (fact table),
                  registry.py (publisher dimension), job.py and run_job.py
                  (the nine-stage job and its command line)
    helpers/      shared plumbing: gbif.py (Java, S3 listing, Spark session)
                  and bench.py (metrics from the Spark UI)
    scripts/      run_ingest.sh, a bigger ingest in fixed-size chunks
    weeks/        the project day by day, one experiment per day
    tests/        uv run python -m gbif_spark test
    data/         everything the pipeline writes; not in git
"""
