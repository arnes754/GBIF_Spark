"""The steps from the S3 snapshot to the result tables.

    curate.py     the curated fact table, built from parquet shards on S3
    registry.py   the publisher dimension, from the GBIF registry API
    job.py        the nine-stage job and the Config it runs from
    run_job.py    the job's command line, and the run log in data/reports/

`uv run python -m gbif_spark all` runs them in that order.
"""
