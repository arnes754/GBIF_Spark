"""Where everything lives on disk.

Everything the pipeline writes goes under gbif_spark/data/: the curated
tables, the registry cache, the results and run reports, logs, and the cached
S3 listing. git keeps the empty folder (data/.gitkeep) and ignores what is in
it.

GBIF_DATA_DIR moves all of it somewhere else, for example to a bigger disk:

    export GBIF_DATA_DIR=/Volumes/external/gbif-data

Every module takes its paths from here, so nothing depends on the directory a
command was started from.
"""
import os
import pathlib

PACKAGE = pathlib.Path(__file__).resolve().parent
REPO = PACKAGE.parent
DATA = pathlib.Path(os.environ.get("GBIF_DATA_DIR") or PACKAGE / "data").resolve()

FACT_TABLE = DATA / "curated" / "occurrence_slim"
MANIFESTS = DATA / "manifests"

REGISTRY = DATA / "registry"
DIMENSION = DATA / "curated" / "dataset_dim"

RESULTS = DATA / "results"
REPORTS = DATA / "reports"
RUNS_LOG = REPORTS / "runs.jsonl"

SCRATCH = DATA / "scratch"
LOGS = DATA / "logs"
SHARD_CACHE = DATA / "shard_cache.json"
