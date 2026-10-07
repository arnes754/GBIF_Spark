"""Day 9 - the end-to-end run, timed stage by stage.

Days 5-8 each answered one question in isolation. This is the job: read the
curated table, enrich it with the registry dimension, compute every aggregate
the analysis needs, write the results, read them back to check them.

The rule is full data in, small aggregates out. The
inputs are gigabytes, the outputs are kilobytes, and nothing downstream ever
reads the fact table again.

    uv run python -m gbif_spark day 9 --batches b0004 --tag run
    uv run python -m gbif_spark day 9 --batches b0004 --cache memory_and_disk \
        --no-write-in-place --no-cache-results --tag as-day-9-shipped
    uv run python -m gbif_spark day 9 --report          # compare past runs

Every run appends a row to data/reports/runs.jsonl with per-stage wall time,
bytes read and bytes shuffled, so "did that change help" is a lookup rather
than a memory.

WEEK 3 NOTE: the nine stages moved into `job.py` so days 10-14 can run them
with one knob moved. This file is now the CLI and the run log; the pipeline
itself is `job.run()`. Nothing about what it computes changed.

AFTER WEEK 3: the CLI and the run log moved out as well, to
pipeline/run_job.py, so the end-to-end command (`python -m gbif_spark all`)
runs the job without going through a day file. This file is kept so the
commands above, and day14.py's subprocesses, work as before.
"""
from gbif_spark.pipeline.run_job import main

if __name__ == "__main__":
    main()
