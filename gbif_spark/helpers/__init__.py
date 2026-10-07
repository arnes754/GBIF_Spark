"""Shared plumbing for the pipeline, the weeks and the tests.

    gbif.py    finds Java, lists the snapshot's shards on S3, builds the Spark
               session
    bench.py   wall time, bytes read, shuffle and spill for a block of Spark
               work, read from the driver UI's REST API
"""
