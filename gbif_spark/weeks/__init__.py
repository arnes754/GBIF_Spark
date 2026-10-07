"""The project day by day.

Each dayN.py is a standalone experiment that prints its measurements and ends
with notes: findings, gotchas and open questions. Together they show how the
pipeline got to its current shape; none of them is needed for the result.
Days are numbered straight through the three weeks, so week2/day5.py is week
2, day 1.

    week1/   days 1-4     the snapshot on S3: session, profiling,
                          transformations, plans and partitions
    week2/   days 5-9     the local curated table: reads and writes, shuffles,
                          joins, aggregations and windows, the first full run
    week3/   days 10-14   optimisation: profiling, partitioning, broadcast and
                          skew, caching, before and after

Run one by its number: uv run python -m gbif_spark day 5
"""
