#!/bin/bash
# Ingest more shards as a sequence of fixed-size batches.
#
#   ./run_ingest.sh [CHUNKS] [GB_PER_CHUNK] [DRIVER_MEM]
#   ./run_ingest.sh 10 10 5g      # ~100 GB as 10 x 10 GB
#
# Why batches and not one big `build`: each chunk is its own ingest_batch=
# directory and its own manifest entry, so a failure six hours in loses one
# chunk, not the lot. Nothing already written is ever touched.
cd "$(dirname "$0")"
CHUNKS=${1:-5}
GB=${2:-10}
MEM=${3:-5g}
mkdir -p logs
echo "=== run start $(date '+%F %T'): ${CHUNKS} x ${GB} GB, driver ${MEM} ===" >> logs/ingest.log
for i in $(seq 1 "$CHUNKS"); do
  echo "=== chunk $i/$CHUNKS  $(date '+%F %T') ===" >> logs/ingest.log
  uv run python -u curate.py --driver-memory "$MEM" build --gb "$GB" >> logs/ingest.log 2>&1
  rc=$?
  # One retry. Chunk 16 of a 20-chunk run died with "Too large frame" while
  # INITIALISING SparkContext - an ephemeral port collision before any data was
  # touched. That is not a reason to abandon the remaining chunks, and the
  # append-only design means a retry is always safe: a chunk that failed wrote
  # nothing to the manifest, so it simply picks its shards again.
  if [ $rc -ne 0 ]; then
    echo "=== chunk $i attempt 1 failed (exit $rc), retrying in 30s ===" >> logs/ingest.log
    sleep 30
    uv run python -u curate.py --driver-memory "$MEM" build --gb "$GB" >> logs/ingest.log 2>&1
    rc=$?
  fi
  echo "=== chunk $i/$CHUNKS exit=$rc  $(date '+%F %T') ===" >> logs/ingest.log
  if [ $rc -ne 0 ]; then
    echo "chunk $i FAILED (exit $rc), stopping" >> logs/ingest.log
    exit $rc
  fi
done
echo "=== all $CHUNKS chunks done $(date '+%F %T') ===" >> logs/ingest.log
