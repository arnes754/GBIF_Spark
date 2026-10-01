#!/bin/bash
# Ingest more shards as a sequence of fixed-size batches.
#
#   ./run_ingest.sh [CHUNKS] [GB_PER_CHUNK] [DRIVER_MEM]
#   ./run_ingest.sh 10 10 5g      # ~100 GB as 10 x 10 GB
#
# Batches rather than one big build: each chunk is its own ingest_batch=
# directory and manifest entry, so a failure hours in loses one chunk rather
# than the whole run.
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
  # One retry. Startup can fail on an ephemeral port collision before any
  # data is touched, and a retry is safe: a chunk that failed wrote nothing
  # to the manifest, so it picks its shards again.
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
