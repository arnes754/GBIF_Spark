#!/bin/bash
cd "$(dirname "$0")/../.."
CHUNKS=${1:-5}
GB=${2:-10}
MEM=${3:-5g}
LOG="${GBIF_DATA_DIR:-gbif_spark/data}/logs/ingest.log"
mkdir -p "$(dirname "$LOG")"
echo "=== run start $(date '+%F %T'): ${CHUNKS} x ${GB} GB, driver ${MEM} ===" >> "$LOG"
for i in $(seq 1 "$CHUNKS"); do
  echo "=== chunk $i/$CHUNKS  $(date '+%F %T') ===" >> "$LOG"
  uv run python -u -m gbif_spark curate --driver-memory "$MEM" build --gb "$GB" >> "$LOG" 2>&1
  rc=$?
  if [ $rc -ne 0 ]; then
    echo "=== chunk $i attempt 1 failed (exit $rc), retrying in 30s ===" >> "$LOG"
    sleep 30
    uv run python -u -m gbif_spark curate --driver-memory "$MEM" build --gb "$GB" >> "$LOG" 2>&1
    rc=$?
  fi
  echo "=== chunk $i/$CHUNKS exit=$rc  $(date '+%F %T') ===" >> "$LOG"
  if [ $rc -ne 0 ]; then
    echo "chunk $i FAILED (exit $rc), stopping" >> "$LOG"
    exit $rc
  fi
done
echo "=== all $CHUNKS chunks done $(date '+%F %T') ===" >> "$LOG"
