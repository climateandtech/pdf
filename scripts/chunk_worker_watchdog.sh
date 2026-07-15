#!/usr/bin/env bash
# Restart the chunk worker when its heartbeat file goes stale.
set -euo pipefail

HEARTBEAT_PATH="${CHUNK_WORKER_HEARTBEAT_PATH:-/home/smoldocling/apps/pdf/chunk-worker.heartbeat}"
TIMEOUT_S="${CHUNK_JOB_TIMEOUT_S:-3600}"
MARGIN_S="${CHUNK_WORKER_WATCHDOG_MARGIN_S:-300}"
UNIT="${CHUNK_WORKER_UNIT:-smoldocling-docling-chunk-worker.service}"
MAX_AGE=$((TIMEOUT_S + MARGIN_S))

if [[ ! -f "$HEARTBEAT_PATH" ]]; then
  echo "chunk-worker watchdog: missing heartbeat file $HEARTBEAT_PATH — restarting $UNIT"
  systemctl --user restart "$UNIT"
  exit 0
fi

now=$(date +%s)
mtime=$(stat -c %Y "$HEARTBEAT_PATH" 2>/dev/null || stat -f %m "$HEARTBEAT_PATH")
age=$((now - mtime))
if (( age > MAX_AGE )); then
  echo "chunk-worker watchdog: heartbeat age ${age}s > ${MAX_AGE}s — restarting $UNIT"
  systemctl --user restart "$UNIT"
else
  echo "chunk-worker watchdog: ok age=${age}s limit=${MAX_AGE}s"
fi
