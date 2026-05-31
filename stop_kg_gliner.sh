#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if systemctl --user is-enabled smoldocling-kg-gliner-worker.service &>/dev/null; then
  systemctl --user stop smoldocling-kg-gliner-worker.service
  rm -f kg_gliner.pid
  echo "smoldocling-kg-gliner-worker stopped"
  exit 0
fi

PID_FILE="kg_gliner.pid"
if [[ -f "$PID_FILE" ]]; then
  kill "$(cat "$PID_FILE")" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "kg_gliner worker stopped"
else
  echo "no kg_gliner.pid"
fi
