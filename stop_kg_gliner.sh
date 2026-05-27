#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
PID_FILE="kg_gliner.pid"
if [[ -f "$PID_FILE" ]]; then
  kill "$(cat "$PID_FILE")" 2>/dev/null || true
  rm -f "$PID_FILE"
  echo "kg_gliner worker stopped"
else
  echo "no kg_gliner.pid"
fi
