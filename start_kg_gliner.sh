#!/usr/bin/env bash
# Start GLiNER NATS infer worker (kg.infer) — same GPU host / repo as docling worker.
set -euo pipefail

CURRENT_DIR=$(cd "$(dirname "$0")" && pwd)
cd "$CURRENT_DIR"

PID_FILE="$CURRENT_DIR/kg_gliner.pid"
LOG_FILE="$CURRENT_DIR/kg_gliner.log"
WORKER_SCRIPT="$CURRENT_DIR/kg_gliner_worker.py"

if [[ ! -f "$WORKER_SCRIPT" ]]; then
  echo "Error: missing kg_gliner_worker.py" >&2
  exit 1
fi

if [[ ! -d "venv" ]]; then
  echo "Error: venv not found. Create venv and: pip install -r requirements.txt -r requirements-gliner.txt" >&2
  exit 1
fi

if [[ ! -f ".env" ]]; then
  echo "Error: .env missing (NATS_URL, NATS_TOKEN). Use gpu-sync-nats-env.sh from coolify-provisioning." >&2
  exit 1
fi

if [[ -f "$PID_FILE" ]]; then
  OLD_PID=$(cat "$PID_FILE")
  if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "kg_gliner worker already running pid=$OLD_PID"
    exit 1
  fi
  rm -f "$PID_FILE"
fi

source venv/bin/activate
nohup python "$WORKER_SCRIPT" >>"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
sleep 2
if kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  echo "kg_gliner worker started pid=$(cat "$PID_FILE") log=$LOG_FILE"
else
  echo "kg_gliner worker failed to start:" >&2
  tail -20 "$LOG_FILE" >&2
  rm -f "$PID_FILE"
  exit 1
fi
