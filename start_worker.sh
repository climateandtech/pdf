#!/bin/bash

# Start PDF Docling Worker as a background process
# Simple approach without systemd

set -e

echo "🚀 Starting PDF Docling Worker"
echo "=============================="

# Get current directory
CURRENT_DIR=$(pwd)
WORKER_SCRIPT="$CURRENT_DIR/docling_worker.py"
PID_FILE="$CURRENT_DIR/worker.pid"
LOG_FILE="$CURRENT_DIR/worker.log"

# Check if we're in the right directory
if [[ ! -f "docling_worker.py" ]]; then
    echo "❌ Error: Must run from pdf/ directory containing docling_worker.py"
    exit 1
fi

# Check if venv exists
if [[ ! -d "venv" ]]; then
    echo "❌ Error: Virtual environment not found. Run: python -m venv venv && pip install -r requirements.txt"
    exit 1
fi

# Check if .env exists
if [[ ! -f ".env" ]]; then
    echo "❌ Error: .env file not found. Copy from environment_config.txt and configure."
    exit 1
fi

# Check if already running
if [[ -f "$PID_FILE" ]]; then
    OLD_PID=$(cat "$PID_FILE")
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "⚠️  Worker already running with PID $OLD_PID"
        echo "Use: ./stop_worker.sh to stop it first"
        exit 1
    else
        echo "🧹 Cleaning up stale PID file"
        rm -f "$PID_FILE"
    fi
fi

echo "🔧 Activating virtual environment..."
source venv/bin/activate

# GPU profile: full (path A / NATS) or capped_5gb (path B benchmark)
if [[ -f ".env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi
export DOCLING_GPU_PROFILE="${DOCLING_GPU_PROFILE:-full}"

# Prefer systemd when installed (Restart=always)
if systemctl --user is-enabled smoldocling-docling-worker.service &>/dev/null; then
  echo "📦 Using systemd user unit (Restart=always)..."
  systemctl --user restart smoldocling-docling-worker.service
  systemctl --user status smoldocling-docling-worker.service --no-pager || true
  exit 0
fi

echo "📋 Starting worker in background (install systemd: ./scripts/install_systemd_worker.sh)..."
nohup python docling_worker.py >> "$LOG_FILE" 2>&1 &
WORKER_PID=$!

# Save PID for later management
echo $WORKER_PID > "$PID_FILE"

echo "✅ Worker started successfully!"
echo "📊 Process ID: $WORKER_PID"
echo "📝 Log file: $LOG_FILE"
echo "🔍 PID file: $PID_FILE"

# Wait a moment and check if it's still running
sleep 3
if kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "🎉 Worker is running and healthy!"
    echo "📡 Ready to process documents via NATS (DOCLING_GPU_PROFILE=$DOCLING_GPU_PROFILE)"
    if [[ -n "${LOKI_PUSH_URL:-}" ]]; then
      nohup python scripts/ship_worker_logs_loki.py >> loki-ship.log 2>&1 &
      echo "📊 Loki log shipper started (LOKI_PUSH_URL set)"
    fi
else
    echo "❌ Worker failed to start. Check logs:"
    tail -20 "$LOG_FILE"
    rm -f "$PID_FILE"
    exit 1
fi

echo ""
echo "📝 Management commands:"
echo "  ./stop_worker.sh          # Stop the worker"
echo "  ./status_worker.sh        # Check worker status"
echo "  tail -f $LOG_FILE         # View live logs"
echo "  ps aux | grep docling     # Find worker process"
echo ""
echo "🔧 To auto-restart on reboot, add to crontab:"
echo "  @reboot cd $CURRENT_DIR && ./start_worker.sh" 