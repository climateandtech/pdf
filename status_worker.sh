#!/bin/bash

# Check PDF Docling Worker status (systemd user unit or legacy nohup PID file)

PID_FILE="$(pwd)/worker.pid"
LOG_FILE="$(pwd)/worker.log"

echo "📊 PDF Docling Worker Status"
echo "============================"

if systemctl --user is-enabled smoldocling-docling-worker.service &>/dev/null; then
  systemctl --user status smoldocling-docling-worker.service --no-pager || true
  if [[ -f "$LOG_FILE" ]]; then
    echo ""
    echo "📝 Recent worker.log (last 10 lines):"
    echo "────────────────────────────────────"
    tail -10 "$LOG_FILE"
  fi
  exit 0
fi

if [[ ! -f "$PID_FILE" ]]; then
  echo "❌ No PID file found - Worker not running"
  exit 1
fi

WORKER_PID=$(cat "$PID_FILE")

if kill -0 "$WORKER_PID" 2>/dev/null; then
  echo "✅ Worker is running"
  echo "📊 Process ID: $WORKER_PID"
  echo "⏰ Started: $(ps -o lstart= -p $WORKER_PID 2>/dev/null || echo 'Unknown')"
  echo "💾 Memory: $(ps -o rss= -p $WORKER_PID 2>/dev/null | awk '{print $1/1024 " MB"}' || echo 'Unknown')"
  echo "⚡ CPU: $(ps -o %cpu= -p $WORKER_PID 2>/dev/null || echo 'Unknown')%"

  if [[ -f "$LOG_FILE" ]]; then
    echo ""
    echo "📝 Recent log entries (last 10 lines):"
    echo "────────────────────────────────────"
    tail -10 "$LOG_FILE"
  fi

  echo ""
  echo "📋 Management commands:"
  echo "  ./stop_worker.sh          # Stop the worker"
  echo "  tail -f $LOG_FILE         # View live logs"
  echo "  ps -fp $WORKER_PID        # Detailed process info"
else
  echo "❌ Worker with PID $WORKER_PID is not running"
  echo "🧹 Cleaning up stale PID file"
  rm -f "$PID_FILE"
  exit 1
fi
