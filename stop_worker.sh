#!/bin/bash

# Stop PDF Docling Worker

PID_FILE="$(pwd)/worker.pid"

echo "🛑 Stopping PDF Docling Worker"
echo "==============================="

if [[ ! -f "$PID_FILE" ]]; then
    echo "❌ No PID file found. Worker may not be running."
    echo "Check with: ps aux | grep docling"
    exit 1
fi

WORKER_PID=$(cat "$PID_FILE")

if kill -0 "$WORKER_PID" 2>/dev/null; then
    echo "📋 Stopping worker with PID $WORKER_PID..."
    kill "$WORKER_PID"
    
    # Wait for graceful shutdown
    sleep 2
    
    # Force kill if still running
    if kill -0 "$WORKER_PID" 2>/dev/null; then
        echo "🔨 Force killing worker..."
        kill -9 "$WORKER_PID"
        sleep 1
    fi
    
    # Verify it's stopped
    if kill -0 "$WORKER_PID" 2>/dev/null; then
        echo "❌ Failed to stop worker"
        exit 1
    else
        echo "✅ Worker stopped successfully"
        rm -f "$PID_FILE"
    fi
else
    echo "⚠️  Worker with PID $WORKER_PID is not running"
    echo "🧹 Cleaning up stale PID file"
    rm -f "$PID_FILE"
fi 