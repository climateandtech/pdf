#!/bin/bash

# Start distributed Docling services
echo "🚀 Starting Distributed Docling Services"
echo "========================================"

# Check if NATS server is already running
if ! pgrep -x "nats-server" > /dev/null; then
    echo "📡 Starting NATS server with JetStream..."
    nats-server -js -p 4222 --addr 0.0.0.0 &
    NATS_PID=$!
    echo "NATS server started with PID: $NATS_PID"
    
    # Wait for NATS to be ready
    sleep 2
else
    echo "📡 NATS server already running"
fi

# Activate virtual environment
if [ -d "venv" ]; then
    echo "🐍 Activating virtual environment..."
    source venv/bin/activate
else
    echo "❌ Virtual environment not found. Please run: python -m venv venv && pip install -r requirements.txt"
    exit 1
fi

# Set up NATS streams
echo "🔧 Setting up NATS streams..."
python setup_nats_streams.py

# Start the docling worker
echo "⚡ Starting Docling worker..."
python docling_worker.py &
WORKER_PID=$!

echo ""
echo "✅ Services started successfully!"
echo "📡 NATS server: localhost:4222"
echo "⚡ Docling worker: PID $WORKER_PID"
echo ""
echo "To stop services: kill $NATS_PID $WORKER_PID"
echo "Or use: pkill -f 'nats-server|docling_worker'"

# Keep script running
wait 