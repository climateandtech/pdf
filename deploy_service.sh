#!/bin/bash

# Deploy PDF Docling Worker as a System Service
# Similar to pnpm deploy but for Python services

set -e

echo "🚀 Deploying PDF Docling Worker Service"
echo "======================================"

# Get current directory and user
CURRENT_DIR=$(pwd)
CURRENT_USER=$(whoami)
SERVICE_NAME="pdf-docling-worker"
PYTHON_PATH="$CURRENT_DIR/venv/bin/python"
WORKER_SCRIPT="$CURRENT_DIR/docling_worker.py"

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

echo "📋 Creating systemd service..."

# Create systemd service file
sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=PDF Docling Worker - Distributed Processing Service
After=network.target
Wants=network.target

[Service]
Type=simple
User=$CURRENT_USER
Group=$CURRENT_USER
WorkingDirectory=$CURRENT_DIR
Environment=PATH=$CURRENT_DIR/venv/bin
ExecStart=$PYTHON_PATH $WORKER_SCRIPT
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=$SERVICE_NAME

# Resource limits
LimitNOFILE=65536
MemoryMax=8G

# Environment
EnvironmentFile=$CURRENT_DIR/.env

[Install]
WantedBy=multi-user.target
EOF

echo "🔄 Reloading systemd..."
sudo systemctl daemon-reload

echo "✅ Enabling service to start on boot..."
sudo systemctl enable $SERVICE_NAME

echo "🚀 Starting service..."
sudo systemctl start $SERVICE_NAME

# Wait a moment for startup
sleep 3

echo "📊 Service status:"
sudo systemctl status $SERVICE_NAME --no-pager -l

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📝 Service management commands:"
echo "  sudo systemctl status $SERVICE_NAME      # Check status"
echo "  sudo systemctl restart $SERVICE_NAME     # Restart service"
echo "  sudo systemctl stop $SERVICE_NAME        # Stop service"
echo "  sudo systemctl start $SERVICE_NAME       # Start service"
echo "  sudo journalctl -u $SERVICE_NAME -f      # View logs (live)"
echo "  sudo journalctl -u $SERVICE_NAME --since '1 hour ago'  # Recent logs"
echo ""

# Test if service is running
if systemctl is-active --quiet $SERVICE_NAME; then
    echo "🎉 Service is running!"
    echo "📡 Ready to process documents via NATS"
else
    echo "⚠️  Service may have issues. Check logs:"
    echo "sudo journalctl -u $SERVICE_NAME -n 50"
fi

echo ""
echo "🔧 Next steps:"
echo "1. Test from your laptop with the DocumentService client"
echo "2. Monitor logs: sudo journalctl -u $SERVICE_NAME -f"
echo "3. Scale by running this on more GPU servers" 