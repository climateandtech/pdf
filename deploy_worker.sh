#!/bin/bash

# Deploy PDF Docling Worker as a User Service (no sudo required)
# pnpm-style deployment for Python services

set -e

echo "🚀 Deploying PDF Docling Worker (User Service)"
echo "=============================================="

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

# Create user systemd directory
mkdir -p ~/.config/systemd/user

echo "📋 Creating user systemd service..."

# Create user systemd service file (no sudo needed)
cat > ~/.config/systemd/user/${SERVICE_NAME}.service <<EOF
[Unit]
Description=PDF Docling Worker - Distributed Processing Service
After=network.target
Wants=network.target

[Service]
Type=simple
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

# Environment file
EnvironmentFile=$CURRENT_DIR/.env

[Install]
WantedBy=default.target
EOF

echo "🔄 Reloading user systemd..."
systemctl --user daemon-reload

echo "✅ Enabling service to start on login..."
systemctl --user enable $SERVICE_NAME

echo "🚀 Starting service..."
systemctl --user start $SERVICE_NAME

# Enable lingering so service starts even when not logged in
echo "🔧 Enabling lingering (service runs even when logged out)..."
sudo loginctl enable-linger $CURRENT_USER 2>/dev/null || echo "⚠️  Could not enable lingering (might need sudo loginctl enable-linger $CURRENT_USER)"

# Wait a moment for startup
sleep 3

echo "📊 Service status:"
systemctl --user status $SERVICE_NAME --no-pager -l

echo ""
echo "✅ Deployment complete!"
echo ""
echo "📝 Service management commands:"
echo "  systemctl --user status $SERVICE_NAME      # Check status"
echo "  systemctl --user restart $SERVICE_NAME     # Restart service"
echo "  systemctl --user stop $SERVICE_NAME        # Stop service"
echo "  systemctl --user start $SERVICE_NAME       # Start service"
echo "  journalctl --user -u $SERVICE_NAME -f      # View logs (live)"
echo "  journalctl --user -u $SERVICE_NAME --since '1 hour ago'  # Recent logs"
echo ""

# Test if service is running
if systemctl --user is-active --quiet $SERVICE_NAME; then
    echo "🎉 Service is running!"
    echo "📡 Ready to process documents via NATS"
else
    echo "⚠️  Service may have issues. Check logs:"
    echo "journalctl --user -u $SERVICE_NAME -n 50"
fi

echo ""
echo "🔧 Next steps:"
echo "1. Test from your laptop with the DocumentService client"
echo "2. Monitor logs: journalctl --user -u $SERVICE_NAME -f"
echo "3. Service will auto-start on login (lingering enabled)" 