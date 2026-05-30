#!/usr/bin/env bash
# Install user systemd unit for auto-restart (run as smoldocling on GPU host).
set -euo pipefail
cd "$(dirname "$0")/.."
UNIT_SRC="$PWD/infrastructure/systemd/smoldocling-docling-worker.service"
UNIT_DST="$HOME/.config/systemd/user/smoldocling-docling-worker.service"

mkdir -p "$HOME/.config/systemd/user"
cp "$UNIT_SRC" "$UNIT_DST"
# Substitute actual home if not /home/smoldocling
sed -i "s|/home/smoldocling|$HOME|g" "$UNIT_DST"

systemctl --user daemon-reload
systemctl --user enable smoldocling-docling-worker.service
echo "Enabled smoldocling-docling-worker (Restart=always)"
echo "  systemctl --user start smoldocling-docling-worker"
echo "  systemctl --user status smoldocling-docling-worker"
echo "  journalctl --user -u smoldocling-docling-worker -f"
echo ""
echo "For reboot persistence: loginctl enable-linger $(whoami)"
