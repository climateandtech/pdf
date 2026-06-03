#!/usr/bin/env bash
# Install user systemd unit for Docling test worker (ct-nats-test / pdf-test clone).
set -euo pipefail
cd "$(dirname "$0")/.."

USER_SYSTEMD="$HOME/.config/systemd/user"
mkdir -p "$USER_SYSTEMD"

src="$PWD/infrastructure/systemd/smoldocling-docling-worker-test.service"
name="$(basename "$src")"
cp "$src" "$USER_SYSTEMD/$name"
sed -i "s|/home/smoldocling|$HOME|g" "$USER_SYSTEMD/$name"
systemctl --user enable "$name"
systemctl --user daemon-reload
echo "Enabled $name (WorkingDirectory=$HOME/apps/pdf-test)"
echo "  systemctl --user restart smoldocling-docling-worker-test"
echo "  journalctl --user -u smoldocling-docling-worker-test -f"
