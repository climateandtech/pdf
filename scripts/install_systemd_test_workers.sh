#!/usr/bin/env bash
# Install user systemd units for Docling parse + chunk test workers (ct-nats-test).
set -euo pipefail
cd "$(dirname "$0")/.."

USER_SYSTEMD="$HOME/.config/systemd/user"
mkdir -p "$USER_SYSTEMD"

install_unit() {
  local src="$1"
  local name
  name="$(basename "$src")"
  cp "$src" "$USER_SYSTEMD/$name"
  sed -i "s|/home/smoldocling|$HOME|g" "$USER_SYSTEMD/$name"
  systemctl --user enable "$name"
  echo "Enabled $name"
}

install_unit "$PWD/infrastructure/systemd/smoldocling-docling-worker-test.service"
install_unit "$PWD/infrastructure/systemd/smoldocling-docling-chunk-worker-test.service"

systemctl --user daemon-reload
echo ""
echo "Test workers (WorkingDirectory=$HOME/apps/pdf-test):"
echo "  systemctl --user restart smoldocling-docling-worker-test smoldocling-docling-chunk-worker-test"
