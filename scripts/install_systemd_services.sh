#!/usr/bin/env bash
# Install user systemd units for Docling + GLiNER workers (run as smoldocling on GPU).
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

install_unit "$PWD/infrastructure/systemd/smoldocling-docling-worker.service"
install_unit "$PWD/infrastructure/systemd/smoldocling-docling-chunk-worker.service"
install_unit "$PWD/infrastructure/systemd/smoldocling-kg-gliner-worker.service"

# Retire legacy unit name from deploy_worker.sh if present.
if [[ -f "$USER_SYSTEMD/pdf-docling-worker.service" ]]; then
  systemctl --user disable pdf-docling-worker.service 2>/dev/null || true
  rm -f "$USER_SYSTEMD/pdf-docling-worker.service"
  echo "Removed legacy pdf-docling-worker.service"
fi

systemctl --user daemon-reload
echo ""
echo "Installed smoldocling-docling-worker + docling-chunk-worker + kg-gliner-worker"
echo "  systemctl --user start smoldocling-docling-worker smoldocling-docling-chunk-worker smoldocling-kg-gliner-worker"
echo "  systemctl --user status smoldocling-docling-worker smoldocling-docling-chunk-worker smoldocling-kg-gliner-worker"
echo ""
echo "For reboot persistence (run once as root): loginctl enable-linger $(whoami)"
