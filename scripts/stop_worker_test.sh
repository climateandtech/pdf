#!/usr/bin/env bash
# Stop Docling parse + chunk test workers (systemd).
set -euo pipefail
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

stopped=0
for unit in smoldocling-docling-worker-test.service smoldocling-docling-chunk-worker-test.service; do
  if systemctl --user is-enabled "${unit}" &>/dev/null; then
    systemctl --user stop "${unit}"
    echo "${unit} stopped"
    stopped=1
  fi
done
if [ "$stopped" -eq 0 ]; then
  echo "Test worker units not installed"
fi
