#!/usr/bin/env bash
set -euo pipefail
if systemctl --user is-enabled smoldocling-docling-worker-test.service &>/dev/null; then
  systemctl --user stop smoldocling-docling-worker-test.service
  echo "smoldocling-docling-worker-test stopped"
else
  echo "Test worker unit not installed"
fi
