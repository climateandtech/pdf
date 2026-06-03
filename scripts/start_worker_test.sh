#!/usr/bin/env bash
# Start Docling worker on test NATS broker (systemd: smoldocling-docling-worker-test).
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ ! -f docling_worker.py ]]; then
  echo "Run from pdf-test clone (~/apps/pdf-test)" >&2
  exit 1
fi
if [[ ! -f .env ]]; then
  echo "Missing .env — run coolify-provisioning/gpu-sync-nats-test-env.sh from laptop" >&2
  exit 1
fi
if systemctl --user is-enabled smoldocling-docling-worker-test.service &>/dev/null; then
  systemctl --user restart smoldocling-docling-worker-test.service
  systemctl --user status smoldocling-docling-worker-test.service --no-pager || true
  exit 0
fi
echo "Install unit first: ./scripts/install_systemd_test_worker.sh" >&2
exit 1
