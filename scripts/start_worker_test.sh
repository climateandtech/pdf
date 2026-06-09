#!/usr/bin/env bash
# Start Docling parse + chunk test workers (systemd).
set -euo pipefail
cd "$(dirname "$0")/.."
export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

if [[ ! -f docling_worker.py ]]; then
  echo "Run from pdf-test clone (~/apps/pdf-test)" >&2
  exit 1
fi
if [[ ! -f .env ]]; then
  echo "Missing .env — copy NATS/S3 test credentials to pdf-test/.env" >&2
  exit 1
fi

if systemctl --user is-enabled smoldocling-docling-worker-test.service &>/dev/null; then
  systemctl --user restart smoldocling-docling-worker-test.service \
    smoldocling-docling-chunk-worker-test.service
  systemctl --user status smoldocling-docling-worker-test.service \
    smoldocling-docling-chunk-worker-test.service --no-pager || true
  exit 0
fi
echo "Install units first: ./scripts/install_systemd_test_workers.sh" >&2
exit 1
