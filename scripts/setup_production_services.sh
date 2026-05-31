#!/usr/bin/env bash
# Migrate GPU workers from nohup/tmux to user systemd units.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Production GPU worker setup (systemd user units)"
echo "    repo: $PWD"

if [[ ! -f ".env" ]]; then
  echo "Error: .env missing. Run gpu-sync-nats-env.sh from coolify-provisioning." >&2
  exit 1
fi

if [[ ! -x venv/bin/python ]]; then
  echo "Error: venv missing. Run: python3 -m venv venv && pip install -r requirements.txt -r requirements-gliner.txt" >&2
  exit 1
fi

./scripts/install_systemd_services.sh

echo ""
echo "==> Stop legacy nohup workers"
./stop_worker.sh 2>/dev/null || true
./stop_kg_gliner.sh 2>/dev/null || true
pkill -f '[p]ython docling_worker.py' 2>/dev/null || true
pkill -f '[p]ython /home/smoldocling/apps/pdf/kg_gliner_worker.py' 2>/dev/null || true
pkill -f '[p]ython kg_gliner_worker.py' 2>/dev/null || true
rm -f worker.pid kg_gliner.pid

echo ""
echo "==> Start systemd services"
systemctl --user restart smoldocling-docling-worker.service
systemctl --user restart smoldocling-kg-gliner-worker.service
sleep 3

systemctl --user --no-pager status smoldocling-docling-worker.service || true
echo ""
systemctl --user --no-pager status smoldocling-kg-gliner-worker.service || true

echo ""
echo "==> Verify imports"
venv/bin/python scripts/verify_torch_import.py
venv/bin/python -c "import docling; print('docling', getattr(docling, '__version__', 'unknown'))"
