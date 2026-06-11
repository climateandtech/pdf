#!/usr/bin/env bash
# Recover GPU systemd workers without changing git state (safe after partial deploy).
#
#   ./scripts/gpu_recover_workers.sh          # local on GPU as smoldocling
#   ./scripts/laptop_gpu_recover_workers.sh   # from laptop via SSH
#
# Does NOT: git fetch, merge, reset, pip install, or scp.
set -euo pipefail
cd "$(dirname "$0")/.."

source venv/bin/activate

if python -c "from worker_runtime import verify_cudnn_conv2d" 2>/dev/null; then
  if [ -f scripts/cudnn_probe.py ]; then
    python scripts/cudnn_probe.py
  fi
fi
python scripts/verify_torch_import.py
./scripts/install_systemd_services.sh
python scripts/ensure_documents_stream.py 2>/dev/null || true

systemctl --user restart \
  smoldocling-docling-worker.service \
  smoldocling-docling-chunk-worker.service \
  smoldocling-kg-gliner-worker.service

systemctl --user is-active \
  smoldocling-docling-worker.service \
  smoldocling-docling-chunk-worker.service \
  smoldocling-kg-gliner-worker.service

echo "GPU workers recovered (git tree untouched)."
