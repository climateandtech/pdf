#!/usr/bin/env bash
# On-GPU E2E: ensure DOCUMENTS stream + run parse/chunk smokes (pdf-test .env).
#
#   cd ~/apps/pdf-test && ./scripts/run_test_nats_e2e.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "Missing .env (NATS_URL, S3_*)" >&2
  exit 1
fi
set -a
source .env
set +a

if [[ ! -x venv/bin/python ]]; then
  echo "Missing venv" >&2
  exit 1
fi

export XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-/run/user/$(id -u)}"

venv/bin/python scripts/ensure_documents_stream.py
venv/bin/python scripts/gpu_nats_chunk_e2e_smoke.py tests/fixtures/minimal.pdf
venv/bin/python scripts/gpu_nats_chunk_e2e_smoke.py tests/fixtures/minimal.pdf --hierarchical
echo "OK"
