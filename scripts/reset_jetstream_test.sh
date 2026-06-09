#!/usr/bin/env bash
# Verify or update JetStream on ct-nats-test (:4223). Run on GPU pdf-test or laptop with .env.
#
#   cd ~/apps/pdf-test && source .env && ./scripts/reset_jetstream_test.sh --verify-only
#   ./scripts/reset_jetstream_test.sh
#
set -euo pipefail
cd "$(dirname "$0")/.."

set -a
[ -f .env ] && source .env
set +a

export NATS_URL="${NATS_URL:-nats://89.167.15.10:4223}"

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  if [ -x venv/bin/python ]; then
    PYTHON=venv/bin/python
  else
    PYTHON=python3
  fi
fi

ARGS=("$@")
if [ ${#ARGS[@]} -eq 0 ]; then
  ARGS=(--verify-only)
fi

echo "==> NATS_URL=$NATS_URL"
"$PYTHON" scripts/bootstrap_streams.py "${ARGS[@]}"
