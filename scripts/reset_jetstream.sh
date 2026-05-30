#!/usr/bin/env bash
# Verify or reset JetStream on prod ct-nats (run on GPU after git pull of pdf repo).
#
#   cd ~/apps/pdf && source venv/bin/activate
#   ./scripts/reset_jetstream.sh --verify-only
#   ./scripts/reset_jetstream.sh --reset --yes
#
set -euo pipefail
cd "$(dirname "$0")/.."

set -a
[ -f .env ] && source .env
set +a

export NATS_URL="${NATS_URL:-nats://89.167.15.10:4222}"

PYTHON="${PYTHON:-}"
if [ -z "$PYTHON" ]; then
  if [ -x venv/bin/python ]; then
    PYTHON=venv/bin/python
  else
    PYTHON=python3
  fi
fi

if ! "$PYTHON" -c "import nats, yaml" 2>/dev/null; then
  echo "Installing nats-py, pyyaml..."
  "$PYTHON" -m pip install -q nats-py pyyaml python-dotenv
fi

ARGS=("$@")
if [ ${#ARGS[@]} -eq 0 ]; then
  ARGS=(--verify-only)
fi

echo "==> NATS_URL=$NATS_URL"
"$PYTHON" scripts/bootstrap_streams.py "${ARGS[@]}"

echo ""
echo "After --reset, restart workers: ./stop_worker.sh && ./start_worker.sh (and kg_gliner if used)"
