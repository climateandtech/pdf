#!/usr/bin/env bash
# Create the Crivo venv used by smoldocling-route-decision-worker.
# Does not touch the Docling venv.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m venv venv-route-decision
./venv-route-decision/bin/pip install -U pip
./venv-route-decision/bin/pip install -r requirements-route-decision.txt

echo "venv-route-decision ready."
echo "Enable the unit with ./scripts/install_systemd_services.sh"
echo "Then: systemctl --user start smoldocling-route-decision-worker"
