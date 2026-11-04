#!/usr/bin/env bash
# Create the Timestral venv used by smoldocling-forecast-worker.
# Does not touch the Docling venv.
set -euo pipefail
cd "$(dirname "$0")/.."

python3 -m venv venv-forecast
./venv-forecast/bin/pip install -U pip
./venv-forecast/bin/pip install -r requirements-forecast.txt

echo "venv-forecast ready."
echo "Enable the unit with ./scripts/install_systemd_services.sh"
echo "Then: systemctl --user start smoldocling-forecast-worker"
