#!/usr/bin/env bash
# Install user systemd units for Docling test workers (ct-nats-test / pdf-test clone).
set -euo pipefail
exec "$(dirname "$0")/install_systemd_test_workers.sh"
