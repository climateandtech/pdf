#!/usr/bin/env bash
# Back-compat wrapper — installs Docling + GLiNER systemd user units.
set -euo pipefail
exec "$(dirname "$0")/install_systemd_services.sh"
