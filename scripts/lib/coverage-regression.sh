#!/usr/bin/env bash
# Coverage regression — vendored from bot/scripts/lib (scripts/sync-coverage-gate.sh)
set -euo pipefail

QG_COV_MONOREPO_PREFIX="pdf/"
QG_COV_STANDALONE_MARKER="parse_modes.py"

_lib="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=coverage-regression-core.sh
source "$_lib/coverage-regression-core.sh"
