#!/usr/bin/env bash
# Run NATS parse+chunk E2E smoke on GPU pdf-test (ct-nats-test :4223).
#
#   ./scripts/laptop_deploy_test.sh --branch main
#   ./scripts/laptop_test_nats_e2e.sh
#   ./scripts/laptop_test_nats_e2e.sh --hierarchical-only
#
set -euo pipefail
cd "$(dirname "$0")/.."
# shellcheck source=scripts/lib/gpu_ssh.sh
source "$(dirname "$0")/lib/gpu_ssh.sh"

HIERARCHICAL_ONLY=false
PDF="${PDF_FIXTURE:-tests/fixtures/minimal.pdf}"

while [ $# -gt 0 ]; do
  case "$1" in
    --hierarchical-only) HIERARCHICAL_ONLY=true; shift ;;
    --pdf) PDF="${2:?}"; shift 2 ;;
    -h|--help)
      sed -n '2,7p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "Unknown: $1" >&2; exit 1 ;;
  esac
done

run_smoke() {
  local extra="$1"
  gpu_smoldocling "
    set -e
    cd ${GPU_TEST_DIR}
    source venv/bin/activate
    source .env
    python scripts/ensure_documents_stream.py
    python scripts/gpu_nats_chunk_e2e_smoke.py ${PDF} ${extra}
  "
}

if [ "$HIERARCHICAL_ONLY" = true ]; then
  echo "==> hierarchical chunk E2E"
  run_smoke "--hierarchical"
else
  echo "==> direct parse E2E"
  run_smoke ""
  echo "==> hierarchical chunk E2E"
  run_smoke "--hierarchical"
fi
echo "OK"
