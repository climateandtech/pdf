#!/usr/bin/env bash
# Isolated Docling 2.42.2 benchmark venv for apples-to-apples version comparisons.
# Does NOT touch production venv/ or systemd worker.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LEGACY_VENV="${LEGACY_VENV:-$ROOT/venv-legacy}"
LEGACY_DOCLING="${LEGACY_DOCLING:-2.42.2}"

echo "==> Legacy Docling benchmark environment (docling==${LEGACY_DOCLING})"
echo "    repo: $ROOT"
echo "    venv: $LEGACY_VENV"
echo ""
echo "Production worker is NOT modified."

PY="${BENCHMARK_PYTHON:-python3}"
if [[ ! -x "$LEGACY_VENV/bin/python" ]]; then
  echo "==> Creating venv"
  "$PY" -m venv "$LEGACY_VENV"
fi

"$LEGACY_VENV/bin/pip" install --upgrade pip
REQ_BASE="$ROOT/requirements.txt"
TMP_REQ="$(mktemp)"
grep -v -E '^docling|^# Pin:' "$REQ_BASE" >"$TMP_REQ"
"$LEGACY_VENV/bin/pip" install -r "$TMP_REQ"
"$LEGACY_VENV/bin/pip" install "docling==${LEGACY_DOCLING}"
rm -f "$TMP_REQ"

echo ""
echo "==> Verify imports"
"$LEGACY_VENV/bin/python" scripts/verify_torch_import.py

echo ""
echo "Next:"
echo "  DOCLING_GPU_PROFILE=20gb_nats $LEGACY_VENV/bin/python scripts/parser_benchmark.py \\"
echo "    --pdf benchmarks/fixtures/opus_global_esg_2025_en.pdf --modes baseline \\"
echo "    --run-id opus-baseline-docling-${LEGACY_DOCLING} --gpu-profile 20gb_nats"
