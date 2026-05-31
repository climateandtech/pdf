#!/usr/bin/env bash
# Install Docling-native Nemotron OCR into venv-benchmark (production venv untouched).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_VENV="${BENCHMARK_VENV:-$ROOT/venv-benchmark}"
TORCH_INDEX="${DOCLING_NEMOTRON_TORCH_INDEX:-https://download.pytorch.org/whl/cu130}"

if [[ ! -x "$BENCHMARK_VENV/bin/python" ]]; then
  echo "ERROR: $BENCHMARK_VENV missing — run scripts/setup_isolated_benchmark_env.sh first" >&2
  exit 1
fi

echo "==> Docling Nemotron OCR extra (venv-benchmark)"
echo "    venv: $BENCHMARK_VENV"
echo "    torch index: $TORCH_INDEX"

"$BENCHMARK_VENV/bin/pip" install --upgrade pip
"$BENCHMARK_VENV/bin/pip" install "docling[nemotron-ocr]" \
  --extra-index-url "$TORCH_INDEX"

if "$BENCHMARK_VENV/bin/docling-tools" models download nemotron_ocr 2>/dev/null; then
  echo "==> nemotron_ocr models downloaded via docling-tools"
else
  echo "WARN: docling-tools models download nemotron_ocr failed — models may fetch on first run" >&2
fi

echo ""
echo "==> Verify NemotronOcrOptions import"
"$BENCHMARK_VENV/bin/python" - <<'PY'
from docling.datamodel.pipeline_options import NemotronOcrOptions

print("NemotronOcrOptions:", NemotronOcrOptions)
PY

echo ""
echo "Next:"
echo "  DOCLING_GPU_PROFILE=capped_5gb $BENCHMARK_VENV/bin/python \\"
echo "    scripts/docling_capability_smoke.py --pdf tests/fixtures/minimal.pdf --mode nemotron_enrich"
