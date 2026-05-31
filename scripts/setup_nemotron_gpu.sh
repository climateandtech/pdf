#!/usr/bin/env bash
# Install Nemotron OCR v2 in an isolated Python 3.12 GPU venv (production Docling worker untouched).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NEMOTRON_VENV="${NEMOTRON_VENV:-$ROOT/venv-nemotron}"
NEMOTRON_REPO="${NEMOTRON_REPO:-$ROOT/vendor/nemotron-ocr-v2}"
TORCH_INDEX="${NEMOTRON_TORCH_INDEX:-https://download.pytorch.org/whl/cu128}"

echo "==> Nemotron OCR v2 GPU setup"
echo "    repo: $ROOT"
echo "    venv: $NEMOTRON_VENV"
echo "    hf checkout: $NEMOTRON_REPO"

if ! command -v nvcc >/dev/null 2>&1; then
  echo "WARNING: nvcc not on PATH — Nemotron C++ CUDA extension build may fail." >&2
  echo "Load your CUDA module or export CUDA_HOME before re-running." >&2
fi

PY="${NEMOTRON_PYTHON:-python3.12}"
if ! command -v "$PY" >/dev/null 2>&1; then
  echo "ERROR: $PY not found. Nemotron OCR requires Python 3.12." >&2
  exit 1
fi

if [[ ! -x "$NEMOTRON_VENV/bin/python" ]]; then
  echo "==> Creating Python 3.12 venv"
  "$PY" -m venv "$NEMOTRON_VENV"
fi

"$NEMOTRON_VENV/bin/pip" install --upgrade pip
"$NEMOTRON_VENV/bin/pip" install torch torchvision --index-url "$TORCH_INDEX"
"$NEMOTRON_VENV/bin/pip" install pymupdf pillow

if [[ ! -d "$NEMOTRON_REPO/.git" ]]; then
  mkdir -p "$(dirname "$NEMOTRON_REPO")"
  git clone https://huggingface.co/nvidia/nemotron-ocr-v2 "$NEMOTRON_REPO"
fi

echo "==> Installing nemotron-ocr package (CUDA extension build)"
export TORCH_CUDA_ARCH_LIST="${TORCH_CUDA_ARCH_LIST:-8.9}"
(
  cd "$NEMOTRON_REPO"
  "$NEMOTRON_VENV/bin/pip" install --no-build-isolation -v .
)

echo ""
echo "==> Verify Nemotron import"
"$NEMOTRON_VENV/bin/python" - <<'PY'
from nemotron_ocr.inference.pipeline_v2 import NemotronOCRV2

print("NemotronOCRV2:", NemotronOCRV2)
PY

echo ""
echo "Next:"
echo "  $NEMOTRON_VENV/bin/python scripts/nemotron_smoke_test.py --pdf /path/to/page.pdf"
echo "  $NEMOTRON_VENV/bin/python scripts/nemotron_enrich_pdf.py --pdf /path/to/report.pdf --pages 0,1"
