#!/usr/bin/env bash
# Install Nemotron OCR v2 in an isolated Python 3.12 GPU venv (production Docling worker untouched).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NEMOTRON_VENV="${NEMOTRON_VENV:-$ROOT/venv-nemotron}"
NEMOTRON_REPO="${NEMOTRON_REPO:-$ROOT/vendor/nemotron-ocr-v2}"
NEMOTRON_PKG="${NEMOTRON_PKG:-$NEMOTRON_REPO/nemotron-ocr}"
TORCH_INDEX="${NEMOTRON_TORCH_INDEX:-https://download.pytorch.org/whl/cu128}"

echo "==> Nemotron OCR v2 GPU setup"
echo "    repo: $ROOT"
echo "    venv: $NEMOTRON_VENV"
echo "    hf checkout: $NEMOTRON_REPO"
echo "    package dir: $NEMOTRON_PKG"

if ! command -v nvcc >/dev/null 2>&1; then
  echo "WARNING: nvcc not on PATH — Nemotron C++ CUDA extension build may fail." >&2
  echo "Load your CUDA module or export CUDA_HOME before re-running." >&2
fi

# Prefer system CUDA 12.6 toolkit over stale /usr/bin/nvcc (12.0) on Ubuntu 24.04.
for cuda_home in /usr/local/cuda-12.6 /usr/local/cuda-12 /usr/local/cuda; do
  if [[ -x "$cuda_home/bin/nvcc" ]]; then
    export CUDA_HOME="$cuda_home"
    export PATH="$CUDA_HOME/bin:$PATH"
    break
  fi
done
export CXX="${CXX:-g++-12}"
export CC="${CC:-gcc-12}"

PY="${NEMOTRON_PYTHON:-python3.12}"
if ! command -v "$PY" >/dev/null 2>&1; then
  echo "ERROR: $PY not found. Nemotron OCR requires Python 3.12." >&2
  exit 1
fi

if [[ ! -x "$NEMOTRON_VENV/bin/python" ]]; then
  echo "==> Creating Python 3.12 venv"
  "$PY" -m venv "$NEMOTRON_VENV"
fi

"$NEMOTRON_VENV/bin/pip" install --upgrade pip hatchling ninja
"$NEMOTRON_VENV/bin/pip" install torch torchvision --index-url "$TORCH_INDEX"
"$NEMOTRON_VENV/bin/pip" install pymupdf pillow

if [[ ! -d "$NEMOTRON_REPO/.git" ]]; then
  mkdir -p "$(dirname "$NEMOTRON_REPO")"
  git clone https://huggingface.co/nvidia/nemotron-ocr-v2 "$NEMOTRON_REPO"
fi

echo "==> Installing nemotron-ocr package (CUDA extension build)"
export TORCH_CUDA_ARCH_LIST="${TORCH_CUDA_ARCH_LIST:-8.9}"
if [[ ! -f "$NEMOTRON_PKG/pyproject.toml" ]]; then
  echo "ERROR: missing $NEMOTRON_PKG/pyproject.toml (clone nvidia/nemotron-ocr-v2 first)" >&2
  exit 1
fi
(
  cd "$NEMOTRON_PKG"
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
echo "  export NEMOTRON_MODEL_DIR=$NEMOTRON_REPO/v2_english   # optional; lang=en also works"
echo "  $NEMOTRON_VENV/bin/python scripts/nemotron_smoke_test.py --pdf tests/fixtures/minimal.pdf --pages 0"
