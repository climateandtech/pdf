#!/usr/bin/env bash
# Create an isolated Docling benchmark venv on the GPU host without touching production worker.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BENCHMARK_VENV="${BENCHMARK_VENV:-$ROOT/venv-benchmark}"
REQ="${BENCHMARK_REQ:-$ROOT/requirements-benchmark.txt}"

echo "==> Isolated Docling benchmark environment"
echo "    repo: $ROOT"
echo "    venv: $BENCHMARK_VENV"
echo "    requirements: $REQ"
echo ""
echo "Production worker is NOT modified. Use this venv only for smoke/benchmark scripts."

PY="${BENCHMARK_PYTHON:-python3}"
if [[ ! -x "$BENCHMARK_VENV/bin/python" ]]; then
  echo "==> Creating venv"
  "$PY" -m venv "$BENCHMARK_VENV"
fi

"$BENCHMARK_VENV/bin/pip" install --upgrade pip
# Base deps from production requirements minus docling pin, then upgraded docling.
REQ_BASE="$ROOT/requirements.txt"
TMP_REQ="$(mktemp)"
grep -v -E '^docling|^# Pin: 2\.96' "$REQ_BASE" >"$TMP_REQ"
"$BENCHMARK_VENV/bin/pip" install -r "$TMP_REQ"
"$BENCHMARK_VENV/bin/pip" install -r "$REQ"
rm -f "$TMP_REQ"

echo ""
echo "==> Verify imports"
"$BENCHMARK_VENV/bin/python" - <<'PY'
import docling
from docling.document_converter import DocumentConverter

print("docling", getattr(docling, "__version__", "unknown"))
print("DocumentConverter", DocumentConverter)
try:
    from docling.chunking import HybridChunker, HierarchicalChunker  # noqa: F401

    print("native chunkers: ok")
except ImportError as exc:
    print("native chunkers: missing", exc)
PY

echo ""
echo "Next:"
echo "  DOCLING_GPU_PROFILE=capped_5gb $BENCHMARK_VENV/bin/python scripts/docling_capability_smoke.py --pdf /path/to/sample.pdf"
echo "  $BENCHMARK_VENV/bin/python scripts/parser_benchmark.py --pdf /path/to/sample.pdf --mode fast_text"
