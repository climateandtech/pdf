#!/usr/bin/env python3
"""Smoke-test Docling capabilities in an isolated benchmark venv."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parse_modes import PARSE_MODES, get_parse_mode
from worker_runtime import bootstrap_gpu, cleanup_gpu_memory


def _probe_native_chunkers() -> dict[str, bool]:
    probes = {"hierarchical": False, "hybrid": False}
    try:
        from docling.chunking import HierarchicalChunker  # noqa: F401

        probes["hierarchical"] = True
    except ImportError:
        pass
    try:
        from docling.chunking import HybridChunker  # noqa: F401

        probes["hybrid"] = True
    except ImportError:
        pass
    return probes


def _convert(pdf_path: Path, mode: str) -> dict:
    from docling_worker import DoclingWorker

    worker = DoclingWorker()
    options = get_parse_mode(mode)
    converter = worker._create_document_converter(options)
    started = time.perf_counter()
    result = converter.convert(str(pdf_path))
    elapsed_s = time.perf_counter() - started
    document = result.document
    markdown = document.export_to_markdown()
    structured = document.export_to_dict()
    page_count = len(getattr(document, "pages", []) or structured.get("pages", {}) or {})
    if not page_count and structured.get("pages"):
        page_count = len(structured["pages"])
    return {
        "mode": mode,
        "elapsed_s": round(elapsed_s, 3),
        "markdown_chars": len(markdown),
        "json_keys": sorted(structured.keys())[:20],
        "page_count": page_count,
        "pages_per_min": round((page_count / elapsed_s) * 60, 2) if elapsed_s > 0 else 0,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Docling capability smoke test")
    parser.add_argument("--pdf", type=Path, required=True, help="Sample PDF path")
    parser.add_argument(
        "--mode",
        default="fast_text",
        choices=sorted(PARSE_MODES),
        help="Parse mode preset",
    )
    parser.add_argument("--gpu-profile", default=None, help="DOCLING_GPU_PROFILE override")
    parser.add_argument("--output", type=Path, help="Optional JSON output path")
    args = parser.parse_args()

    if not args.pdf.is_file():
        print(f"PDF not found: {args.pdf}", file=sys.stderr)
        return 2

    import docling

    report = {
        "docling_version": getattr(docling, "__version__", "unknown"),
        "gpu_profile": bootstrap_gpu(args.gpu_profile),
        "native_chunkers": _probe_native_chunkers(),
        "pdf": str(args.pdf.resolve()),
        "modes_tested": [],
    }

    try:
        report["modes_tested"].append(_convert(args.pdf, args.mode))
    finally:
        cleanup_gpu_memory(force=True)

    print(json.dumps(report, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
