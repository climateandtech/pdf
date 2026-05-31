#!/usr/bin/env python3
"""GPU smoke test for Nemotron OCR v2."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from nemotron_service import NemotronConfig, NemotronOcrService, probe_nemotron_gpu


def _page_indices_arg(raw: str, page_count: int) -> list[int]:
    indices: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value < 0 or value >= page_count:
            raise ValueError(f"Page index out of range: {value}")
        indices.append(value)
    return indices


def _pdf_page_count(pdf_path: Path) -> int:
    import fitz

    doc = fitz.open(pdf_path)
    try:
        return doc.page_count
    finally:
        doc.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Nemotron OCR GPU smoke test")
    parser.add_argument("--pdf", type=Path, required=True)
    parser.add_argument("--pages", default="0", help="Comma-separated zero-based page indices")
    parser.add_argument("--output", type=Path, help="Optional JSON output")
    args = parser.parse_args()

    if not args.pdf.is_file():
        print(f"PDF not found: {args.pdf}", file=sys.stderr)
        return 2

    probe = probe_nemotron_gpu()
    if not probe.get("available"):
        print(json.dumps(probe, indent=2), file=sys.stderr)
        return 1

    page_count = _pdf_page_count(args.pdf)
    page_indices = _page_indices_arg(args.pages, page_count)
    service = NemotronOcrService(NemotronConfig.from_env())

    started = time.perf_counter()
    results = []
    for page_index in page_indices:
        page_started = time.perf_counter()
        result = service.ocr_pdf_page(args.pdf, page_index)
        results.append(
            {
                "page_index": result.page_index,
                "chars": len(result.text),
                "preview": result.text[:240],
                "elapsed_s": round(time.perf_counter() - page_started, 3),
            }
        )

    report = {
        "probe": probe,
        "pdf": str(args.pdf.resolve()),
        "page_count": page_count,
        "pages_tested": page_indices,
        "elapsed_s": round(time.perf_counter() - started, 3),
        "results": results,
    }
    print(json.dumps(report, indent=2))
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
