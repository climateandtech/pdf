#!/usr/bin/env python3
"""Run Nemotron OCR enrichment on selected PDF pages and emit markdown."""

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


def _parse_pages(raw: str, page_count: int) -> list[int]:
    if raw.strip().lower() == "all":
        return list(range(page_count))
    indices: list[int] = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value < 0 or value >= page_count:
            raise ValueError(f"Invalid page index: {value}")
        indices.append(value)
    return indices


def _page_count(pdf_path: Path) -> int:
    import fitz

    doc = fitz.open(pdf_path)
    try:
        return doc.page_count
    finally:
        doc.close()


def main() -> int:
    parser = argparse.ArgumentParser(description="Nemotron PDF page enrichment")
    parser.add_argument("--pdf", type=Path, required=True)
    parser.add_argument("--pages", default="0", help="Comma-separated page indices or 'all'")
    parser.add_argument("--base-markdown", type=Path, help="Optional existing markdown to append to")
    parser.add_argument("--output-dir", type=Path, default=Path("benchmarks/nemotron/runs"))
    parser.add_argument("--run-id", default=None)
    args = parser.parse_args()

    if not args.pdf.is_file():
        print(f"PDF not found: {args.pdf}", file=sys.stderr)
        return 2

    probe = probe_nemotron_gpu()
    if not probe.get("available"):
        print(json.dumps(probe, indent=2), file=sys.stderr)
        return 1

    page_count = _page_count(args.pdf)
    page_indices = _parse_pages(args.pages, page_count)
    base_markdown = args.base_markdown.read_text(encoding="utf-8") if args.base_markdown else ""

    service = NemotronOcrService(NemotronConfig.from_env())
    started = time.perf_counter()
    page_results = service.ocr_pdf_pages(args.pdf, page_indices)
    enriched_markdown = service.merge_page_text_into_markdown(base_markdown, page_results)

    run_id = args.run_id or time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    out_dir = args.output_dir / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "enriched.md").write_text(enriched_markdown + "\n", encoding="utf-8")
    metrics = {
        "run_id": run_id,
        "backend": "nemotron",
        "pdf": str(args.pdf.resolve()),
        "pages": page_indices,
        "elapsed_s": round(time.perf_counter() - started, 3),
        "markdown_chars": len(enriched_markdown),
        "probe": probe,
    }
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(metrics, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
