#!/usr/bin/env python3
"""Simulate multistage indexing: pass-1 fast_text + selective page-range passes."""

from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

PLATFORM_BACKEND = ROOT.parent / "ct-platform" / "backend"
if str(PLATFORM_BACKEND) not in sys.path:
    sys.path.insert(0, str(PLATFORM_BACKEND))

from app.enrichment_plan import build_page_enrichment_plan

from parse_modes import get_parse_mode
from parser_benchmark import (
    SCHEMA_VERSION,
    _page_count,
    _picture_description_stats,
    benchmark_pdf,
)
from parser_registry import collect_environment, sha256_file, table_stats
from worker_runtime import bootstrap_gpu, cleanup_gpu_memory

# Pass-2 presets (match enrichment_plan routing; not all modes exist in PARSE_MODES).
TABLE_PASS_MODE = "fast_text_tables"
VLM_PASS_MODE = "rich"
OCR_PASS_MODE = "baseline"


def pages_to_contiguous_ranges(pages: list[int]) -> list[tuple[int, int]]:
    if not pages:
        return []
    sorted_pages = sorted(set(int(p) for p in pages))
    ranges: list[tuple[int, int]] = []
    start = prev = sorted_pages[0]
    for page in sorted_pages[1:]:
        if page == prev + 1:
            prev = page
            continue
        ranges.append((start, prev))
        start = prev = page
    ranges.append((start, prev))
    return ranges


def benchmark_pdf_with_options(
    pdf_path: Path,
    *,
    mode: str,
    options_override: dict[str, Any] | None = None,
    page_range: tuple[int, int] | None = None,
) -> dict[str, Any]:
    """Like parser_benchmark.benchmark_pdf but with optional page_range and option merge."""
    from docling_worker import DoclingWorker

    worker = DoclingWorker()
    options = get_parse_mode(mode)
    if options_override:
        options = {**options, **options_override}
    if page_range is not None:
        options = {**options, "page_range": [page_range[0], page_range[1]]}

    converter = worker._create_document_converter(options)
    convert_kwargs: dict[str, Any] = {}
    pr = options.get("page_range")
    if isinstance(pr, (list, tuple)) and len(pr) == 2:
        convert_kwargs["page_range"] = (int(pr[0]), int(pr[1]))

    started = time.perf_counter()
    result = converter.convert(str(pdf_path), **convert_kwargs)
    elapsed_s = time.perf_counter() - started

    document = result.document
    markdown = document.export_to_markdown()
    structured = document.export_to_dict()
    pages = _page_count(document, structured)
    page_span = (page_range[1] - page_range[0] + 1) if page_range else pages
    return {
        "mode": mode,
        "page_range": list(page_range) if page_range else None,
        "elapsed_s": round(elapsed_s, 3),
        "page_count_in_result": pages,
        "page_span": page_span,
        "markdown_chars": len(markdown),
        "picture_stats": _picture_description_stats(structured),
        "table_stats": table_stats(structured),
        "options": options,
    }


def run_selective_passes(
    pdf_path: Path,
    *,
    pages: list[int],
    mode: str,
    label: str,
) -> dict[str, Any]:
    ranges = pages_to_contiguous_ranges(pages)
    segments: list[dict[str, Any]] = []
    total_elapsed = 0.0
    total_span = 0
    for page_range in ranges:
        row = benchmark_pdf_with_options(pdf_path, mode=mode, page_range=page_range)
        row["label"] = label
        segments.append(row)
        total_elapsed += row["elapsed_s"]
        total_span += row["page_span"]
        cleanup_gpu_memory(force=True)
    return {
        "label": label,
        "mode": mode,
        "pages_requested": sorted(set(int(p) for p in pages)),
        "contiguous_ranges": [list(r) for r in ranges],
        "range_count": len(ranges),
        "page_span_total": total_span,
        "elapsed_s": round(total_elapsed, 3),
        "segments": segments,
    }


def simulate_multistage(pdf_path: Path) -> dict[str, Any]:
    """Pass-1 fast_text on full doc, then timed selective passes from enrichment plan."""
    pass1 = benchmark_pdf(pdf_path, "fast_text")
    cleanup_gpu_memory(force=True)

    from docling_worker import DoclingWorker

    worker = DoclingWorker()
    options = get_parse_mode("fast_text")
    converter = worker._create_document_converter(options)
    structured = converter.convert(str(pdf_path)).document.export_to_dict()
    plan = build_page_enrichment_plan(structured)

    table_pages = plan["pages_needing_table_structure"]
    vlm_pages = plan["pages_needing_vlm"]
    ocr_pages = plan["pages_needing_ocr"]

    pass2_table = run_selective_passes(
        pdf_path, pages=table_pages, mode=TABLE_PASS_MODE, label="pass2_table"
    )
    pass2_vlm = run_selective_passes(
        pdf_path, pages=vlm_pages, mode=VLM_PASS_MODE, label="pass2_vlm"
    )
    pass2_ocr = run_selective_passes(
        pdf_path, pages=ocr_pages, mode=OCR_PASS_MODE, label="pass2_ocr"
    )

    total_elapsed = (
        pass1["elapsed_s"]
        + pass2_table["elapsed_s"]
        + pass2_vlm["elapsed_s"]
        + pass2_ocr["elapsed_s"]
    )
    page_count = pass1["page_count"]
    return {
        "pass1_fast_text": pass1,
        "enrichment_plan": plan,
        "pass2_table": pass2_table,
        "pass2_vlm": pass2_vlm,
        "pass2_ocr": pass2_ocr,
        "total_elapsed_s": round(total_elapsed, 3),
        "effective_pages_per_min": round((page_count / total_elapsed) * 60, 2)
        if total_elapsed > 0
        else 0,
        "note": (
            "Docling page_range is contiguous (min..max per segment), so sparse page lists "
            "may over-count work vs a true per-page merge."
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Multistage parser benchmark (simulated)")
    parser.add_argument("--pdf", type=Path, required=True)
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("benchmarks/parser/registry/runs"),
    )
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--gpu-profile", default=None)
    args = parser.parse_args()

    if not args.pdf.is_file():
        print(f"PDF not found: {args.pdf}", file=sys.stderr)
        return 2

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bootstrap_gpu(args.gpu_profile)
    try:
        report = simulate_multistage(args.pdf)
    finally:
        cleanup_gpu_memory(force=True)

    run_dir = args.output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "kind": "multistage_simulation",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "pdf": str(args.pdf.resolve()),
        "pdf_sha256": sha256_file(args.pdf),
        "environment": collect_environment(gpu_profile=args.gpu_profile),
    }
    (run_dir / "run_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    (run_dir / "metrics.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    plan = report["enrichment_plan"]
    summary = [
        f"# Multistage simulation `{run_id}`",
        "",
        f"- PDF: `{args.pdf.name}`",
        f"- Pass-1 `fast_text`: {report['pass1_fast_text']['elapsed_s']}s "
        f"({report['pass1_fast_text']['pages_per_min']} pages/min)",
        f"- Plan: OCR pages {len(plan['pages_needing_ocr'])}, "
        f"VLM pages {len(plan['pages_needing_vlm'])}, "
        f"table pages {len(plan['pages_needing_table_structure'])}",
        f"- Pass-2 table (`{TABLE_PASS_MODE}`): {report['pass2_table']['elapsed_s']}s "
        f"({report['pass2_table']['range_count']} ranges, span {report['pass2_table']['page_span_total']} pp)",
        f"- Pass-2 VLM (`{VLM_PASS_MODE}`): {report['pass2_vlm']['elapsed_s']}s "
        f"({report['pass2_vlm']['range_count']} ranges, span {report['pass2_vlm']['page_span_total']} pp)",
        f"- Pass-2 OCR (`{OCR_PASS_MODE}`): {report['pass2_ocr']['elapsed_s']}s",
        f"- **Total simulated**: {report['total_elapsed_s']}s "
        f"({report['effective_pages_per_min']} effective pages/min)",
        "",
        report["note"],
        "",
    ]
    (run_dir / "summary.md").write_text("\n".join(summary), encoding="utf-8")
    print("\n".join(summary))
    print(f"Wrote multistage benchmark: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
