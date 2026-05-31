#!/usr/bin/env python3
"""Benchmark Docling parser modes locally on GPU with minimal run artifacts."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from parse_modes import PARSE_MODES, get_parse_mode
from worker_runtime import bootstrap_gpu, cleanup_gpu_memory

SCHEMA_VERSION = "parser-benchmark/v1"


def _nvidia_smi_snapshot() -> dict[str, Any]:
    try:
        out = subprocess.check_output(
            [
                "nvidia-smi",
                "--query-gpu=memory.used,memory.total,utilization.gpu",
                "--format=csv,noheader,nounits",
            ],
            text=True,
            timeout=5,
        ).strip()
        used, total, util = [part.strip() for part in out.split(",")]
        return {
            "memory_used_mib": int(float(used)),
            "memory_total_mib": int(float(total)),
            "gpu_util_percent": int(float(util)),
        }
    except (FileNotFoundError, subprocess.SubprocessError, ValueError):
        return {}


def _page_count(document, structured: dict) -> int:
    pages = getattr(document, "pages", None)
    if pages:
        return len(pages)
    raw_pages = structured.get("pages")
    if isinstance(raw_pages, dict):
        return len(raw_pages)
    if isinstance(raw_pages, list):
        return len(raw_pages)
    return 0


def benchmark_pdf(pdf_path: Path, mode: str) -> dict[str, Any]:
    from docling_worker import DoclingWorker

    worker = DoclingWorker()
    options = get_parse_mode(mode)
    converter = worker._create_document_converter(options)
    vram_before = _nvidia_smi_snapshot()
    started = time.perf_counter()
    result = converter.convert(str(pdf_path))
    elapsed_s = time.perf_counter() - started
    vram_after = _nvidia_smi_snapshot()
    document = result.document
    markdown = document.export_to_markdown()
    structured = document.export_to_dict()
    pages = _page_count(document, structured)
    return {
        "mode": mode,
        "elapsed_s": round(elapsed_s, 3),
        "page_count": pages,
        "pages_per_min": round((pages / elapsed_s) * 60, 2) if elapsed_s > 0 else 0,
        "markdown_chars": len(markdown),
        "markdown_path_hint": f"artifacts/{pdf_path.stem}_{mode}.md",
        "structured_keys": sorted(structured.keys()),
        "vram_before": vram_before,
        "vram_after": vram_after,
        "options": options,
    }


def write_run(
    *,
    output_root: Path,
    run_id: str,
    pdf_path: Path,
    modes: list[str],
    results: list[dict[str, Any]],
) -> Path:
    run_dir = output_root / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    config = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "pdf": str(pdf_path.resolve()),
        "modes": modes,
    }
    metrics = {
        "schema_version": SCHEMA_VERSION,
        "results": results,
        "best_pages_per_min": max((r["pages_per_min"] for r in results), default=0),
    }
    (run_dir / "run_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2) + "\n", encoding="utf-8")
    summary_lines = [
        f"# Parser benchmark `{run_id}`",
        "",
        f"- PDF: `{pdf_path.name}`",
        "",
    ]
    for row in results:
        summary_lines.append(
            f"- `{row['mode']}`: {row['pages_per_min']} pages/min, "
            f"{row['markdown_chars']} chars, {row['elapsed_s']}s"
        )
    (run_dir / "summary.md").write_text("\n".join(summary_lines) + "\n", encoding="utf-8")
    return run_dir


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark Docling parser modes")
    parser.add_argument("--pdf", type=Path, required=True)
    parser.add_argument(
        "--modes",
        default="baseline,fast_text,standard",
        help="Comma-separated parse modes",
    )
    parser.add_argument("--output-root", type=Path, default=Path("benchmarks/parser/runs"))
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--gpu-profile", default=None)
    args = parser.parse_args()

    if not args.pdf.is_file():
        print(f"PDF not found: {args.pdf}", file=sys.stderr)
        return 2

    modes = [m.strip() for m in args.modes.split(",") if m.strip()]
    for mode in modes:
        if mode not in PARSE_MODES:
            print(f"Unknown mode: {mode}", file=sys.stderr)
            return 2

    run_id = args.run_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bootstrap_gpu(args.gpu_profile)
    results: list[dict[str, Any]] = []
    try:
        for mode in modes:
            print(f"==> mode={mode}")
            results.append(benchmark_pdf(args.pdf, mode))
            cleanup_gpu_memory(force=True)
    finally:
        cleanup_gpu_memory(force=True)

    run_dir = write_run(
        output_root=args.output_root,
        run_id=run_id,
        pdf_path=args.pdf,
        modes=modes,
        results=results,
    )
    print(f"Wrote parser benchmark: {run_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
