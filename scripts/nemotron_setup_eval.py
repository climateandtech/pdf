#!/usr/bin/env python3
"""Record Nemotron OCR/Parse setup options without blocking the Docling benchmark path."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path


ROUTES = [
    {
        "route": "huggingface_local",
        "model": "nvidia/nemotron-ocr-v2",
        "notes": "Requires local GPU memory and HF model download; best for offline enrichment smoke tests.",
    },
    {
        "route": "nemo_retriever_parse_container",
        "model": "NeMo Retriever Parse",
        "notes": "Container-based parse pipeline; evaluate setup friction separately from Docling worker.",
    },
    {
        "route": "nvidia_nim_endpoint",
        "model": "Nemotron Parse NIM",
        "notes": "Hosted endpoint if local RTX 4000 SFF Ada VRAM is insufficient.",
    },
]


def evaluate_prerequisites() -> dict:
    return {
        "docker_available": shutil.which("docker") is not None,
        "huggingface_cli": shutil.which("huggingface-cli") is not None,
        "nvidia_smi": shutil.which("nvidia-smi") is not None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Nemotron setup evaluation checklist")
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("benchmarks/nemotron/setup_eval.json"),
        help="Where to write the evaluation record",
    )
    args = parser.parse_args()

    report = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "deferred",
        "recommendation": "Continue with Docling-only benchmarks; run Nemotron only for enrichment candidates.",
        "prerequisites": evaluate_prerequisites(),
        "routes": ROUTES,
        "smoke_pages": [
            "text-native page",
            "table-heavy page",
            "scanned/image-heavy page",
        ],
        "blocked_by_default": True,
        "reason": "Nemotron is an optional enrichment backend, not required for fast-first indexing rollout.",
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
