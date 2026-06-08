"""Docling parse mode presets for benchmark and platform NATS requests.

Each mode is a dict of simple options understood by ``docling_worker._convert_simple_options``.
Use ``describe_parse_mode(name)`` for a human-readable summary.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any

# Keys match docling_worker._is_simple_options so NATS JSON passes through unchanged.
FAST_TEXT: dict[str, Any] = {
    "do_ocr": False,
    "do_table_structure": False,
    "do_picture_description": False,
    "generate_page_images": False,
    "generate_picture_images": False,
    "generate_table_images": False,
    "force_backend_text": True,
}

FAST_TEXT_TABLES: dict[str, Any] = {
    **FAST_TEXT,
    "do_table_structure": True,
    "table_do_cell_matching": False,
    "num_threads": 2,
    "layout_batch_size": 1,
    "ocr_batch_size": 1,
    "table_batch_size": 1,
    "queue_max_size": 1,
    "generate_parsed_pages": False,
}

BASELINE: dict[str, Any] = {
    "do_ocr": True,
    "do_table_structure": True,
    "do_picture_description": False,
    "generate_page_images": False,
    "generate_picture_images": False,
    "generate_table_images": False,
}

STANDARD: dict[str, Any] = deepcopy(BASELINE)

RICH: dict[str, Any] = {
    **BASELINE,
    "do_picture_description": True,
    "generate_picture_images": True,
    # Granite/SmolVLM load via transformers; flash-attn is optional in benchmark venv.
    "cuda_use_flash_attention2": False,
}

NEMOTRON_ENRICH: dict[str, Any] = {
    **BASELINE,
    "do_ocr": True,
    "ocr_engine": "nemotron",
    "ocr_merge_level": "word",
    "force_backend_text": False,
}

PARSE_MODES: dict[str, dict[str, Any]] = {
    "baseline": BASELINE,
    "fast_text": FAST_TEXT,
    "fast_text_tables": FAST_TEXT_TABLES,
    "standard": STANDARD,
    "rich": RICH,
    "nemotron_enrich": NEMOTRON_ENRICH,
}

# Plain-language summaries for ops docs and benchmark reports.
MODE_DESCRIPTIONS: dict[str, str] = {
    "fast_text": (
        "Pass-1 bulk ingest: reads embedded PDF text only (no OCR, no tables, no figures). "
        "Fastest; use for born-digital reports where text is already selectable."
    ),
    "fast_text_tables": (
        "Recommended pass-1 default: embedded PDF text plus table cell structure and layout "
        "regions (pictures/tables with page bbox). No OCR or VLM. ~30-40% slower than fast_text "
        "on born-digital reports; best balance of throughput and structured tables."
    ),
    "baseline": (
        "Full layout parse: OCR for scanned regions, table structure recovery, no figure captions. "
        "Production-like default when you need tables and scanned pages but not chart semantics."
    ),
    "standard": (
        "Alias of baseline today — same OCR + tables, no picture description. "
        "Kept for explicit 'standard pipeline' naming in platform flags."
    ),
    "rich": (
        "Baseline plus VLM picture description: charts, photos, and diagrams get a text caption "
        "in the markdown (Granite Vision by default). Slowest and highest VRAM; best for "
        "image-heavy sustainability reports."
    ),
    "nemotron_enrich": (
        "Baseline with Nemotron OCR instead of RapidOCR (requires Docling build with "
        "NemotronOcrOptions). Pending upstream release; benchmark separately."
    ),
}


def describe_parse_mode(name: str) -> str:
    key = (name or "baseline").strip().lower()
    if key not in MODE_DESCRIPTIONS:
        supported = ", ".join(sorted(MODE_DESCRIPTIONS))
        raise ValueError(f"Unknown parse mode {name!r}; supported: {supported}")
    return MODE_DESCRIPTIONS[key]


def get_parse_mode(name: str) -> dict[str, Any]:
    key = (name or "baseline").strip().lower()
    if key not in PARSE_MODES:
        supported = ", ".join(sorted(PARSE_MODES))
        raise ValueError(f"Unknown parse mode {name!r}; supported: {supported}")
    return deepcopy(PARSE_MODES[key])
