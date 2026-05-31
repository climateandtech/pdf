"""Docling parse mode presets for benchmark and platform NATS requests."""

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
    "standard": STANDARD,
    "rich": RICH,
    "nemotron_enrich": NEMOTRON_ENRICH,
}


def get_parse_mode(name: str) -> dict[str, Any]:
    key = (name or "baseline").strip().lower()
    if key not in PARSE_MODES:
        supported = ", ".join(sorted(PARSE_MODES))
        raise ValueError(f"Unknown parse mode {name!r}; supported: {supported}")
    return deepcopy(PARSE_MODES[key])
