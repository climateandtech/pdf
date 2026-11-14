"""Isolated GLiNER 2.5 inference (GPU worker — no platform DB dependency)."""

from .extract import DEFAULT_MODEL, extract_spans

__all__ = ["DEFAULT_MODEL", "extract_spans"]
