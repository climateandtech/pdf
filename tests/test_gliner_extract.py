"""Unit tests for isolated gliner.extract (no model download)."""

from kg_gliner.extract import extract_spans


def test_extract_empty_text():
    entities, relations = extract_spans("", ["CO2"])
    assert entities == []
    assert relations == []


def test_heuristic_label_match():
    """Runs without downloading GLiNER if package missing or on CPU-only hosts."""
    text = "Mitigation of CO2 emissions requires renewable energy."
    entities, relations = extract_spans(text, ["CO2"])
    assert relations == []
    assert entities
