"""Tests for Docling parse mode presets."""

from parse_modes import FAST_TEXT, PARSE_MODES, get_parse_mode


def test_fast_text_disables_expensive_pipeline_stages():
    mode = get_parse_mode("fast_text")
    assert mode["do_ocr"] is False
    assert mode["do_table_structure"] is False
    assert mode["force_backend_text"] is True
    assert mode["do_picture_description"] is False


def test_get_parse_mode_returns_copy():
    first = get_parse_mode("standard")
    first["do_ocr"] = False
    second = get_parse_mode("standard")
    assert second["do_ocr"] is True


def test_all_modes_registered():
    assert set(PARSE_MODES) >= {"baseline", "fast_text", "standard", "rich"}
    assert FAST_TEXT is PARSE_MODES["fast_text"]
