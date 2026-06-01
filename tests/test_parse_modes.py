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
    assert set(PARSE_MODES) >= {
        "baseline",
        "fast_text",
        "fast_text_tables",
        "standard",
        "rich",
        "nemotron_enrich",
    }
    assert FAST_TEXT is PARSE_MODES["fast_text"]


def test_fast_text_tables_enables_table_structure_only():
    mode = get_parse_mode("fast_text_tables")
    assert mode["force_backend_text"] is True
    assert mode["do_table_structure"] is True
    assert mode["do_ocr"] is False


def test_describe_parse_mode_documents_fast_text_tables():
    from parse_modes import describe_parse_mode

    text = describe_parse_mode("fast_text_tables")
    assert "table" in text.lower()
    assert "no ocr" in text.lower()


def test_nemotron_enrich_uses_docling_native_ocr_engine():
    mode = get_parse_mode("nemotron_enrich")
    assert mode["do_ocr"] is True
    assert mode["ocr_engine"] == "nemotron"
    assert mode["do_table_structure"] is True
