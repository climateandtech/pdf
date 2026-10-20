"""Unit tests for isolated gliner.extract (no model download)."""

from unittest.mock import MagicMock, patch

from kg_gliner.extract import (
    DEFAULT_MODEL,
    _is_relex_model,
    _normalize_gliner25_entities,
    extract_spans,
)


def test_default_model_is_gliner25():
    assert "gliner2.5" in DEFAULT_MODEL


def test_is_relex_model_detection():
    assert _is_relex_model("knowledgator/gliner-relex-large-v0.5")
    assert not _is_relex_model("fastino/gliner2.5-base-v1")


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


def test_normalize_gliner25_dict_of_lists():
    raw = {"entities": {"person": ["Ada Lovelace"], "organization": ["ETH Zurich"]}}
    ents = _normalize_gliner25_entities(raw, ["person", "organization"])
    labels = {e["label"] for e in ents}
    texts = {e["text"] for e in ents}
    assert labels == {"person", "organization"}
    assert "Ada Lovelace" in texts
    assert "ETH Zurich" in texts


def test_extract_gliner25_path_mocked():
    """Belief: 2.5 path uses AutoExtractor.extract_entities, not ReLex inference."""
    mock_model = MagicMock()
    mock_model.extract_entities.return_value = {
        "entities": {"person": ["Michaela Bachmann"], "organization": ["CLIMAAX"]}
    }
    with patch("kg_gliner.extract._load_gliner25_model", return_value=mock_model):
        entities, relations = extract_spans(
            "Michaela Bachmann thanks CLIMAAX project.",
            ["person", "organization"],
            model_name="fastino/gliner2.5-base-v1",
        )
    mock_model.extract_entities.assert_called_once()
    assert relations == []
    assert any(e["text"] == "Michaela Bachmann" for e in entities)
    assert any(e["label"] == "organization" for e in entities)
