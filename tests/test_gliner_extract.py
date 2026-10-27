"""Unit tests for isolated gliner.extract (no model download)."""

from unittest.mock import MagicMock, patch

from kg_gliner.extract import (
    DEFAULT_MODEL,
    _batched_labels,
    _is_relex_model,
    _merge_entities,
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
    # String spans have no model score — do not invent confidence.
    assert all(e.get("score") is None for e in ents)


def test_normalize_gliner25_preserves_explicit_score():
    raw = {
        "entities": {
            "person": [{"text": "Ada", "score": 0.91}],
        }
    }
    ents = _normalize_gliner25_entities(raw, ["person"])
    assert ents[0]["score"] == 0.91


def test_extract_gliner25_path_mocked():
    """Belief: 2.5 path uses AutoExtractor; may add an org-family reinforce pass."""
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
    assert mock_model.extract_entities.call_count >= 1
    assert relations == []
    assert any(e["text"] == "Michaela Bachmann" for e in entities)
    assert any(e["label"] == "organization" for e in entities)


def test_batched_labels_splits_when_over_size():
    assert _batched_labels(["a", "b", "c", "d", "e"], 2) == [
        ["a", "b"],
        ["c", "d"],
        ["e"],
    ]
    assert _batched_labels(["a", "b"], 4) == [["a", "b"]]


def test_merge_entities_dedupes_by_label_text_span():
    merged = _merge_entities(
        [
            [{"text": "NHESS", "label": "organization", "start": 1, "end": 6}],
            [
                {"text": "NHESS", "label": "organization", "start": 1, "end": 6},
                {"text": "Baku", "label": "location", "start": 10, "end": 14},
            ],
        ]
    )
    assert len(merged) == 2


def test_extract_gliner25_batches_crowded_labels(monkeypatch):
    """Belief: crowded labels batch + org-family reinforcement pass."""
    monkeypatch.setenv("KG_GLINER_LABEL_BATCH_SIZE", "2")
    mock_model = MagicMock()

    def _extract(_text, labels):
        return {"entities": {labels[0]: [f"span-{labels[0]}"]}}

    mock_model.extract_entities.side_effect = _extract
    labels = ["person", "organization", "company", "location"]
    with patch("kg_gliner.extract._load_gliner25_model", return_value=mock_model):
        entities, _ = extract_spans(
            "x",
            labels,
            model_name="fastino/gliner2.5-base-v1",
        )
    # 2 primary batches + 1 org-family reinforce (organization, company)
    assert mock_model.extract_entities.call_count == 3
    assert "span-person" in {e["text"] for e in entities}
    assert "span-organization" in {e["text"] for e in entities} or "span-company" in {
        e["text"] for e in entities
    }
