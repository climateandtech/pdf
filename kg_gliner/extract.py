"""GLiNER extraction — runs on GPU; platform calls via NATS kg.infer.

Supports:
- GLiNER 2.5 boundary models via ``gliner2.AutoExtractor`` (default)
- Legacy GLiNER-ReLex via ``gliner.GLiNER`` when ``KG_GLINER_MODEL`` is a ReLex id
"""

from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Fastino 2.5 IE family is small/base/multi only (no *-large*). GPU uses base.
DEFAULT_MODEL = "fastino/gliner2.5-base-v1"
RELEX_MODEL_MARKERS = ("gliner-relex", "knowledgator/")


def _is_relex_model(model_name: str) -> bool:
    lower = (model_name or "").lower()
    return any(marker in lower for marker in RELEX_MODEL_MARKERS)


@lru_cache(maxsize=2)
def _load_relex_model(model_name: str):
    from gliner import GLiNER  # pip package (not this directory)

    return GLiNER.from_pretrained(model_name)


@lru_cache(maxsize=2)
def _load_gliner25_model(model_name: str):
    from gliner2 import AutoExtractor

    return AutoExtractor.from_pretrained(model_name)


def extract_spans(
    text: str,
    entity_labels: List[str],
    relation_labels: Optional[List[str]] = None,
    *,
    threshold: float = 0.4,
    model_name: str = DEFAULT_MODEL,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Return (entities, relations) as dicts with text, label, score, start, end."""
    if not text or not text.strip():
        return [], []

    labels = [label for label in entity_labels if label]
    if not labels:
        return [], []

    try:
        if _is_relex_model(model_name):
            return _extract_relex(
                text,
                labels,
                relation_labels,
                threshold=threshold,
                model_name=model_name,
            )
        return _extract_gliner25(
            text,
            labels,
            relation_labels,
            threshold=threshold,
            model_name=model_name,
        )
    except ImportError:
        logger.warning("gliner/gliner2 not installed; using heuristic extraction")
        return _heuristic_entities(text, labels), []
    except (OSError, RuntimeError, ValueError, TypeError) as e:
        logger.error("gliner inference failed: %s", e)
        return _heuristic_entities(text, labels), []


def _extract_relex(
    text: str,
    labels: List[str],
    relation_labels: Optional[List[str]],
    *,
    threshold: float,
    model_name: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    model = _load_relex_model(model_name)
    rel = relation_labels or []
    if rel:
        entities, relations = model.inference(
            texts=[text],
            labels=labels,
            relations=rel,
            threshold=threshold,
        )
        return (
            _normalize_entities(entities[0] if entities else []),
            _normalize_relations(relations[0] if relations else []),
        )
    entities = model.inference(
        texts=[text],
        labels=labels,
        threshold=threshold,
    )
    return _normalize_entities(entities[0] if entities else []), []


def _extract_gliner25(
    text: str,
    labels: List[str],
    relation_labels: Optional[List[str]],
    *,
    threshold: float,
    model_name: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """GLiNER 2.5 via AutoExtractor; relations optional when API supports them."""
    del threshold  # AutoExtractor uses model defaults; keep param for NATS parity
    model = _load_gliner25_model(model_name)
    raw = model.extract_entities(text, labels)
    entities = _normalize_gliner25_entities(raw, labels)
    relations: List[Dict[str, Any]] = []
    rel = relation_labels or []
    if rel and hasattr(model, "extract_relations"):
        try:
            rel_raw = model.extract_relations(text, rel)
            relations = _normalize_relations(rel_raw)
        except (OSError, RuntimeError, ValueError, TypeError, AttributeError) as exc:
            logger.debug("gliner25 relations skipped: %s", exc)
    return entities, relations


def _normalize_gliner25_entities(raw: Any, _labels: List[str]) -> List[Dict[str, Any]]:
    """Normalize AutoExtractor output (dict-of-lists or list-of-dicts)."""
    if raw is None:
        return []
    # Common shape: {'entities': {'person': ['Ada'], ...}} or {'person': [...]}
    if isinstance(raw, dict):
        bag = raw.get("entities") if isinstance(raw.get("entities"), dict) else raw
        out: List[Dict[str, Any]] = []
        if isinstance(bag, dict):
            for label, spans in bag.items():
                if label == "entities":
                    continue
                for span in spans or []:
                    out.append(_span_to_entity(span, label=str(label)))
            return [e for e in out if e.get("text")]
        return _normalize_entities(raw if isinstance(raw, list) else [])
    if isinstance(raw, list):
        return _normalize_entities(raw)
    return []


def _span_to_entity(span: Any, *, label: str) -> Dict[str, Any]:
    if isinstance(span, str):
        return {"text": span, "label": label, "score": 0.5, "start": None, "end": None}
    if isinstance(span, dict):
        return {
            "text": span.get("text") or span.get("span") or "",
            "label": span.get("label") or span.get("type") or label,
            "score": float(span.get("score", 0.5)),
            "start": span.get("start"),
            "end": span.get("end"),
        }
    return {"text": str(span), "label": label, "score": 0.5, "start": None, "end": None}


def _normalize_entities(raw: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not raw:
        return out
    for item in raw:
        if isinstance(item, dict):
            out.append(
                {
                    "text": item.get("text") or item.get("span") or "",
                    "label": item.get("label") or item.get("type") or "",
                    "score": float(item.get("score", 0.5)),
                    "start": item.get("start"),
                    "end": item.get("end"),
                }
            )
    return out


def _normalize_relations(raw: Any) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not raw:
        return out
    if isinstance(raw, dict):
        # Possible {'relations': [...]} wrapper
        raw = raw.get("relations") or raw.get("items") or []
    for item in raw:
        if isinstance(item, dict):
            out.append(dict(item))
    return out


def _heuristic_entities(text: str, labels: List[str]) -> List[Dict[str, Any]]:
    """Lightweight span detector when GLiNER is unavailable."""
    found: List[Dict[str, Any]] = []
    for label in labels[:20]:
        pattern = re.compile(re.escape(label), re.IGNORECASE)
        for m in pattern.finditer(text):
            found.append(
                {
                    "text": m.group(0),
                    "label": label,
                    "score": 0.51,
                    "start": m.start(),
                    "end": m.end(),
                }
            )
    if not found and labels:
        for m in re.finditer(r"\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\b", text):
            found.append(
                {
                    "text": m.group(0),
                    "label": labels[0],
                    "score": 0.5,
                    "start": m.start(),
                    "end": m.end(),
                }
            )
    return found[:50]
