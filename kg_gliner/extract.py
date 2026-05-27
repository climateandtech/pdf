"""GLiNER-ReLex extraction — runs on GPU; platform calls via NATS kg.infer."""

from __future__ import annotations

import logging
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

DEFAULT_MODEL = "knowledgator/gliner-relex-large-v0.5"


@lru_cache(maxsize=1)
def _load_model(model_name: str = DEFAULT_MODEL):
    from gliner import GLiNER  # pip package (not this directory)

    return GLiNER.from_pretrained(model_name)


def extract_spans(
    text: str,
    entity_labels: List[str],
    relation_labels: Optional[List[str]] = None,
    *,
    threshold: float = 0.4,
    model_name: str = DEFAULT_MODEL,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Return (entities, relations) as dicts with text, label, score, start, end.
    """
    if not text or not text.strip():
        return [], []

    labels = [label for label in entity_labels if label]
    if not labels:
        return [], []

    try:
        model = _load_model(model_name)
        rel = relation_labels or []
        if rel:
            entities, relations = model.inference(
                texts=[text],
                labels=labels,
                relations=rel,
                threshold=threshold,
            )
            ent_list = _normalize_entities(entities[0] if entities else [])
            rel_list = _normalize_relations(relations[0] if relations else [])
            return ent_list, rel_list
        entities = model.inference(
            texts=[text],
            labels=labels,
            threshold=threshold,
        )
        return _normalize_entities(entities[0] if entities else []), []
    except ImportError:
        logger.warning("gliner not installed; using heuristic extraction")
        return _heuristic_entities(text, labels), []
    except Exception as e:
        logger.error("gliner inference failed: %s", e)
        return _heuristic_entities(text, labels), []


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
