"""GLiNER extraction — runs on GPU; platform calls via NATS kg.infer.

Supports:
- GLiNER 2.5 boundary models via ``gliner2.AutoExtractor`` (default)
- Legacy GLiNER-ReLex via ``gliner.GLiNER`` when ``KG_GLINER_MODEL`` is a ReLex id

Crowded label sets collapse recall (e.g. NHESS/UNFCCC vanish at 7 labels).
When more than ``KG_GLINER_LABEL_BATCH_SIZE`` labels are requested, we run the
same model in smaller batches and merge — still ML, no acronym allowlists.
"""

from __future__ import annotations

import logging
import os
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple  # Optional used for unscored spans

logger = logging.getLogger(__name__)

# Fastino 2.5 IE family is small/base/multi only (no *-large*). GPU uses base.
DEFAULT_MODEL = "fastino/gliner2.5-base-v1"
RELEX_MODEL_MARKERS = ("gliner-relex", "knowledgator/")
# Empirically, batches of 3-4 keep org/event recall on LinkedIn-length text.
DEFAULT_LABEL_BATCH_SIZE = 4
# Extra ML pass with org-family labels only (no allowlists) recovers orgs that
# vanish when person/location compete in the same batch.
ORG_FAMILY_LABELS: tuple[str, ...] = (
    "organization",
    "company",
    "ngo",
    "government organization",
    "standard",
)


def _label_batch_size() -> int:
    raw = (os.getenv("KG_GLINER_LABEL_BATCH_SIZE") or "").strip()
    if not raw:
        return DEFAULT_LABEL_BATCH_SIZE
    try:
        return max(1, int(raw))
    except ValueError:
        return DEFAULT_LABEL_BATCH_SIZE


def _batched_labels(labels: List[str], batch_size: int) -> List[List[str]]:
    if len(labels) <= batch_size:
        return [labels]
    return [labels[i : i + batch_size] for i in range(0, len(labels), batch_size)]


def _entity_dedupe_key(ent: Dict[str, Any]) -> Tuple[str, str, Any, Any]:
    return (
        str(ent.get("label") or "").casefold(),
        str(ent.get("text") or "").casefold(),
        ent.get("start"),
        ent.get("end"),
    )


def _merge_entities(batches: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    seen: set[Tuple[str, str, Any, Any]] = set()
    for ents in batches:
        for ent in ents:
            key = _entity_dedupe_key(ent)
            if key in seen:
                continue
            seen.add(key)
            out.append(ent)
    return out


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
    batch_size = _label_batch_size()
    batches = _batched_labels(labels, batch_size)
    org_family = [lab for lab in ORG_FAMILY_LABELS if lab in labels]
    # Reinforce with a dedicated org-family pass when those labels were diluted.
    if org_family and (len(batches) > 1 or len(labels) > len(org_family)):
        batches = [*list(batches), org_family]
    if len(batches) > 1:
        logger.info(
            "gliner25 label batching labels=%s batch_size=%s passes=%s",
            len(labels),
            batch_size,
            len(batches),
        )
    entity_batches: List[List[Dict[str, Any]]] = []
    for batch in batches:
        raw = model.extract_entities(text, batch)
        entity_batches.append(_normalize_gliner25_entities(raw, batch))
    entities = _merge_entities(entity_batches)
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


def _optional_score(raw: Any) -> Optional[float]:
    """Pass through model scores; never invent a default confidence."""
    if raw is None:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _span_to_entity(span: Any, *, label: str) -> Dict[str, Any]:
    if isinstance(span, str):
        return {"text": span, "label": label, "score": None, "start": None, "end": None}
    if isinstance(span, dict):
        return {
            "text": span.get("text") or span.get("span") or "",
            "label": span.get("label") or span.get("type") or label,
            "score": _optional_score(span.get("score")),
            "start": span.get("start"),
            "end": span.get("end"),
        }
    return {"text": str(span), "label": label, "score": None, "start": None, "end": None}


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
                    "score": _optional_score(item.get("score")),
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
