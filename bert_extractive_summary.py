"""Extractive summarize via sentence embeddings + k-means (Miller / Altameemi phase-1).

Runs on the GPU host under ``summarize_worker.py`` for ``docs.summarize.*`` jobs.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

import numpy as np

logger = logging.getLogger(__name__)

_encoder = None
_encoder_failed = False
_legacy_summarizer = None
_legacy_failed = False

DEFAULT_NUM_SENTENCES = int(os.getenv("BERT_SUMMARY_NUM_SENTENCES", "3"))
DEFAULT_MAX_CHARS = int(os.getenv("BERT_SUMMARY_MAX_CHARS", "12000"))
DEFAULT_ST_MODEL = os.getenv("BERT_SUMMARY_MODEL", "sentence-transformers/all-MiniLM-L6-v2")


def _split_sentences(text: str) -> list[str]:
    normalized = re.sub(r"\s+", " ", (text or "").strip())
    parts = re.split(r"(?<=[.!?])\s+", normalized)
    return [p.strip() for p in parts if len(p.strip()) >= 20]


def _get_encoder():
    global _encoder, _encoder_failed
    if _encoder_failed:
        return None
    if _encoder is not None:
        return _encoder
    try:
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415

        _encoder = SentenceTransformer(DEFAULT_ST_MODEL)
    except Exception as exc:  # noqa: BLE001 — optional ML dep
        logger.warning("sentence-transformers unavailable for extractive summary: %s", exc)
        _encoder_failed = True
        return None
    return _encoder


def _get_legacy_miller_summarizer():
    """Optional Miller package; often fails on modern transformers."""
    global _legacy_summarizer, _legacy_failed
    if _legacy_failed:
        return None
    if _legacy_summarizer is not None:
        return _legacy_summarizer
    try:
        from summarizer.bert import Summarizer  # noqa: PLC0415

        _legacy_summarizer = Summarizer(model="bert-base-uncased")
    except Exception as exc:  # noqa: BLE001
        logger.info(
            "bert-extractive-summarizer unavailable (expected on transformers>=4.36): %s",
            exc,
        )
        _legacy_failed = True
        return None
    return _legacy_summarizer


def _select_by_kmeans(sentences: list[str], num_sentences: int) -> list[str] | None:
    encoder = _get_encoder()
    if encoder is None:
        return None
    if len(sentences) <= num_sentences:
        return sentences[:num_sentences]

    from sklearn.cluster import KMeans  # noqa: PLC0415

    embeddings = np.asarray(encoder.encode(sentences, show_progress_bar=False))
    k = min(num_sentences, len(sentences))
    km = KMeans(n_clusters=k, n_init=10, random_state=42)
    km.fit(embeddings)
    selected_idx: list[int] = []
    for cluster_id in range(k):
        members = np.where(km.labels_ == cluster_id)[0]
        if len(members) == 0:
            continue
        center = km.cluster_centers_[cluster_id]
        dists = np.linalg.norm(embeddings[members] - center, axis=1)
        selected_idx.append(int(members[int(np.argmin(dists))]))
    selected_idx = sorted(set(selected_idx))
    return [sentences[i] for i in selected_idx]


def _exclude_intro_from_summary() -> bool:
    raw = (os.getenv("BERT_SUMMARY_EXCLUDE_INTRO_FROM_SUMMARY", "1") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _fields_from_sentences(out_sentences: list[str], n: int, backend: str) -> dict[str, Any]:
    if len(out_sentences) == 1:
        summary_text = out_sentences[0]
    elif _exclude_intro_from_summary():
        summary_text = " ".join(out_sentences[1:n])
    else:
        summary_text = " ".join(out_sentences[:n])
    return {
        "introduction": out_sentences[0],
        "key_points": ", ".join(out_sentences[:n]),
        "summary": summary_text,
        "limitations": "None stated",
        "summary_backend": backend,
    }


def summarize_extractive(
    text: str,
    *,
    num_sentences: int | None = None,
) -> dict[str, Any]:
    """Return catalog-compatible summary fields from extractive sentence selection.

    Shape matches LLM summarize merge keys: ``summary``, ``key_points``,
    ``introduction`` (first selected sentence).
    """
    body = (text or "").strip()
    if len(body) < 100:
        return {}

    n = num_sentences if num_sentences is not None else DEFAULT_NUM_SENTENCES
    bounded = body[:DEFAULT_MAX_CHARS]
    sentences = _split_sentences(bounded)
    if not sentences:
        return {}

    selected = ""
    backend = "sentence_kmeans"

    legacy = _get_legacy_miller_summarizer()
    if legacy is not None:
        try:
            selected = str(legacy(bounded, num_sentences=n) or "").strip()
            if selected:
                backend = "bert_extractive_summarizer"
        except Exception as exc:  # noqa: BLE001
            logger.warning("Legacy Miller summarizer failed: %s", exc)
            selected = ""

    if not selected:
        try:
            picked = _select_by_kmeans(sentences, n)
            if picked:
                selected = " ".join(picked)
                backend = "sentence_kmeans"
        except Exception as exc:  # noqa: BLE001
            logger.warning("K-means extractive summarize failed: %s", exc)
            selected = ""

    if not selected:
        selected = " ".join(sentences[:n])
        backend = "lead_sentences"

    out_sentences = _split_sentences(selected) or [selected]
    return _fields_from_sentences(out_sentences, n, backend)
