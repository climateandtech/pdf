"""Hierarchical multi-tier chunking via Docling HybridChunker (CPU chunk worker)."""

from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

DEFAULT_MICRO_TOKENS = int(os.getenv("CHUNK_MICRO_TARGET_TOKENS", "150"))
DEFAULT_CHILD_TOKENS = int(os.getenv("CHUNK_TARGET_TOKENS", "512"))
DEFAULT_PARENT_MAX_TOKENS = int(os.getenv("CHUNK_PARENT_MAX_TOKENS", "2000"))
DEFAULT_MICRO_OVERLAP = int(os.getenv("CHUNK_MICRO_TOKEN_OVERLAP", "32"))


def approx_token_count(text: str) -> int:
    """Whitespace token proxy aligned with platform chunking_config."""
    return len(re.findall(r"\S+", text or ""))


def _heading_path(chunk: Any) -> list[str]:
    meta = getattr(chunk, "meta", None)
    if meta is None:
        return []
    headings = getattr(meta, "headings", None)
    if headings:
        return [str(item) for item in headings]
    if isinstance(meta, dict):
        raw = meta.get("headings") or []
        return [str(item) for item in raw]
    return []


def _chunk_text(chunk: Any) -> str:
    text = getattr(chunk, "text", None)
    if text:
        return str(text).strip()
    if hasattr(chunk, "export_to_markdown"):
        return str(chunk.export_to_markdown()).strip()
    return str(chunk).strip()


def load_docling_document(structured_data: dict[str, Any]) -> Any:
    """Deserialize stored GPU Docling JSON into a DoclingDocument."""
    from docling.datamodel.document import DoclingDocument

    payload = dict(structured_data or {})
    payload.pop("platform_hierarchical_chunks", None)
    payload.pop("__platform_hierarchical_chunks__", None)
    return DoclingDocument.model_validate(payload)


def _make_hybrid_chunker(max_tokens: int) -> Any:
    from docling.chunking import HybridChunker

    try:
        from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer

        tokenizer = HuggingFaceTokenizer(model_name="BAAI/bge-m3")
        return HybridChunker(
            tokenizer=tokenizer,
            max_tokens=max_tokens,
            merge_peers=True,
        )
    except Exception:
        return HybridChunker(max_tokens=max_tokens, merge_peers=True)


@dataclass(frozen=True)
class TierChunkRecord:
    """One serializable hierarchical chunk tier."""

    chunk_index: int
    chunk_level: str
    target_tokens: int | None
    text: str
    contextual_text: str | None
    heading_path: list[str]
    token_count: int
    embed: bool
    parent_index: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _split_parent_text(text: str, *, max_tokens: int) -> list[str]:
    tokens = re.findall(r"\S+", text)
    if not tokens:
        return []
    if len(tokens) <= max_tokens:
        return [text.strip()]

    parts: list[str] = []
    start = 0
    while start < len(tokens):
        end = min(len(tokens), start + max_tokens)
        parts.append(" ".join(tokens[start:end]))
        if end >= len(tokens):
            break
        start = end
    return parts


def _build_parent_records(
    hybrid_chunks: list[Any],
    hybrid_chunker: Any,
    *,
    parent_max_tokens: int,
) -> tuple[list[TierChunkRecord], dict[tuple[str, ...], int]]:
    groups: dict[tuple[str, ...], list[Any]] = defaultdict(list)
    for chunk in hybrid_chunks:
        groups[tuple(_heading_path(chunk))].append(chunk)

    parent_records: list[TierChunkRecord] = []
    heading_to_parent_index: dict[tuple[str, ...], int] = {}
    parent_index = 0
    for heading_path, chunks in groups.items():
        combined = "\n\n".join(_chunk_text(chunk) for chunk in chunks if _chunk_text(chunk))
        for part_index, part_text in enumerate(
            _split_parent_text(combined, max_tokens=parent_max_tokens)
        ):
            contextual = part_text
            if heading_path:
                contextual = "\n".join(heading_path) + "\n\n" + part_text
            parent_records.append(
                TierChunkRecord(
                    chunk_index=parent_index,
                    chunk_level="parent",
                    target_tokens=parent_max_tokens,
                    text=part_text,
                    contextual_text=contextual,
                    heading_path=list(heading_path),
                    token_count=approx_token_count(part_text),
                    embed=False,
                    parent_index=None,
                    metadata={"part_index": part_index, "child_count": len(chunks)},
                )
            )
            if part_index == 0:
                heading_to_parent_index[heading_path] = parent_index
            parent_index += 1
    return parent_records, heading_to_parent_index


def _hybrid_tier_records(
    hybrid_chunks: Iterator[Any],
    hybrid_chunker: Any,
    *,
    chunk_level: str,
    target_tokens: int,
    heading_to_parent_index: dict[tuple[str, ...], int],
    start_index: int,
) -> list[TierChunkRecord]:
    records: list[TierChunkRecord] = []
    for offset, chunk in enumerate(hybrid_chunks):
        heading_path = _heading_path(chunk)
        text = _chunk_text(chunk)
        if not text:
            continue
        contextual = hybrid_chunker.contextualize(chunk)
        records.append(
            TierChunkRecord(
                chunk_index=start_index + offset,
                chunk_level=chunk_level,
                target_tokens=target_tokens,
                text=text,
                contextual_text=contextual,
                heading_path=heading_path,
                token_count=approx_token_count(text),
                embed=True,
                parent_index=heading_to_parent_index.get(tuple(heading_path)),
                metadata={},
            )
        )
    return records


def chunk_hybrid(
    structured_data: dict[str, Any],
    *,
    max_tokens: int = DEFAULT_CHILD_TOKENS,
) -> dict[str, Any]:
    """Single-tier Docling HybridChunker output (upper bound = max_tokens)."""
    started = time.perf_counter()
    document = load_docling_document(structured_data)
    chunker = _make_hybrid_chunker(max_tokens)
    hybrid_chunks = list(chunker.chunk(dl_doc=document))
    records = _hybrid_tier_records(
        iter(hybrid_chunks),
        chunker,
        chunk_level="child",
        target_tokens=max_tokens,
        heading_to_parent_index={},
        start_index=0,
    )
    elapsed_s = time.perf_counter() - started
    payload_records = [record.to_dict() for record in records]
    token_counts = [record.token_count for record in records]
    return {
        "records": payload_records,
        "tier_counts": {"child": len(records)},
        "metrics": {
            "chunk_wall_time_s": round(elapsed_s, 3),
            "embed_vector_count": len(records),
            "storage_text_bytes": sum(len(record.text) for record in records),
            "max_tokens": max_tokens,
            "avg_chunk_tokens": (
                round(sum(token_counts) / len(token_counts), 1) if token_counts else 0.0
            ),
            "p95_chunk_tokens": (
                sorted(token_counts)[max(0, int(0.95 * len(token_counts)) - 1)] if token_counts else 0
            ),
        },
    }


def chunk_hierarchical(
    structured_data: dict[str, Any],
    *,
    micro_tokens: int = DEFAULT_MICRO_TOKENS,
    child_tokens: int = DEFAULT_CHILD_TOKENS,
    parent_max_tokens: int = DEFAULT_PARENT_MAX_TOKENS,
) -> dict[str, Any]:
    """Build element/micro/child/parent tiers from stored Docling JSON."""
    from docling.chunking import HierarchicalChunker

    started = time.perf_counter()
    document = load_docling_document(structured_data)

    element_records: list[TierChunkRecord] = []
    for index, chunk in enumerate(HierarchicalChunker().chunk(dl_doc=document)):
        text = _chunk_text(chunk)
        if not text:
            continue
        element_records.append(
            TierChunkRecord(
                chunk_index=index,
                chunk_level="element",
                target_tokens=None,
                text=text,
                contextual_text=None,
                heading_path=_heading_path(chunk),
                token_count=approx_token_count(text),
                embed=False,
                metadata={},
            )
        )

    child_chunker = _make_hybrid_chunker(child_tokens)
    child_hybrid = list(child_chunker.chunk(dl_doc=document))
    parent_records, heading_to_parent_index = _build_parent_records(
        child_hybrid,
        child_chunker,
        parent_max_tokens=parent_max_tokens,
    )

    micro_chunker = _make_hybrid_chunker(micro_tokens)
    micro_records = _hybrid_tier_records(
        micro_chunker.chunk(dl_doc=document),
        micro_chunker,
        chunk_level="micro",
        target_tokens=micro_tokens,
        heading_to_parent_index=heading_to_parent_index,
        start_index=0,
    )
    child_records = _hybrid_tier_records(
        iter(child_hybrid),
        child_chunker,
        chunk_level="child",
        target_tokens=child_tokens,
        heading_to_parent_index=heading_to_parent_index,
        start_index=0,
    )

    elapsed_s = time.perf_counter() - started
    all_records = (
        [record.to_dict() for record in element_records]
        + [record.to_dict() for record in micro_records]
        + [record.to_dict() for record in child_records]
        + [record.to_dict() for record in parent_records]
    )
    tier_counts = {
        "element": len(element_records),
        "micro": len(micro_records),
        "child": len(child_records),
        "parent": len(parent_records),
    }
    searchable = [record for record in all_records if record.get("embed")]
    storage_bytes = sum(len(record.get("text") or "") for record in all_records)
    storage_bytes += sum(len(record.get("contextual_text") or "") for record in all_records)

    return {
        "records": all_records,
        "tier_counts": tier_counts,
        "metrics": {
            "chunk_wall_time_s": round(elapsed_s, 3),
            "embed_vector_count": len(searchable),
            "storage_text_bytes": storage_bytes,
            "micro_tokens": micro_tokens,
            "child_tokens": child_tokens,
            "parent_max_tokens": parent_max_tokens,
        },
    }
