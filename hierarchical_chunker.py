"""Hierarchical multi-tier chunking via Docling HybridChunker (CPU chunk worker)."""

from __future__ import annotations

import os
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

from result_publish import _strip_binary_blobs

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


def _docling_model_dump(value: Any) -> Any:
    """Serialize Docling pydantic models to JSON-safe dicts."""
    if value is None:
        return None
    if hasattr(value, "model_dump"):
        try:
            return value.model_dump(mode="json")
        except TypeError:
            return value.model_dump()
    if isinstance(value, (list, tuple)):
        return [_docling_model_dump(item) for item in value]
    if isinstance(value, dict):
        return {key: _docling_model_dump(item) for key, item in value.items()}
    if hasattr(value, "value"):
        return str(value.value)
    return value


def _build_docling_ref_index(document: Any) -> dict[str, tuple[str, dict[str, Any]]]:
    """Map Docling self_ref to (collection, item dict) for provenance enrichment."""
    exported = document.export_to_dict() if hasattr(document, "export_to_dict") else {}
    index: dict[str, tuple[str, dict[str, Any]]] = {}
    for collection in ("texts", "tables", "pictures", "groups"):
        items = exported.get(collection) or []
        if isinstance(items, dict):
            items = list(items.values())
        for item in items:
            if not isinstance(item, dict):
                continue
            ref = item.get("self_ref")
            if ref:
                index[str(ref)] = (collection, item)
    return index


_RESOLVED_REF_FIELDS = (
    "label",
    "prov",
    "captions",
    "annotations",
    "footnotes",
    "references",
    "content_layer",
    "parent",
    "children",
)


def _enrich_doc_item(
    item: dict[str, Any],
    *,
    ref_index: dict[str, tuple[str, dict[str, Any]]] | None,
) -> dict[str, Any]:
    """Attach resolved Docling element metadata for a chunk doc_item."""
    enriched = dict(item)
    ref = enriched.get("self_ref")
    if not ref_index or not ref:
        return enriched
    hit = ref_index.get(str(ref))
    if hit is None:
        return enriched
    collection, resolved = hit
    enriched["resolved"] = _strip_binary_blobs(
        {
            "collection": collection,
            **{
                key: resolved.get(key)
                for key in _RESOLVED_REF_FIELDS
                if resolved.get(key) is not None
            },
        }
    )
    return enriched


def _extract_docling_chunk_metadata(
    chunk: Any,
    *,
    ref_index: dict[str, tuple[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Capture HybridChunker/HierarchicalChunker meta on the chunk record."""
    meta = getattr(chunk, "meta", None)
    if meta is None:
        return {}

    raw_meta = _docling_model_dump(meta)
    if not isinstance(raw_meta, dict):
        return {}

    doc_items: list[dict[str, Any]] = []
    labels: set[str] = set()
    page_numbers: set[int] = set()
    self_refs: list[str] = []

    for raw_item in raw_meta.get("doc_items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = _enrich_doc_item(raw_item, ref_index=ref_index)
        label = str(item.get("label") or "")
        if label:
            labels.add(label)
        ref = item.get("self_ref")
        if ref:
            self_refs.append(str(ref))
        for prov in item.get("prov") or []:
            if not isinstance(prov, dict):
                continue
            page_no = prov.get("page_no")
            if page_no is not None:
                page_numbers.add(int(page_no))
        doc_items.append(item)

    origin = raw_meta.get("origin")
    captions = raw_meta.get("captions")
    page_list = sorted(page_numbers)
    content_labels = sorted(labels)

    return _strip_binary_blobs(
        {
            "schema_name": raw_meta.get("schema_name"),
            "version": raw_meta.get("version"),
            "doc_items": doc_items,
            "captions": captions,
            "origin": origin,
            "content_labels": content_labels,
            "page_numbers": page_list,
            "page_number": page_list[0] if page_list else None,
            "has_table": any(
                label in {"table", "document_index"} for label in content_labels
            ),
            "has_picture": "picture" in content_labels,
            "has_image": any(label in {"picture", "figure"} for label in content_labels),
            "self_refs": self_refs,
        }
    )


def _aggregate_docling_metadata(metas: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge Docling metadata from sibling chunks (parent tier grouping)."""
    if not metas:
        return {}
    if len(metas) == 1:
        return dict(metas[0])

    labels: set[str] = set()
    page_numbers: set[int] = set()
    doc_items: list[dict[str, Any]] = []
    self_refs: list[str] = []
    origin = None
    for meta in metas:
        labels.update(meta.get("content_labels") or [])
        page_numbers.update(meta.get("page_numbers") or [])
        doc_items.extend(meta.get("doc_items") or [])
        self_refs.extend(meta.get("self_refs") or [])
        if origin is None and meta.get("origin"):
            origin = meta.get("origin")

    page_list = sorted(page_numbers)
    content_labels = sorted(labels)
    return {
        "doc_items": doc_items,
        "origin": origin,
        "content_labels": content_labels,
        "page_numbers": page_list,
        "page_number": page_list[0] if page_list else None,
        "has_table": any(label in {"table", "document_index"} for label in content_labels),
        "has_picture": "picture" in content_labels,
        "has_image": any(label in {"picture", "figure"} for label in content_labels),
        "self_refs": self_refs,
        "aggregated_from_chunks": len(metas),
    }


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
    child_index: int | None = None
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
    ref_index: dict[str, tuple[str, dict[str, Any]]] | None = None,
) -> tuple[list[TierChunkRecord], dict[tuple[str, ...], int]]:
    groups: dict[tuple[str, ...], list[Any]] = defaultdict(list)
    for chunk in hybrid_chunks:
        groups[tuple(_heading_path(chunk))].append(chunk)

    parent_records: list[TierChunkRecord] = []
    heading_to_parent_index: dict[tuple[str, ...], int] = {}
    parent_index = 0
    for heading_path, chunks in groups.items():
        chunk_metas = [
            _extract_docling_chunk_metadata(chunk, ref_index=ref_index) for chunk in chunks
        ]
        combined = "\n\n".join(_chunk_text(chunk) for chunk in chunks if _chunk_text(chunk))
        for part_index, part_text in enumerate(
            _split_parent_text(combined, max_tokens=parent_max_tokens)
        ):
            contextual = part_text
            if heading_path:
                contextual = "\n".join(heading_path) + "\n\n" + part_text
            parent_metadata = {
                "part_index": part_index,
                "child_count": len(chunks),
                **_aggregate_docling_metadata(chunk_metas),
            }
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
                    metadata=parent_metadata,
                )
            )
            if part_index == 0:
                heading_to_parent_index[heading_path] = parent_index
            parent_index += 1
    return parent_records, heading_to_parent_index


def _normalize_ws(text: str) -> str:
    return " ".join((text or "").split())


def _token_overlap_ratio(micro_text: str, child_text: str) -> float:
    micro_tokens = set(_normalize_ws(micro_text).split())
    if not micro_tokens:
        return 0.0
    child_tokens = set(_normalize_ws(child_text).split())
    return len(micro_tokens & child_tokens) / len(micro_tokens)


def _resolve_micro_child_index(
    micro: TierChunkRecord,
    child_records: list[TierChunkRecord],
) -> int | None:
    """Map a micro tier to the child hybrid chunk that contains it (text overlap)."""
    micro_norm = _normalize_ws(micro.text)
    if not micro_norm:
        return None
    heading = tuple(micro.heading_path)
    pool = [child for child in child_records if tuple(child.heading_path) == heading]
    if not pool:
        pool = child_records
    for child in pool:
        child_norm = _normalize_ws(child.text)
        if micro_norm in child_norm:
            return child.chunk_index
    best_child: TierChunkRecord | None = None
    best_score = 0.0
    for child in pool:
        score = _token_overlap_ratio(micro.text, child.text)
        if score > best_score:
            best_score = score
            best_child = child
    if best_child is not None and best_score >= 0.5:
        return best_child.chunk_index
    return None


def _micro_records_with_child_index(
    micro_records: list[TierChunkRecord],
    child_records: list[TierChunkRecord],
) -> list[TierChunkRecord]:
    """Attach child_index so platform links micro --contained by-- child, not parent."""
    from dataclasses import replace

    out: list[TierChunkRecord] = []
    for micro in micro_records:
        child_index = _resolve_micro_child_index(micro, child_records)
        out.append(replace(micro, child_index=child_index))
    return out


def _hybrid_tier_records(
    hybrid_chunks: Iterator[Any],
    hybrid_chunker: Any,
    *,
    chunk_level: str,
    target_tokens: int,
    heading_to_parent_index: dict[tuple[str, ...], int],
    start_index: int,
    ref_index: dict[str, tuple[str, dict[str, Any]]] | None = None,
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
                metadata=_extract_docling_chunk_metadata(chunk, ref_index=ref_index),
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
    ref_index = _build_docling_ref_index(document)
    chunker = _make_hybrid_chunker(max_tokens)
    hybrid_chunks = list(chunker.chunk(dl_doc=document))
    records = _hybrid_tier_records(
        iter(hybrid_chunks),
        chunker,
        chunk_level="child",
        target_tokens=max_tokens,
        heading_to_parent_index={},
        start_index=0,
        ref_index=ref_index,
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
    ref_index = _build_docling_ref_index(document)

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
                metadata=_extract_docling_chunk_metadata(chunk, ref_index=ref_index),
            )
        )

    child_chunker = _make_hybrid_chunker(child_tokens)
    child_hybrid = list(child_chunker.chunk(dl_doc=document))
    parent_records, heading_to_parent_index = _build_parent_records(
        child_hybrid,
        child_chunker,
        parent_max_tokens=parent_max_tokens,
        ref_index=ref_index,
    )

    micro_chunker = _make_hybrid_chunker(micro_tokens)
    child_records = _hybrid_tier_records(
        iter(child_hybrid),
        child_chunker,
        chunk_level="child",
        target_tokens=child_tokens,
        heading_to_parent_index=heading_to_parent_index,
        start_index=0,
        ref_index=ref_index,
    )
    micro_records = _micro_records_with_child_index(
        _hybrid_tier_records(
            micro_chunker.chunk(dl_doc=document),
            micro_chunker,
            chunk_level="micro",
            target_tokens=micro_tokens,
            heading_to_parent_index=heading_to_parent_index,
            start_index=0,
            ref_index=ref_index,
        ),
        child_records,
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
