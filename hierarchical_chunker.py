"""Hierarchical multi-tier chunking via Docling HybridChunker (CPU chunk worker)."""

from __future__ import annotations

import os
import re
import time
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from typing import Any, Iterator

# Bump when splitting/bounding semantics change. Results are stamped with this
# so docs.chunk jobs can request a re-chunk of stale S3 results
# (min_chunker_version); unstamped legacy results count as version 0.
CHUNKER_VERSION = 2

DEFAULT_MICRO_TOKENS = int(os.getenv("CHUNK_MICRO_TARGET_TOKENS", "150"))
DEFAULT_CHILD_TOKENS = int(os.getenv("CHUNK_TARGET_TOKENS", "512"))
DEFAULT_PARENT_MAX_TOKENS = int(os.getenv("CHUNK_PARENT_MAX_TOKENS", "2000"))
DEFAULT_MICRO_OVERLAP = int(os.getenv("CHUNK_MICRO_TOKEN_OVERLAP", "32"))
DEFAULT_CHUNK_TOKENIZER_MODEL = os.getenv("CHUNK_TOKENIZER_MODEL", "BAAI/bge-m3")
# Char budget before calling HuggingFace tokenizer / semchunk (avoids 1.7M-token hangs).
DEFAULT_SAFE_TOKENIZE_CHARS = int(os.getenv("CHUNK_SAFE_TOKENIZE_CHARS", "32000"))


def _chunk_tokenizer_require_cuda() -> bool:
    """Whether bge-m3 chunk tokenizer load requires a CUDA-capable GPU host."""
    raw = (os.getenv("CHUNK_TOKENIZER_REQUIRE_CUDA") or "1").strip().lower()
    return raw in {"1", "true", "yes", "on"}


def _ensure_chunk_tokenizer_cuda() -> None:
    """Fail fast when production chunk worker is not on a GPU host."""
    if not _chunk_tokenizer_require_cuda():
        return
    import torch

    if not torch.cuda.is_available():
        msg = (
            "CHUNK_TOKENIZER_REQUIRE_CUDA=1 but torch.cuda.is_available() is False; "
            "chunk worker must run on the GPU host (smoldocling)"
        )
        raise RuntimeError(msg)


def warmup_chunk_tokenizer(*, max_tokens: int = DEFAULT_CHILD_TOKENS) -> str:
    """Preload bge-m3 tokenizer on GPU host startup (vocabulary only, no embed)."""
    _ensure_chunk_tokenizer_cuda()
    chunker = _make_hybrid_chunker(max_tokens)
    model_name = str(chunker.tokenizer.get_tokenizer().name_or_path)
    return model_name


def approx_token_count(text: str) -> int:
    """Whitespace token proxy aligned with platform chunking_config."""
    return len(re.findall(r"\S+", text or ""))


def _token_count(text: str, *, chunker: Any | None = None) -> int:
    """Count tokens with the active HybridChunker tokenizer (bge-m3 vocab)."""
    if not text:
        return 0
    if chunker is not None:
        return int(chunker.tokenizer.count_tokens(text=text))
    return approx_token_count(text)


_RESOLVED_REF_FIELDS = ()  # legacy name retained; resolved detail stays on S3


class _TextChunk:
    """Lightweight chunk stand-in after Docling-native doc_item pre-splits."""

    def __init__(self, text: str, heading_path: list[str], metadata: dict[str, Any]) -> None:
        self.text = text
        self.meta = {"headings": heading_path, **metadata}


def _doc_item_texts(chunk: Any) -> list[tuple[str, dict[str, Any]]]:
    """Extract per-doc_item text segments from a HierarchicalChunker chunk."""
    meta = getattr(chunk, "meta", None)
    raw_meta = _docling_model_dump(meta) if meta is not None else {}
    if not isinstance(raw_meta, dict):
        return []
    out: list[tuple[str, dict[str, Any]]] = []
    for raw_item in raw_meta.get("doc_items") or []:
        if not isinstance(raw_item, dict):
            continue
        text = str(raw_item.get("text") or "").strip()
        # Prov-only refs often lack inline text; skip empty items.
        if not text:
            continue
        out.append((text, _slim_item_fields(raw_item)))
    return out


def _split_chunk_along_doc_items(
    chunk: Any,
    *,
    max_chars: int,
) -> list[Any]:
    """Split an oversized HierarchicalChunker chunk on Docling doc_item boundaries."""
    heading = _heading_path(chunk)
    items = _doc_item_texts(chunk)
    if len(items) <= 1:
        # Single item or no item texts: fall back to paragraph boundaries (still structural).
        text = _chunk_text(chunk)
        if len(text) <= max_chars:
            return [chunk]
        parts = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
        if len(parts) <= 1:
            # Last resort: fixed windows so tokenizer never sees mega strings.
            windows = []
            for i in range(0, len(text), max_chars):
                part = text[i : i + max_chars]
                if part:
                    windows.append(part)
            return [
                _TextChunk(part, heading, _extract_docling_chunk_metadata(chunk))
                for part in windows
            ]
        bounded_parts: list[str] = []
        for part in parts:
            if len(part) > max_chars:
                # A single paragraph can still exceed the char budget.
                bounded_parts.extend(
                    part[i : i + max_chars] for i in range(0, len(part), max_chars)
                )
            else:
                bounded_parts.append(part)
        return [
            _TextChunk(part, heading, _extract_docling_chunk_metadata(chunk))
            for part in bounded_parts
        ]

    segments: list[Any] = []
    batch_texts: list[str] = []
    batch_refs: list[str] = []
    batch_labels: set[str] = set()
    batch_pages: set[int] = set()
    batch_len = 0

    def flush() -> None:
        nonlocal batch_texts, batch_refs, batch_labels, batch_pages, batch_len
        if not batch_texts:
            return
        page_list = sorted(batch_pages)
        labels = sorted(batch_labels)
        meta = {
            "content_labels": labels,
            "page_numbers": page_list,
            "page_number": page_list[0] if page_list else None,
            "has_table": any(label in {"table", "document_index"} for label in labels),
            "has_picture": "picture" in labels,
            "has_image": any(label in {"picture", "figure"} for label in labels),
            "self_refs": list(batch_refs),
        }
        segments.append(_TextChunk("\n\n".join(batch_texts), heading, meta))
        batch_texts = []
        batch_refs = []
        batch_labels = set()
        batch_pages = set()
        batch_len = 0

    for text, item in items:
        if len(text) > max_chars:
            flush()
            for i in range(0, len(text), max_chars):
                part = text[i : i + max_chars]
                if not part:
                    continue
                page_list = sorted(item.get("page_numbers") or [])
                label = str(item.get("label") or "")
                labels = [label] if label else []
                meta = {
                    "content_labels": labels,
                    "page_numbers": page_list,
                    "page_number": page_list[0] if page_list else None,
                    "has_table": label in {"table", "document_index"},
                    "has_picture": label == "picture",
                    "has_image": label in {"picture", "figure"},
                    "self_refs": [str(item["self_ref"])] if item.get("self_ref") else [],
                }
                segments.append(_TextChunk(part, heading, meta))
            continue
        if batch_texts and batch_len + len(text) + 2 > max_chars:
            flush()
        batch_texts.append(text)
        if item.get("self_ref"):
            batch_refs.append(str(item["self_ref"]))
        if item.get("label"):
            batch_labels.add(str(item["label"]))
        batch_pages.update(item.get("page_numbers") or [])
        batch_len += len(text) + 2
    flush()
    return segments or [chunk]


def _iter_size_safe_hierarchical_chunks(
    document: Any,
    *,
    max_chars: int = DEFAULT_SAFE_TOKENIZE_CHARS,
) -> Iterator[Any]:
    """HierarchicalChunker output with Docling-native pre-split of oversized chunks."""
    from docling.chunking import HierarchicalChunker

    for chunk in HierarchicalChunker().chunk(dl_doc=document):
        text = _chunk_text(chunk)
        if len(text) <= max_chars:
            yield chunk
            continue
        yield from _split_chunk_along_doc_items(chunk, max_chars=max_chars)


def _split_text_token_windows(text: str, *, chunker: Any, max_tokens: int) -> list[str]:
    """Hard token-id windowing — guaranteed ≤ max_tokens per window when a real tokenizer exists."""
    budget = max(1, int(max_tokens))
    tokenizer = None
    get_tokenizer = getattr(getattr(chunker, "tokenizer", None), "get_tokenizer", None)
    if callable(get_tokenizer):
        try:
            tokenizer = get_tokenizer()
        except (TypeError, ValueError, RuntimeError, OSError):
            tokenizer = None
    if tokenizer is not None and hasattr(tokenizer, "encode") and hasattr(tokenizer, "decode"):
        try:
            token_ids = tokenizer.encode(text, add_special_tokens=False)
            windows = []
            for i in range(0, len(token_ids), budget):
                part = tokenizer.decode(
                    token_ids[i : i + budget], skip_special_tokens=True
                ).strip()
                if part:
                    windows.append(part)
            if windows:
                return windows
        except (TypeError, ValueError, RuntimeError):
            pass
    # No usable tokenizer: word windows sized conservatively (words ≥ tokens is
    # not guaranteed for bge-m3, so halve the budget instead of inflating it).
    words = re.findall(r"\S+", text)
    step = max(8, budget // 2)
    return [" ".join(words[i : i + step]) for i in range(0, len(words), step)] or [text]


def _split_text_token_aligned(text: str, *, chunker: Any, max_tokens: int) -> list[str]:
    """Token-budgeted text split via semchunk (the mechanism Docling's HybridChunker uses)."""
    budget = max(1, int(max_tokens))
    try:
        import semchunk  # deferred: required in production, optional for unit tests

        def _counter(value: str) -> int:
            return _token_count(value, chunker=chunker)

        sem_chunker = semchunk.chunkerify(_counter, chunk_size=budget)
        # semchunk>=newer returns a callable Chunker; older APIs used .chunk().
        if callable(sem_chunker):
            raw = sem_chunker(text)
        else:
            raw = sem_chunker.chunk(text)
        segments = [s.strip() for s in (raw or []) if s and str(s).strip()]
        if segments:
            return segments
    except (ImportError, TypeError, ValueError, RuntimeError, AttributeError):
        pass
    return _split_text_token_windows(text, chunker=chunker, max_tokens=budget)


def _split_piece_token_aligned(piece: Any, *, chunker: Any, max_tokens: int) -> list[Any]:
    """Split one token-oversized (char-safe) chunk into ≤ max_tokens pieces.

    Prefers Docling's own ``HybridChunker._split_using_plain_text`` (semchunk with
    heading/caption headroom) for genuine DocChunks; pre-split ``_TextChunk``
    instances go through semchunk / token windows directly with heading headroom
    reserved for embed-time contextual text.
    """
    heading = _heading_path(piece)
    meta = getattr(piece, "meta", None)
    docling_split = getattr(chunker, "_split_using_plain_text", None)
    if callable(docling_split) and not isinstance(meta, dict):
        try:
            parts = [p for p in docling_split(piece) if _chunk_text(p)]
        except (TypeError, ValueError, AttributeError, RuntimeError):
            parts = []
        if parts and all(
            _token_count(_chunk_text(p), chunker=chunker) <= max_tokens for p in parts
        ):
            return parts
    base_meta = meta if isinstance(meta, dict) else _extract_docling_chunk_metadata(piece)
    base_meta = {k: v for k, v in (base_meta or {}).items() if k != "headings"}
    heading_overhead = (
        _token_count("\n".join(heading) + "\n\n", chunker=chunker) if heading else 0
    )
    # Keep a minimum body budget when headings eat the whole allowance, but
    # never exceed max_tokens itself.
    floor = max(1, min(16, int(max_tokens)))
    budget = max(floor, int(max_tokens) - heading_overhead)
    out: list[Any] = []
    for segment in _split_text_token_aligned(
        _chunk_text(piece), chunker=chunker, max_tokens=budget
    ):
        if _token_count(segment, chunker=chunker) > budget:
            out.extend(
                _TextChunk(window, heading, dict(base_meta))
                for window in _split_text_token_windows(
                    segment, chunker=chunker, max_tokens=budget
                )
            )
        else:
            out.append(_TextChunk(segment, heading, dict(base_meta)))
    return out


def _hybrid_chunks_bounded(document: Any, chunker: Any) -> list[Any]:
    """Hybrid-style chunking that never HF-tokenizes mega strings.

    Char-oversized chunks split along Docling doc_items first (tokenizer safety
    only), then *every* piece is bounded by the real tokenizer budget
    (max_tokens) via the Docling-native plain-text splitter. Windows align with
    the token limit — the 32k char safety cap never leaks into output sizes.
    """
    max_tokens = int(getattr(chunker, "max_tokens", DEFAULT_CHILD_TOKENS) or DEFAULT_CHILD_TOKENS)
    max_chars = max(DEFAULT_SAFE_TOKENIZE_CHARS, max_tokens * 4)

    queue: deque[Any] = deque(_iter_size_safe_hierarchical_chunks(document, max_chars=max_chars))
    out: list[Any] = []
    while queue:
        piece = queue.popleft()
        text = _chunk_text(piece)
        if not text:
            continue
        if len(text) > max_chars:
            segments = _split_chunk_along_doc_items(piece, max_chars=max_chars)
            if len(segments) == 1 and segments[0] is piece:
                # Structural split made no progress: hard char windows so the
                # tokenizer never sees a mega string.
                heading = _heading_path(piece)
                piece_meta = _extract_docling_chunk_metadata(piece)
                segments = [
                    _TextChunk(text[i : i + max_chars], heading, dict(piece_meta))
                    for i in range(0, len(text), max_chars)
                ]
            queue.extendleft(reversed(segments))
            continue
        if _token_count(text, chunker=chunker) <= max_tokens:
            out.append(piece)
            continue
        out.extend(_split_piece_token_aligned(piece, chunker=chunker, max_tokens=max_tokens))
    return out


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
    """Map Docling self_ref to (collection, item dict) for optional on-demand joins."""
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


def derive_contextual_text(text: str, heading_path: list[str] | None) -> str:
    """Build contextual embed text from heading path + body (not stored on records)."""
    body = (text or "").strip()
    headings = [str(item) for item in (heading_path or []) if str(item).strip()]
    if headings and body:
        return "\n".join(headings) + "\n\n" + body
    return body


def _slim_item_fields(item: dict[str, Any]) -> dict[str, Any]:
    """Keep only lightweight provenance; resolved detail stays in docling.json."""
    out: dict[str, Any] = {}
    for key in ("self_ref", "label"):
        if item.get(key) is not None:
            out[key] = item.get(key)
    pages: list[int] = []
    for prov in item.get("prov") or []:
        if isinstance(prov, dict) and prov.get("page_no") is not None:
            pages.append(int(prov["page_no"]))
    if pages:
        out["page_numbers"] = sorted(set(pages))
    return out


def _extract_docling_chunk_metadata(
    chunk: Any,
    *,
    ref_index: dict[str, tuple[str, dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Capture slim HybridChunker/HierarchicalChunker meta (no resolved doc_items blobs)."""
    del ref_index  # kept for call-site compatibility; join via self_ref on demand
    meta = getattr(chunk, "meta", None)
    if meta is None:
        return {}

    # Already-slim TextChunk metadata from doc_item pre-split.
    if isinstance(meta, dict) and (
        "self_refs" in meta or "content_labels" in meta or "page_numbers" in meta
    ):
        page_list = list(meta.get("page_numbers") or [])
        content_labels = list(meta.get("content_labels") or [])
        return {
            "content_labels": content_labels,
            "page_numbers": page_list,
            "page_number": meta.get("page_number") or (page_list[0] if page_list else None),
            "has_table": bool(meta.get("has_table"))
            or any(label in {"table", "document_index"} for label in content_labels),
            "has_picture": bool(meta.get("has_picture")) or "picture" in content_labels,
            "has_image": bool(meta.get("has_image"))
            or any(label in {"picture", "figure"} for label in content_labels),
            "self_refs": list(meta.get("self_refs") or []),
        }

    raw_meta = _docling_model_dump(meta)
    if not isinstance(raw_meta, dict):
        return {}

    labels: set[str] = set()
    page_numbers: set[int] = set()
    self_refs: list[str] = []

    for raw_item in raw_meta.get("doc_items") or []:
        if not isinstance(raw_item, dict):
            continue
        item = _slim_item_fields(raw_item)
        label = str(item.get("label") or "")
        if label:
            labels.add(label)
        ref = item.get("self_ref")
        if ref:
            self_refs.append(str(ref))
        page_numbers.update(item.get("page_numbers") or [])

    page_list = sorted(page_numbers)
    content_labels = sorted(labels)

    return {
        "content_labels": content_labels,
        "page_numbers": page_list,
        "page_number": page_list[0] if page_list else None,
        "has_table": any(label in {"table", "document_index"} for label in content_labels),
        "has_picture": "picture" in content_labels,
        "has_image": any(label in {"picture", "figure"} for label in content_labels),
        "self_refs": self_refs,
    }


def _aggregate_docling_metadata(metas: list[dict[str, Any]]) -> dict[str, Any]:
    """Merge slim Docling metadata from sibling chunks (parent tier grouping)."""
    if not metas:
        return {}
    if len(metas) == 1:
        slim = dict(metas[0])
        slim.pop("doc_items", None)
        return slim

    labels: set[str] = set()
    page_numbers: set[int] = set()
    self_refs: list[str] = []
    for meta in metas:
        labels.update(meta.get("content_labels") or [])
        page_numbers.update(meta.get("page_numbers") or [])
        self_refs.extend(meta.get("self_refs") or [])

    page_list = sorted(page_numbers)
    content_labels = sorted(labels)
    return {
        "content_labels": content_labels,
        "page_numbers": page_list,
        "page_number": page_list[0] if page_list else None,
        "has_table": any(label in {"table", "document_index"} for label in content_labels),
        "has_picture": "picture" in content_labels,
        "has_image": any(label in {"picture", "figure"} for label in content_labels),
        "self_refs": self_refs,
        "aggregated_from_chunks": len(metas),
        "child_indices": [
            meta.get("chunk_index") for meta in metas if meta.get("chunk_index") is not None
        ],
    }


def load_docling_document(structured_data: dict[str, Any]) -> Any:
    """Deserialize stored GPU Docling JSON into a DoclingDocument."""
    from docling.datamodel.document import DoclingDocument

    payload = dict(structured_data or {})
    payload.pop("platform_hierarchical_chunks", None)
    payload.pop("__platform_hierarchical_chunks__", None)
    return DoclingDocument.model_validate(payload)


def _make_hybrid_chunker(max_tokens: int) -> Any:
    """Build a HybridChunker sized for Ollama bge-m3 embed (same tokenizer vocab)."""
    from docling.chunking import HybridChunker
    from docling_core.transforms.chunker.tokenizer.huggingface import HuggingFaceTokenizer

    _ensure_chunk_tokenizer_cuda()
    tokenizer = HuggingFaceTokenizer.from_pretrained(
        model_name=DEFAULT_CHUNK_TOKENIZER_MODEL,
        max_tokens=max_tokens,
    )
    return HybridChunker(
        tokenizer=tokenizer,
        max_tokens=max_tokens,
        merge_peers=True,
    )


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


def _build_parent_records(
    hybrid_chunks: list[Any],
    hybrid_chunker: Any,
    *,
    parent_max_tokens: int,
    ref_index: dict[str, tuple[str, dict[str, Any]]] | None = None,
) -> tuple[list[TierChunkRecord], dict[tuple[str, ...], int]]:
    """Build parents by packing whole Docling child chunks (no mid-chunk whitespace cuts)."""
    del hybrid_chunker  # kept for call-site compatibility; packing uses approx tokens
    groups: dict[tuple[str, ...], list[Any]] = defaultdict(list)
    for chunk in hybrid_chunks:
        groups[tuple(_heading_path(chunk))].append(chunk)

    parent_records: list[TierChunkRecord] = []
    heading_to_parent_index: dict[tuple[str, ...], int] = {}
    parent_index = 0
    # Global index aligned with child tier chunk_index (enumerate order of hybrid_chunks).
    chunk_global_index = {id(chunk): idx for idx, chunk in enumerate(hybrid_chunks)}
    for heading_path, chunks in groups.items():
        batches: list[list[Any]] = []
        current: list[Any] = []
        current_tokens = 0
        for chunk in chunks:
            text = _chunk_text(chunk)
            if not text:
                continue
            # Whitespace proxy keeps parent packing cheap and HF-free.
            tc = approx_token_count(text)
            if current and current_tokens + tc > parent_max_tokens:
                batches.append(current)
                current = []
                current_tokens = 0
            current.append(chunk)
            current_tokens += tc
        if current:
            batches.append(current)

        for part_index, batch in enumerate(batches):
            child_indices: list[int] = []
            texts: list[str] = []
            metas: list[dict[str, Any]] = []
            for chunk in batch:
                texts.append(_chunk_text(chunk))
                global_idx = chunk_global_index.get(id(chunk))
                meta = _extract_docling_chunk_metadata(chunk, ref_index=ref_index)
                if global_idx is not None:
                    meta = {**meta, "chunk_index": global_idx}
                    child_indices.append(global_idx)
                metas.append(meta)
            part_text = "\n\n".join(texts)
            parent_metadata = {
                "part_index": part_index,
                "child_count": len(batch),
                "child_indices": child_indices,
                **_aggregate_docling_metadata(metas),
            }
            parent_records.append(
                TierChunkRecord(
                    chunk_index=parent_index,
                    chunk_level="parent",
                    target_tokens=parent_max_tokens,
                    text=part_text,
                    contextual_text=None,
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


def _children_by_heading(
    child_records: list[TierChunkRecord],
) -> dict[tuple[str, ...], list[TierChunkRecord]]:
    groups: dict[tuple[str, ...], list[TierChunkRecord]] = defaultdict(list)
    for child in child_records:
        groups[tuple(child.heading_path)].append(child)
    return groups


def _resolve_micro_child_index(
    micro: TierChunkRecord,
    child_records: list[TierChunkRecord],
    *,
    by_heading: dict[tuple[str, ...], list[TierChunkRecord]] | None = None,
    max_candidates: int = 64,
) -> int | None:
    """Map a micro tier to the child hybrid chunk that contains it (text overlap)."""
    micro_norm = _normalize_ws(micro.text)
    if not micro_norm:
        return None
    heading = tuple(micro.heading_path)
    groups = by_heading if by_heading is not None else _children_by_heading(child_records)
    pool = groups.get(heading) or child_records
    if len(pool) > max_candidates:
        pool = pool[:max_candidates]
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

    by_heading = _children_by_heading(child_records)
    out: list[TierChunkRecord] = []
    for micro in micro_records:
        child_index = _resolve_micro_child_index(
            micro,
            child_records,
            by_heading=by_heading,
        )
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
        # contextual_text is derived at embed time from heading_path + text (not stored).
        if len(text) <= DEFAULT_SAFE_TOKENIZE_CHARS:
            token_count = _token_count(text, chunker=hybrid_chunker)
        else:
            token_count = approx_token_count(text)
        records.append(
            TierChunkRecord(
                chunk_index=start_index + offset,
                chunk_level=chunk_level,
                target_tokens=target_tokens,
                text=text,
                contextual_text=None,
                heading_path=heading_path,
                token_count=token_count,
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
    chunker = _make_hybrid_chunker(max_tokens)
    hybrid_chunks = _hybrid_chunks_bounded(document, chunker)
    records = _hybrid_tier_records(
        iter(hybrid_chunks),
        chunker,
        chunk_level="child",
        target_tokens=max_tokens,
        heading_to_parent_index={},
        start_index=0,
        ref_index=None,
    )
    elapsed_s = time.perf_counter() - started
    payload_records = [record.to_dict() for record in records]
    token_counts = [record.token_count for record in records]
    return {
        "records": payload_records,
        "tier_counts": {"child": len(records)},
        "chunker_version": CHUNKER_VERSION,
        "metrics": {
            "chunk_wall_time_s": round(elapsed_s, 3),
            "embed_vector_count": len(records),
            "storage_text_bytes": sum(len(record.text) for record in records),
            "max_tokens": max_tokens,
            "chunker_version": CHUNKER_VERSION,
            "avg_chunk_tokens": (
                round(sum(token_counts) / len(token_counts), 1) if token_counts else 0.0
            ),
            "p95_chunk_tokens": (
                (
                    sorted(token_counts)[max(0, int(0.95 * len(token_counts)) - 1)]
                    if token_counts
                    else 0
                )
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
    started = time.perf_counter()
    document = load_docling_document(structured_data)

    child_chunker = _make_hybrid_chunker(child_tokens)
    element_records: list[TierChunkRecord] = []
    for index, chunk in enumerate(_iter_size_safe_hierarchical_chunks(document)):
        text = _chunk_text(chunk)
        if not text:
            continue
        if len(text) <= DEFAULT_SAFE_TOKENIZE_CHARS:
            token_count = _token_count(text, chunker=child_chunker)
        else:
            token_count = approx_token_count(text)
        element_records.append(
            TierChunkRecord(
                chunk_index=index,
                chunk_level="element",
                target_tokens=None,
                text=text,
                contextual_text=None,
                heading_path=_heading_path(chunk),
                token_count=token_count,
                embed=False,
                metadata=_extract_docling_chunk_metadata(chunk, ref_index=None),
            )
        )

    child_hybrid = _hybrid_chunks_bounded(document, child_chunker)
    parent_records, heading_to_parent_index = _build_parent_records(
        child_hybrid,
        child_chunker,
        parent_max_tokens=parent_max_tokens,
        ref_index=None,
    )

    micro_chunker = _make_hybrid_chunker(micro_tokens)
    child_records = _hybrid_tier_records(
        iter(child_hybrid),
        child_chunker,
        chunk_level="child",
        target_tokens=child_tokens,
        heading_to_parent_index=heading_to_parent_index,
        start_index=0,
        ref_index=None,
    )
    micro_records = _micro_records_with_child_index(
        _hybrid_tier_records(
            iter(_hybrid_chunks_bounded(document, micro_chunker)),
            micro_chunker,
            chunk_level="micro",
            target_tokens=micro_tokens,
            heading_to_parent_index=heading_to_parent_index,
            start_index=0,
            ref_index=None,
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

    return {
        "records": all_records,
        "tier_counts": tier_counts,
        "chunker_version": CHUNKER_VERSION,
        "metrics": {
            "chunk_wall_time_s": round(elapsed_s, 3),
            "embed_vector_count": len(searchable),
            "storage_text_bytes": storage_bytes,
            "micro_tokens": micro_tokens,
            "child_tokens": child_tokens,
            "parent_max_tokens": parent_max_tokens,
            "chunker_version": CHUNKER_VERSION,
        },
    }
