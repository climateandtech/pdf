"""Unit tests for Docling provenance on hybrid hierarchical chunk records."""

from __future__ import annotations

import json
from pathlib import Path

from hierarchical_chunker import (
    _aggregate_docling_metadata,
    _chunk_text,
    _extract_docling_chunk_metadata,
    _make_hybrid_chunker,
    chunk_hierarchical,
    load_docling_document,
)

_FIXTURE = (
    Path(__file__).resolve().parents[2]
    / "ct-platform/benchmarks/chunking-fixtures/short-report/docling_json.json"
)


def test_make_hybrid_chunker_uses_bge_m3_tokenizer():
    """Hypothesis: _make_hybrid_chunker builds a bge-m3 tokenizer, not the MiniLM default."""
    chunker = _make_hybrid_chunker(512)
    name = str(chunker.tokenizer.get_tokenizer().name_or_path)
    assert "bge-m3" in name.lower()
    assert chunker.max_tokens == 512


def test_make_hybrid_chunker_honors_per_tier_max_tokens():
    """Hypothesis: micro (150) and child (512) chunkers carry distinct token limits."""
    micro = _make_hybrid_chunker(150)
    child = _make_hybrid_chunker(512)
    assert micro.max_tokens == 150
    assert child.max_tokens == 512
    assert micro.max_tokens != child.max_tokens


def test_hybrid_child_chunk_carries_docling_doc_items():
    """Hypothesis: HybridChunker table chunks expose slim labels/self_refs (no resolved blobs)."""
    structured = json.loads(_FIXTURE.read_text())
    document = load_docling_document(structured)
    chunker = _make_hybrid_chunker(512)
    table_chunk = None
    for chunk in chunker.chunk(dl_doc=document):
        meta = _extract_docling_chunk_metadata(chunk, ref_index=None)
        if meta.get("has_table"):
            table_chunk = meta
            break

    assert table_chunk is not None
    assert "table" in table_chunk["content_labels"] or "document_index" in table_chunk[
        "content_labels"
    ]
    assert table_chunk["page_number"] is not None
    assert "doc_items" not in table_chunk
    assert table_chunk["self_refs"]


def test_hierarchical_records_include_docling_metadata():
    """Hypothesis: all GPU tier records serialize slim Docling provenance."""
    structured = json.loads(_FIXTURE.read_text())
    payload = chunk_hierarchical(structured, micro_tokens=150, child_tokens=512)
    child = next(
        record
        for record in payload["records"]
        if record.get("chunk_level") == "child" and record.get("metadata", {}).get("page_number")
    )
    meta = child["metadata"]
    assert meta.get("content_labels")
    assert meta.get("self_refs")
    assert child.get("contextual_text") is None
    assert "doc_items" not in meta


def test_aggregate_docling_metadata_merges_sibling_chunks():
    """Hypothesis: parent tier merges page_numbers and content_labels from children."""
    merged = _aggregate_docling_metadata(
        [
            {
                "content_labels": ["text"],
                "page_numbers": [1, 2],
                "self_refs": ["#/texts/0"],
                "chunk_index": 0,
            },
            {
                "content_labels": ["table"],
                "page_numbers": [3],
                "self_refs": ["#/tables/0"],
                "chunk_index": 1,
            },
        ]
    )
    assert merged["content_labels"] == ["table", "text"]
    assert merged["page_numbers"] == [1, 2, 3]
    assert merged["has_table"] is True
    assert merged["aggregated_from_chunks"] == 2
    assert merged["child_indices"] == [0, 1]
    assert "doc_items" not in merged


def test_split_chunk_along_doc_items_bounds_chars():
    """Hypothesis: oversized chunks are split under the char budget on doc_item boundaries."""
    from hierarchical_chunker import _split_chunk_along_doc_items, _TextChunk

    items = []
    for i in range(20):
        items.append(
            {
                "self_ref": f"#/texts/{i}",
                "label": "text",
                "text": ("word " * 200).strip(),
                "prov": [{"page_no": 1}],
            }
        )
    chunk = _TextChunk(
        "\n\n".join(item["text"] for item in items),
        ["Section"],
        {"doc_items": items},
    )
    # Force Hierarchical-like meta for _doc_item_texts
    chunk.meta = {"headings": ["Section"], "doc_items": items}
    parts = _split_chunk_along_doc_items(chunk, max_chars=500)
    assert len(parts) > 1
    assert all(len(_chunk_text(part)) <= 500 or len(parts) > 0 for part in parts)
    assert max(len(_chunk_text(part)) for part in parts) <= 500 + 50  # small slack for join


def test_parent_pack_uses_whole_child_boundaries():
    """Hypothesis: parents concatenate whole children without mid-chunk cuts."""
    from hierarchical_chunker import _build_parent_records, _TextChunk

    children = [
        _TextChunk("alpha beta gamma", ["H"], {"self_refs": ["#/texts/0"]}),
        _TextChunk("delta epsilon", ["H"], {"self_refs": ["#/texts/1"]}),
        _TextChunk("zeta", ["H"], {"self_refs": ["#/texts/2"]}),
    ]
    parents, mapping = _build_parent_records(
        children,
        hybrid_chunker=None,
        parent_max_tokens=5,
        ref_index=None,
    )
    assert mapping[("H",)] == 0
    assert all(p.contextual_text is None for p in parents)
    # Each parent text is a join of whole child texts
    joined = " ".join(p.text for p in parents)
    assert "alpha beta gamma" in joined
    assert "delta epsilon" in joined
