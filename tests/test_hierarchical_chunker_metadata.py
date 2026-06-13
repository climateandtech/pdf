"""Unit tests for Docling provenance on hybrid hierarchical chunk records."""

from __future__ import annotations

import json
from pathlib import Path

from hierarchical_chunker import (
    _aggregate_docling_metadata,
    _build_docling_ref_index,
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
    """Hypothesis: HybridChunker meta.doc_items land on child tier metadata."""
    structured = json.loads(_FIXTURE.read_text())
    document = load_docling_document(structured)
    ref_index = _build_docling_ref_index(document)
    chunker = _make_hybrid_chunker(512)
    table_chunk = None
    for chunk in chunker.chunk(dl_doc=document):
        meta = _extract_docling_chunk_metadata(chunk, ref_index=ref_index)
        if meta.get("has_table"):
            table_chunk = meta
            break

    assert table_chunk is not None
    assert "table" in table_chunk["content_labels"] or "document_index" in table_chunk[
        "content_labels"
    ]
    assert table_chunk["page_number"] is not None
    assert table_chunk["doc_items"]
    table_items = [
        item
        for item in table_chunk["doc_items"]
        if (item.get("resolved") or {}).get("collection") == "tables"
    ]
    assert table_items


def test_hierarchical_records_include_docling_metadata():
    """Hypothesis: all GPU tier records serialize Docling provenance."""
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


def test_aggregate_docling_metadata_merges_sibling_chunks():
    """Hypothesis: parent tier merges page_numbers and content_labels from children."""
    merged = _aggregate_docling_metadata(
        [
            {
                "content_labels": ["text"],
                "page_numbers": [1, 2],
                "doc_items": [{"self_ref": "#/texts/0"}],
                "self_refs": ["#/texts/0"],
            },
            {
                "content_labels": ["table"],
                "page_numbers": [3],
                "doc_items": [{"self_ref": "#/tables/0"}],
                "self_refs": ["#/tables/0"],
            },
        ]
    )
    assert merged["content_labels"] == ["table", "text"]
    assert merged["page_numbers"] == [1, 2, 3]
    assert merged["has_table"] is True
    assert merged["aggregated_from_chunks"] == 2
