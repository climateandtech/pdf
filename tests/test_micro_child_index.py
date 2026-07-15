"""Unit tests for micro→child index matching without Docling GPU deps."""

from __future__ import annotations

from hierarchical_chunker import (
    TierChunkRecord,
    _micro_records_with_child_index,
)


def test_micro_child_index_prefers_same_heading_pool():
    """Hypothesis: micro rows under a heading only scan that heading's children."""
    children = [
        TierChunkRecord(
            chunk_index=0,
            chunk_level="child",
            target_tokens=512,
            text="alpha beta gamma delta",
            contextual_text=None,
            heading_path=["A"],
            token_count=4,
            embed=True,
        ),
        TierChunkRecord(
            chunk_index=1,
            chunk_level="child",
            target_tokens=512,
            text="omega unique other",
            contextual_text=None,
            heading_path=["B"],
            token_count=3,
            embed=True,
        ),
    ]
    micros = [
        TierChunkRecord(
            chunk_index=0,
            chunk_level="micro",
            target_tokens=150,
            text="alpha beta",
            contextual_text=None,
            heading_path=["A"],
            token_count=2,
            embed=True,
        )
    ]
    out = _micro_records_with_child_index(micros, children)
    assert out[0].child_index == 0
