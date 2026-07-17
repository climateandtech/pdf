"""Runtime pin: semchunk must match GPU HybridChunker API (3.x callable Chunker)."""

from __future__ import annotations

import importlib.metadata

import pytest
from packaging.version import Version

pytestmark = pytest.mark.unit


def test_semchunk_version_aligned_with_gpu_and_docling_chunking() -> None:
    """Laptop/CI must not drift to semchunk 2.x while GPU runs 3.x.

    docling-core[chunking] allows ``>=2.2,<4``; we pin ``>=3.2.5,<4`` in
    requirements.txt so call-site behavior is consistent.
    """
    ver = Version(importlib.metadata.version("semchunk"))
    assert ver >= Version("3.2.5"), f"semchunk {ver} too old; pip install -r requirements.txt"
    assert ver < Version("4.0.0"), f"semchunk {ver} breaks docling-core[chunking] (<4)"


def test_semchunk_chunkerify_returns_callable_without_requiring_chunk_method() -> None:
    """GPU 3.2.5 contract: invoke Chunker via call; .chunk may be absent."""
    semchunk = pytest.importorskip("semchunk")
    chunker = semchunk.chunkerify(lambda text: len(text.split()), chunk_size=8)
    assert callable(chunker)
    parts = chunker(" ".join(f"w{i}" for i in range(20)))
    assert isinstance(parts, list)
    assert parts
    assert all(isinstance(p, str) for p in parts)
