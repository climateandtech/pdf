"""Unit: splitter windows must align with the token limit, never the char safety cap.

Regression tests for the embed context-overflow class of bugs: char-oversized
pieces used to bypass the token check entirely (32k-char ≈ 9k-16k token chunks),
and the word-window fallback misused ``max_tokens * 4`` (a chars-per-token
heuristic) as a word count.
"""

from __future__ import annotations

import re
from types import SimpleNamespace

import pytest

import hierarchical_chunker as hc
from hierarchical_chunker import (
    CHUNKER_VERSION,
    _hybrid_chunks_bounded,
    _split_chunk_along_doc_items,
    _split_piece_token_aligned,
    _split_text_token_aligned,
    _split_text_token_windows,
    _TextChunk,
)


class _WordTokenizer:
    """Whitespace tokenizer stand-in (1 word == 1 token, no get_tokenizer)."""

    def count_tokens(self, *, text: str) -> int:
        return len(re.findall(r"\S+", text or ""))


class _FakeChunker:
    def __init__(self, max_tokens: int) -> None:
        self.max_tokens = max_tokens
        self.tokenizer = _WordTokenizer()


def _tokens(chunker: _FakeChunker, text: str) -> int:
    return chunker.tokenizer.count_tokens(text=text)


def test_chunker_version_is_stamped_at_least_two() -> None:
    assert CHUNKER_VERSION >= 2


class TestSplitTextTokenAligned:
    def test_all_segments_fit_token_budget(self) -> None:
        chunker = _FakeChunker(max_tokens=10)
        text = " ".join(f"word{i}" for i in range(137))

        segments = _split_text_token_aligned(text, chunker=chunker, max_tokens=16)

        assert segments
        assert all(_tokens(chunker, s) <= 16 for s in segments)
        # No content lost.
        assert sum(_tokens(chunker, s) for s in segments) == 137

    def test_short_text_returns_single_segment(self) -> None:
        chunker = _FakeChunker(max_tokens=512)
        segments = _split_text_token_aligned("just a few words", chunker=chunker, max_tokens=512)
        assert segments == ["just a few words"]

    def test_semchunk_v3_chunker_has_no_chunk_method(self) -> None:
        """Hypothesis (GPU prod semchunk==3.2.5): Chunker is callable; .chunk does not exist.

        Pin the failure mode that crashed docling_chunk_worker:
        AttributeError: 'Chunker' object has no attribute 'chunk'
        """

        class _SemchunkV3Chunker:
            def __call__(self, text: str) -> list[str]:
                words = text.split()
                return [" ".join(words[i : i + 8]) for i in range(0, len(words), 8)]

        c = _SemchunkV3Chunker()
        assert callable(c)
        assert not hasattr(c, "chunk")
        with pytest.raises(AttributeError, match="chunk"):
            c.chunk("w0 w1 w2")  # type: ignore[attr-defined]

    def test_split_text_token_aligned_works_with_semchunk_v3_shaped_chunker(
        self, monkeypatch
    ) -> None:
        """Failing lap: code that only calls ``.chunk()`` raises; must call the Chunker."""
        chunker = _FakeChunker(max_tokens=8)
        seen: list[str] = []

        class _SemchunkV3Chunker:
            def __call__(self, text: str) -> list[str]:
                seen.append(text)
                words = text.split()
                return [" ".join(words[i : i + 8]) for i in range(0, len(words), 8)]

        class _SemchunkV3:
            @staticmethod
            def chunkerify(counter, chunk_size=None):
                return _SemchunkV3Chunker()

        monkeypatch.setitem(__import__("sys").modules, "semchunk", _SemchunkV3)
        text = " ".join(f"w{i}" for i in range(20))
        segments = _split_text_token_aligned(text, chunker=chunker, max_tokens=8)

        assert seen, "semchunk Chunker must be invoked via call, not skipped"
        assert segments
        assert all(_tokens(chunker, s) <= 8 for s in segments)
        assert " ".join(segments).split() == text.split()

    def test_falls_back_when_semchunk_returns_non_callable_without_chunk(
        self, monkeypatch
    ) -> None:
        chunker = _FakeChunker(max_tokens=8)

        class _BrokenSem:
            @staticmethod
            def chunkerify(counter, chunk_size=None):
                return object()

        monkeypatch.setitem(__import__("sys").modules, "semchunk", _BrokenSem)
        text = " ".join(f"w{i}" for i in range(40))
        segments = _split_text_token_aligned(text, chunker=chunker, max_tokens=8)
        assert segments
        assert all(_tokens(chunker, s) <= 8 for s in segments)


class TestSplitTextTokenWindows:
    def test_word_fallback_halves_budget_not_multiplies(self) -> None:
        """The old bug used max_tokens * 4 words per window; must be conservative now."""
        chunker = _FakeChunker(max_tokens=100)
        text = " ".join(f"w{i}" for i in range(1000))

        windows = _split_text_token_windows(text, chunker=chunker, max_tokens=100)

        assert all(_tokens(chunker, w) <= 100 for w in windows)
        # Conservative direction: windows are at most half the token budget in words.
        assert max(_tokens(chunker, w) for w in windows) <= 50

    def test_prefers_real_tokenizer_encode_decode(self) -> None:
        class _IdTokenizer:
            def encode(self, text: str, add_special_tokens: bool = False) -> list[str]:
                return text.split()

            def decode(self, ids: list[str], skip_special_tokens: bool = True) -> str:
                return " ".join(ids)

        class _Wrapper:
            def count_tokens(self, *, text: str) -> int:
                return len(text.split())

            def get_tokenizer(self) -> _IdTokenizer:
                return _IdTokenizer()

        chunker = SimpleNamespace(max_tokens=8, tokenizer=_Wrapper())
        text = " ".join(f"t{i}" for i in range(30))

        windows = _split_text_token_windows(text, chunker=chunker, max_tokens=8)

        assert all(len(w.split()) <= 8 for w in windows)
        assert " ".join(windows) == text


class TestSplitPieceTokenAligned:
    def test_reserves_heading_headroom(self) -> None:
        chunker = _FakeChunker(max_tokens=20)
        heading = ["Chapter One", "Section Two"]  # 4 words + separator overhead
        body = " ".join(f"body{i}" for i in range(200))
        piece = _TextChunk(body, heading, {"content_labels": ["text"]})

        out = _split_piece_token_aligned(piece, chunker=chunker, max_tokens=20)

        heading_overhead = _tokens(chunker, "\n".join(heading) + "\n\n")
        budget = 20 - heading_overhead
        assert out
        for segment in out:
            assert _tokens(chunker, segment.text) <= budget
            # Contextual embed text (headings + body) stays within max_tokens.
            contextual = "\n".join(heading) + "\n\n" + segment.text
            assert _tokens(chunker, contextual) <= 20

    def test_prefers_docling_native_split_for_doc_chunks(self) -> None:
        small_parts = [
            SimpleNamespace(text="part one", meta=SimpleNamespace(headings=["H"])),
            SimpleNamespace(text="part two", meta=SimpleNamespace(headings=["H"])),
        ]

        class _DoclingChunker(_FakeChunker):
            def _split_using_plain_text(self, piece):
                return small_parts

        chunker = _DoclingChunker(max_tokens=10)
        # Non-dict meta marks a genuine DocChunk → docling path applies.
        piece = SimpleNamespace(
            text=" ".join(f"w{i}" for i in range(50)),
            meta=SimpleNamespace(headings=["H"]),
        )

        out = _split_piece_token_aligned(piece, chunker=chunker, max_tokens=10)

        assert out == small_parts

    def test_falls_back_when_docling_split_returns_oversized(self) -> None:
        class _BadDoclingChunker(_FakeChunker):
            def _split_using_plain_text(self, piece):
                return [SimpleNamespace(text=" ".join(f"x{i}" for i in range(99)), meta=None)]

        chunker = _BadDoclingChunker(max_tokens=10)
        piece = SimpleNamespace(
            text=" ".join(f"w{i}" for i in range(50)),
            meta=SimpleNamespace(headings=[]),
        )

        out = _split_piece_token_aligned(piece, chunker=chunker, max_tokens=10)

        assert all(_tokens(chunker, c.text) <= 10 for c in out)


class TestSplitChunkAlongDocItemsCharGuards:
    def test_single_giant_paragraph_is_windowed(self) -> None:
        chunk = _TextChunk("x" * 500, ["H"], {})
        out = _split_chunk_along_doc_items(chunk, max_chars=100)
        assert all(len(c.text) <= 100 for c in out)

    def test_oversized_paragraph_among_small_ones_is_windowed(self) -> None:
        """Regression: individual paragraphs over max_chars used to pass through."""
        text = "small one\n\n" + ("y" * 350) + "\n\nsmall two"
        chunk = _TextChunk(text, ["H"], {})

        out = _split_chunk_along_doc_items(chunk, max_chars=100)

        assert all(len(c.text) <= 100 for c in out)


class TestHybridChunksBounded:
    def _run(self, monkeypatch, pieces, max_tokens: int):
        chunker = _FakeChunker(max_tokens=max_tokens)
        monkeypatch.setattr(
            hc,
            "_iter_size_safe_hierarchical_chunks",
            lambda document, *, max_chars: iter(pieces),
        )
        return chunker, _hybrid_chunks_bounded(document=object(), chunker=chunker)

    def test_char_oversized_piece_still_gets_token_bounded(self, monkeypatch) -> None:
        """Regression: > max_chars pieces used to skip the token check entirely."""
        monkeypatch.setattr(hc, "DEFAULT_SAFE_TOKENIZE_CHARS", 200)
        giant = _TextChunk(" ".join(f"w{i}" for i in range(2000)), ["H"], {})

        chunker, out = self._run(monkeypatch, [giant], max_tokens=16)

        assert out
        assert all(_tokens(chunker, hc._chunk_text(c)) <= 16 for c in out)

    def test_token_oversized_but_char_safe_piece_is_split(self, monkeypatch) -> None:
        piece = _TextChunk(" ".join(f"w{i}" for i in range(100)), [], {})

        chunker, out = self._run(monkeypatch, [piece], max_tokens=16)

        assert len(out) > 1
        assert all(_tokens(chunker, hc._chunk_text(c)) <= 16 for c in out)

    def test_fitting_pieces_pass_through_unchanged(self, monkeypatch) -> None:
        piece = _TextChunk("short enough text", ["H"], {})

        _, out = self._run(monkeypatch, [piece], max_tokens=16)

        assert out == [piece]

    def test_no_content_lost_across_splits(self, monkeypatch) -> None:
        words = [f"w{i}" for i in range(300)]
        piece = _TextChunk(" ".join(words), [], {})

        _, out = self._run(monkeypatch, [piece], max_tokens=32)

        emitted = " ".join(hc._chunk_text(c) for c in out).split()
        assert emitted == words
