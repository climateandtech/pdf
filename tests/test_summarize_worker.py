"""Unit tests for GPU summarize worker helpers."""

from __future__ import annotations

from summarize_worker import build_summarize_result_message


def test_build_summarize_result_message() -> None:
    msg = build_summarize_result_message(
        resource_id="11111111-1111-1111-1111-111111111111",
        fields={
            "introduction": "Intro sentence.",
            "key_points": "A, B",
            "summary": "Intro sentence.",
            "limitations": "None stated",
            "summary_backend": "sentence_kmeans",
        },
    )
    assert msg["kind"] == "summarize_result"
    assert msg["summary_backend"] == "sentence_kmeans"
    assert msg["introduction"] == "Intro sentence."
