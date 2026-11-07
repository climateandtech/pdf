"""Speech request limits and handler replies, without loading Whisper or Kokoro."""

from __future__ import annotations

import base64

import pytest

from speech.contract import (
    MAX_AUDIO_BYTES,
    SERVER_TTS_CHARS,
    canonical_whisper,
    media_suffix,
    parse_synthesize,
    parse_transcribe,
    spoken_text,
)
from speech.handlers import handle_synthesize, handle_transcribe


def test_whisper_alias_matches_docling_worker_default() -> None:
    assert canonical_whisper("whisper") == "whisper_tiny"
    assert canonical_whisper("WHISPER_SMALL") == "whisper_small"
    assert canonical_whisper("nope") == "whisper_tiny"


def test_transcribe_rejects_audio_over_the_nats_cap() -> None:
    payload = {
        "audio_base64": base64.b64encode(b"a" * (MAX_AUDIO_BYTES + 1)).decode("ascii"),
        "media_type": "audio/webm",
    }
    with pytest.raises(ValueError, match="audio_too_large"):
        parse_transcribe(payload)


def test_webm_codec_parameter_keeps_the_webm_suffix() -> None:
    assert media_suffix("audio/webm;codecs=opus") == ".webm"


def test_synthesize_trims_to_the_server_character_cap() -> None:
    text = "word " * 200
    request = parse_synthesize({"text": text, "voice": "af_heart"})
    assert len(request.text) <= SERVER_TTS_CHARS
    assert request.text.endswith("…")
    assert spoken_text("  hello   there ", 80) == "hello there"


def test_handle_transcribe_returns_model_text() -> None:
    payload = {
        "audio_base64": base64.b64encode(b"RIFF").decode("ascii"),
        "media_type": "audio/wav",
        "model": "whisper_base",
    }
    reply = handle_transcribe(payload, lambda _request: "sea level rise")
    assert reply == {
        "ok": True,
        "text": "sea level rise",
        "model": "whisper_base",
        "runtime": "server",
    }


def test_handle_synthesize_encodes_wav() -> None:
    reply = handle_synthesize({"text": "Hello", "voice": "af_heart"}, lambda _request: b"RIFF")
    assert reply["ok"] is True
    assert base64.b64decode(reply["audio_base64"]) == b"RIFF"
    assert reply["model"] == "kokoro-82m"
    assert reply["voice"] == "af_heart"


def test_handle_transcribe_keeps_parser_errors_in_the_body() -> None:
    reply = handle_transcribe({}, lambda _request: "unused")
    assert reply == {"ok": False, "error": "audio_required"}
