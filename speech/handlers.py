"""Map speech NATS bodies to ok/error replies without loading models."""

from __future__ import annotations

import base64
from collections.abc import Callable

from speech.contract import SynthesizeRequest, TranscribeRequest, parse_synthesize, parse_transcribe


def handle_transcribe(
    payload: dict,
    transcribe: Callable[[TranscribeRequest], str],
) -> dict:
    """Transcribe one request. Model failures stay inside the reply body."""
    try:
        request = parse_transcribe(payload)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    try:
        text = transcribe(request)
    except Exception as exc:  # noqa: BLE001 — ASR libraries raise their own types
        return {"ok": False, "error": str(exc)}
    return {"ok": True, "text": text, "model": request.model, "runtime": "server"}


def handle_synthesize(
    payload: dict,
    synthesize: Callable[[SynthesizeRequest], bytes],
) -> dict:
    """Synthesize one request. Audio is standard base64 WAV."""
    try:
        request = parse_synthesize(payload)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    try:
        wav = synthesize(request)
    except Exception as exc:  # noqa: BLE001 — TTS libraries raise their own types
        return {"ok": False, "error": str(exc)}
    if not wav:
        return {"ok": False, "error": "empty_audio"}
    encoded = base64.b64encode(wav).decode("ascii")
    return {
        "ok": True,
        "audio_base64": encoded,
        "media_type": "audio/wav",
        "model": "kokoro-82m",
        "voice": request.voice,
        "runtime": "server",
    }
