"""Request parsing for audio.command.transcribe and audio.command.synthesize.

Mirrors ct-platform ``app.speech_contract`` and
``ct-platform/docs/contracts/bot-voice.v1.schema.json``.
"""

from __future__ import annotations

import base64
import binascii
import re
from dataclasses import dataclass

MAX_AUDIO_BYTES = 600_000
SERVER_TTS_CHARS = 220
DEFAULT_WHISPER_MODEL = "whisper_tiny"
DEFAULT_VOICE = "af_nicole"
DEFAULT_SPEED = 1.1
DEFAULT_PITCH = 1.01

_WHISPER_MODELS = frozenset(
    {
        "whisper_tiny",
        "whisper_base",
        "whisper_small",
        "whisper_medium",
        "whisper_large",
        "whisper_turbo",
    }
)
_MEDIA_SUFFIX = {
    "audio/webm": ".webm",
    "video/webm": ".webm",
    "audio/wav": ".wav",
    "audio/wave": ".wav",
    "audio/x-wav": ".wav",
    "audio/mpeg": ".mp3",
    "audio/mp3": ".mp3",
    "audio/ogg": ".ogg",
    "audio/flac": ".flac",
    "audio/mp4": ".m4a",
    "audio/x-m4a": ".m4a",
}
_LANGUAGE = re.compile(r"^[a-z]{2}$")
_VOICE = re.compile(r"^[a-z]{2}_[a-z0-9]{1,24}$")


@dataclass(frozen=True)
class TranscribeRequest:
    """One utterance for the Docling Whisper pipeline."""

    audio: bytes
    media_type: str
    suffix: str
    model: str
    language: str


@dataclass(frozen=True)
class SynthesizeRequest:
    """One reply for Kokoro, already trimmed for the NATS payload cap."""

    text: str
    voice: str
    speed: float = DEFAULT_SPEED
    pitch: float = DEFAULT_PITCH


def spoken_text(text: str, limit: int) -> str:
    """Collapse whitespace and trim to ``limit`` characters."""
    cleaned = " ".join((text or "").split())
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 1].rstrip() + "…"


def canonical_whisper(name: str) -> str:
    """Map the Docling worker's asr names onto one preset. Unknown names use tiny."""
    key = (name or DEFAULT_WHISPER_MODEL).strip().lower()
    if key == "whisper":
        return DEFAULT_WHISPER_MODEL
    if key in _WHISPER_MODELS:
        return key
    return DEFAULT_WHISPER_MODEL


def media_suffix(media_type: str) -> str:
    """File suffix Docling should see for this content type."""
    normalized = (media_type or "audio/webm").split(";", 1)[0].strip().lower()
    return _MEDIA_SUFFIX.get(normalized, ".webm")


def canonical_language(language: str) -> str:
    """Accept ``auto`` or an ISO 639-1 code."""
    key = (language or "auto").strip().lower()
    if key in ("", "auto"):
        return "auto"
    if _LANGUAGE.match(key):
        return key
    raise ValueError("bad_language")


def canonical_voice(voice: str) -> str:
    """Accept a Kokoro voice id such as ``af_nicole``."""
    key = (voice or DEFAULT_VOICE).strip().lower()
    if not _VOICE.match(key):
        raise ValueError("bad_voice")
    return key


def parse_transcribe(payload: dict) -> TranscribeRequest:
    """Validate a transcribe request body."""
    raw_b64 = payload.get("audio_base64")
    if not isinstance(raw_b64, str) or not raw_b64.strip():
        raise ValueError("audio_required")
    try:
        audio = base64.b64decode(raw_b64, validate=True)
    except binascii.Error as exc:
        raise ValueError("bad_audio") from exc
    if not audio:
        raise ValueError("audio_required")
    if len(audio) > MAX_AUDIO_BYTES:
        raise ValueError("audio_too_large")
    media_type = str(payload.get("media_type") or "audio/webm")
    return TranscribeRequest(
        audio=audio,
        media_type=media_type,
        suffix=media_suffix(media_type),
        model=canonical_whisper(str(payload.get("model") or "")),
        language=canonical_language(str(payload.get("language") or "auto")),
    )


def _clamped_float(value: object, default: float, low: float, high: float) -> float:
    try:
        number = float(value) if value not in (None, "") else default
    except (TypeError, ValueError):
        return default
    return min(high, max(low, number))


def parse_synthesize(payload: dict) -> SynthesizeRequest:
    """Validate a synthesize request body and trim it for the broker cap."""
    text = spoken_text(str(payload.get("text") or ""), SERVER_TTS_CHARS)
    if not text:
        raise ValueError("text_required")
    return SynthesizeRequest(
        text=text,
        voice=canonical_voice(str(payload.get("voice") or "")),
        speed=_clamped_float(payload.get("speed"), DEFAULT_SPEED, 0.75, 1.15),
        pitch=_clamped_float(payload.get("pitch"), DEFAULT_PITCH, 0.8, 1.05),
    )
