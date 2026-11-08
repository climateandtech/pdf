"""Kokoro-82M synthesis. Apache-2.0, CPU-friendly, kept off the Docling GPU queue.

Requires ``kokoro`` and ``soundfile`` from ``requirements-speech.txt``, plus
system ``espeak-ng``.
"""

from __future__ import annotations

import io
from typing import Any

import numpy as np
import soundfile as sf

from speech.contract import SynthesizeRequest

_PIPELINES: dict[str, Any] = {}
_SAMPLE_RATE = 24_000


def _as_numpy(audio: Any) -> np.ndarray:
    if hasattr(audio, "detach"):
        audio = audio.detach().cpu().numpy()
    return np.asarray(audio, dtype=np.float32)


def _pipeline(lang_code: str) -> Any:
    from kokoro import KPipeline

    cached = _PIPELINES.get(lang_code)
    if cached is not None:
        return cached
    pipeline = KPipeline(lang_code=lang_code)
    _PIPELINES[lang_code] = pipeline
    return pipeline


def synthesize_request(request: SynthesizeRequest) -> bytes:
    """Return a 24 kHz mono WAV for one trimmed reply."""
    lang = request.voice[0]
    generated = _pipeline(lang)(request.text, voice=request.voice, speed=request.speed)
    chunks = [_as_numpy(audio) for _gs, _ps, audio in generated]
    if not chunks:
        raise RuntimeError("kokoro returned no audio")
    merged = np.concatenate(chunks)
    rate = max(8_000, int(_SAMPLE_RATE * request.pitch))
    buffer = io.BytesIO()
    sf.write(buffer, merged, rate, format="WAV")
    return buffer.getvalue()


def warmup(voice: str = "af_nicole") -> None:
    """Load Kokoro once so the first live synthesize is not a multi-second cold start."""
    synthesize_request(
        SynthesizeRequest(text="Ready.", voice=voice, speed=1.0, pitch=1.0)
    )
