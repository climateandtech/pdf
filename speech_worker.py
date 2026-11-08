#!/usr/bin/env python3
"""Agent speech worker.

Core NATS request/reply, same shape as ``forecast_worker``:

- ``audio.command.transcribe`` — Docling Whisper (``requirements.txt``)
- ``audio.command.synthesize`` — Kokoro-82M (``requirements-speech.txt``)

Run from the Docling venv on the GPU host. Kokoro stays on CPU inside this
process so it does not take the Docling CUDA queue.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

import nats

from config import config as nats_config
from speech.handlers import handle_synthesize, handle_transcribe

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("speech_worker")

TRANSCRIBE_SUBJECT = os.getenv("AUDIO_TRANSCRIBE_SUBJECT", "audio.command.transcribe")
SYNTHESIZE_SUBJECT = os.getenv("AUDIO_SYNTHESIZE_SUBJECT", "audio.command.synthesize")
QUEUE = "speech_workers"


def _reply_transcribe(payload: dict) -> dict:
    from speech.whisper_runtime import transcribe_request

    return handle_transcribe(payload, transcribe_request)


def _reply_synthesize(payload: dict) -> dict:
    from speech.kokoro_runtime import synthesize_request

    return handle_synthesize(payload, synthesize_request)


async def _on_message(msg, handler) -> None:
    try:
        data = json.loads(msg.data.decode()) if msg.data else {}
        if not isinstance(data, dict):
            reply = {"ok": False, "error": "bad_request"}
        else:
            reply = handler(data)
    except ImportError as exc:
        logger.info("speech runtime missing: %s", exc)
        reply = {"ok": False, "error": str(exc)}
    except (json.JSONDecodeError, UnicodeError, TypeError, ValueError) as exc:
        logger.info("speech request rejected: %s", exc)
        reply = {"ok": False, "error": "bad_request"}
    if msg.reply:
        await msg.respond(json.dumps(reply).encode())


async def _on_transcribe(msg) -> None:
    await _on_message(msg, _reply_transcribe)


async def _on_synthesize(msg) -> None:
    await _on_message(msg, _reply_synthesize)


def _warmup_kokoro() -> None:
    """Prime Kokoro in-process before accepting NATS traffic."""
    voice = (os.getenv("BOT_VOICE_TTS_VOICE") or "af_nicole").strip() or "af_nicole"
    try:
        from speech.kokoro_runtime import warmup

        logger.info("Warming Kokoro voice=%s", voice)
        warmup(voice)
        logger.info("Kokoro warmup done")
    except Exception as exc:  # noqa: BLE001 — startup must still serve Whisper
        logger.warning("Kokoro warmup skipped: %s", exc)


async def main() -> None:
    """Subscribe both speech subjects until the process is stopped."""
    url = nats_config.connection_url
    logger.info("Connecting to NATS %s", nats_config.url.split("@")[-1])
    # Load Kokoro before subscribe so the first page-speak is not a ~5s cold start.
    await asyncio.to_thread(_warmup_kokoro)
    nc = await nats.connect(
        url,
        connect_timeout=nats_config.connect_timeout,
        max_reconnect_attempts=nats_config.max_reconnect_attempts,
    )
    await nc.subscribe(TRANSCRIBE_SUBJECT, queue=QUEUE, cb=_on_transcribe)
    await nc.subscribe(SYNTHESIZE_SUBJECT, queue=QUEUE, cb=_on_synthesize)
    logger.info(
        "Speech worker ready transcribe=%s synthesize=%s",
        TRANSCRIBE_SUBJECT,
        SYNTHESIZE_SUBJECT,
    )
    try:
        await asyncio.Event().wait()
    except KeyboardInterrupt:
        logger.info("Shutting down")
    finally:
        await nc.drain()
        await nc.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
