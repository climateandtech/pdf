#!/usr/bin/env python3
"""Timestral forecast worker (GPU).

Listens on NATS request/reply subject forecast.infer.
CUDA is required unless FORECAST_DEVICE=cpu. A missing GPU does not fall back.

Run alongside docling_worker on the GPU host — own venv, separate process.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

import nats

from config import config as nats_config
from forecasting.timestral_runtime import forecast_payload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("forecast_worker")

INFER_SUBJECT = os.getenv("FORECAST_INFER_SUBJECT", "forecast.infer")


def _handle_payload(data: dict) -> dict:
    return forecast_payload(data)


async def _on_infer(msg):
    reply: dict
    try:
        data = json.loads(msg.data.decode()) if msg.data else {}
        reply = _handle_payload(data)
    except Exception as exc:
        logger.exception("forecast failed")
        reply = {"ok": False, "error": str(exc)}
    if msg.reply:
        await msg.respond(json.dumps(reply).encode())


async def main():
    url = nats_config.connection_url
    logger.info("Connecting to NATS %s", nats_config.url.split("@")[-1])
    nc = await nats.connect(
        url,
        connect_timeout=nats_config.connect_timeout,
        max_reconnect_attempts=nats_config.max_reconnect_attempts,
    )
    await nc.subscribe(INFER_SUBJECT, cb=_on_infer)
    logger.info(
        "Forecast worker ready subject=%s device=%s",
        INFER_SUBJECT,
        os.getenv("FORECAST_DEVICE", "cuda"),
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
