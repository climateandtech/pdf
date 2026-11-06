#!/usr/bin/env python3
"""Route-decision worker (GPU).

Listens on NATS request/reply subject route.decide.
Payload: { "state", "criteria" }
Reply: { "ok": true, "choice", "confidence", "model" } or { "ok": false, "error" }

The model is chosen here (ROUTE_DECISION_MODEL). Run alongside docling_worker
in its own process and venv. Do not load this model inside the Docling worker.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

import nats

from config import config as nats_config
from route_decision.decide import decide
from route_decision.runtime import get_model, model_id

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("route_decision_worker")

DECIDE_SUBJECT = os.getenv("ROUTE_DECIDE_SUBJECT", "route.decide")


def _handle_payload(data: dict) -> dict:
    return decide(get_model(), data, model_name=model_id())


async def _on_decide(msg):
    reply: dict
    try:
        data = json.loads(msg.data.decode()) if msg.data else {}
        reply = _handle_payload(data)
    except Exception as exc:
        logger.exception("route decision failed")
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
    await nc.subscribe(DECIDE_SUBJECT, cb=_on_decide)
    logger.info("Route decision ready subject=%s model=%s", DECIDE_SUBJECT, model_id())
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
