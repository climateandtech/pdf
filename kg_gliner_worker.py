#!/usr/bin/env python3
"""
GLiNER inference worker (GPU).

Listens on NATS request/reply subject kg.infer (override with KG_GLINER_INFER_SUBJECT).
Payload: { "text", "entity_labels", "relation_labels"?, "threshold"?, "model_name"? }
Reply: { "ok": true, "entities": [...], "relations": [...] } or { "ok": false, "error": "..." }

Run alongside docling_worker on the GPU host — same repo, separate process.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys

import nats

from config import config as nats_config
from kg_gliner.extract import DEFAULT_MODEL, extract_spans

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("kg_gliner_worker")

INFER_SUBJECT = os.getenv("KG_GLINER_INFER_SUBJECT", "kg.infer")
INFER_TIMEOUT_SEC = float(os.getenv("KG_GLINER_INFER_TIMEOUT", "120"))


def _handle_payload(data: dict) -> dict:
    text = data.get("text") or ""
    labels = data.get("entity_labels") or data.get("labels") or []
    if not isinstance(labels, list):
        labels = list(labels)
    rel_labels = data.get("relation_labels")
    if rel_labels is not None and not isinstance(rel_labels, list):
        rel_labels = list(rel_labels)
    threshold = float(data.get("threshold", 0.4))
    model_name = data.get("model_name") or os.getenv("KG_GLINER_MODEL", DEFAULT_MODEL)
    entities, relations = extract_spans(
        text,
        labels,
        relation_labels=rel_labels,
        threshold=threshold,
        model_name=model_name,
    )
    return {"ok": True, "entities": entities, "relations": relations}


async def _on_infer(msg):
    reply: dict
    try:
        data = json.loads(msg.data.decode()) if msg.data else {}
        reply = _handle_payload(data)
    except Exception as e:
        logger.exception("infer failed")
        reply = {"ok": False, "error": str(e)}
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
        "GLiNER infer worker ready subject=%s model=%s",
        INFER_SUBJECT,
        os.getenv("KG_GLINER_MODEL", DEFAULT_MODEL),
    )
    try:
        while True:
            await asyncio.sleep(3600)
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
