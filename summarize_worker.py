#!/usr/bin/env python3
"""GPU extractive summarize worker: docs.summarize.* → docs.summarize.result.*.

Miller / Altameemi phase-1: sentence embeddings + k-means (see bert_extractive_summary).
Platform publishes excerpt text; this worker returns summary fields for DB merge.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import nats

from bert_extractive_summary import summarize_extractive
from config import NatsConfig
from worker_ack import ack_heartbeat, ensure_pull_subscribe, is_retryable_error

logger = logging.getLogger(__name__)

SUMMARIZE_FILTER = "docs.summarize.*"
SUMMARIZE_DURABLE = "gpu_summarize_worker"
RESULT_PREFIX = "docs.summarize.result"
JOB_TIMEOUT_S = int(os.getenv("SUMMARIZE_JOB_TIMEOUT_S", "600"))
HEARTBEAT_PATH = Path(
    os.getenv(
        "SUMMARIZE_WORKER_HEARTBEAT_PATH",
        "/home/smoldocling/apps/pdf/summarize-worker.heartbeat",
    )
)


def _touch_heartbeat() -> None:
    try:
        HEARTBEAT_PATH.write_text(str(time.time()), encoding="utf-8")
    except OSError as exc:
        logger.warning("heartbeat write failed: %s", exc)


def build_summarize_result_message(
    *,
    resource_id: str,
    fields: dict[str, Any],
    source: str = "gpu_summarize_worker",
) -> dict[str, Any]:
    keys = (
        "introduction",
        "key_points",
        "summary",
        "limitations",
        "summary_backend",
    )
    return {
        "v": 1,
        "kind": "summarize_result",
        "resource_id": resource_id,
        "source": source,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        **{k: fields[k] for k in keys if k in fields},
    }


class GpuSummarizeWorker:
    """Pull docs.summarize.* and publish extractive results."""

    def __init__(self) -> None:
        self.nats_config = NatsConfig()
        self.nc = None
        self.js = None
        self.running = False

    async def setup(self) -> None:
        self.nc = await nats.connect(self.nats_config.url, connect_timeout=15)
        self.js = self.nc.jetstream()
        print(f"✅ Summarize Worker connected to NATS: {self.nats_config.url}")

    async def close(self) -> None:
        self.running = False
        if self.nc:
            await self.nc.close()
        self.nc = None
        self.js = None

    async def _publish_result(self, resource_id: str, fields: dict[str, Any]) -> None:
        assert self.js is not None
        payload = build_summarize_result_message(resource_id=resource_id, fields=fields)
        subject = f"{RESULT_PREFIX}.{resource_id}"
        await self.js.publish(subject, json.dumps(payload).encode())
        logger.info(
            "Published %s backend=%s",
            subject,
            fields.get("summary_backend"),
        )

    async def _handle_job(self, data: dict[str, Any]) -> None:
        resource_id = str(data.get("resource_id") or "")
        if not resource_id:
            raise ValueError("summarize job missing resource_id")
        excerpt = str(data.get("excerpt_text") or data.get("text") or "").strip()
        num_sentences = data.get("num_sentences")
        n = int(num_sentences) if num_sentences is not None else None
        fields = await asyncio.to_thread(summarize_extractive, excerpt, num_sentences=n)
        if not fields.get("summary"):
            logger.warning("Empty extractive summary for %s (excerpt_len=%s)", resource_id, len(excerpt))
            fields = {
                "introduction": "",
                "key_points": "",
                "summary": "",
                "limitations": "None stated",
                "summary_backend": "empty",
            }
        await self._publish_result(resource_id, fields)

    async def run(self) -> None:
        """Pull loop for docs.summarize.*."""
        assert self.js is not None
        self.running = True
        subscription = await ensure_pull_subscribe(
            self.js,
            stream=self.nats_config.stream_name,
            subject=SUMMARIZE_FILTER,
            durable=SUMMARIZE_DURABLE,
        )
        print(f"👂 Summarize Worker listening on {SUMMARIZE_FILTER} durable={SUMMARIZE_DURABLE}")

        while self.running:
            _touch_heartbeat()
            try:
                msgs = await subscription.fetch(1, timeout=5)
            except nats.errors.TimeoutError:
                continue
            except Exception:
                logger.exception("summarize pull failed — resubscribing")
                await asyncio.sleep(2)
                try:
                    subscription = await ensure_pull_subscribe(
                        self.js,
                        stream=self.nats_config.stream_name,
                        subject=SUMMARIZE_FILTER,
                        durable=SUMMARIZE_DURABLE,
                    )
                except Exception:
                    logger.exception("summarize resubscribe failed")
                continue

            for msg in msgs:
                async with ack_heartbeat(msg, interval_s=30):
                    try:
                        data = json.loads(msg.data.decode())
                        await asyncio.wait_for(self._handle_job(data), timeout=JOB_TIMEOUT_S)
                        await msg.ack()
                    except Exception as exc:
                        logger.exception("summarize job failed: %s", exc)
                        if is_retryable_error(exc):
                            try:
                                await msg.nak()
                            except Exception:
                                logger.exception("nak failed")
                        else:
                            try:
                                await msg.term()
                            except Exception:
                                logger.exception("term failed")


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    worker = GpuSummarizeWorker()
    await worker.setup()
    try:
        await worker.run()
    finally:
        await worker.close()


if __name__ == "__main__":
    asyncio.run(main())
