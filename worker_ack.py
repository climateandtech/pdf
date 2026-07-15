"""JetStream ack heartbeats and durable subscribe helpers for GPU workers.

Durable ack_wait / max_deliver are provisioned from
``ct-platform/backend/config/nats_streams.yaml`` via
``coolify-provisioning/scripts/ensure-jetstream-streams.sh``.
Workers subscribe only; they do not mutate durables on the broker.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from nats.js.api import AckPolicy, ConsumerConfig, DeliverPolicy

logger = logging.getLogger(__name__)

# Defaults match nats_streams.yaml pdf_service durables (env override for test only).
DEFAULT_ACK_WAIT_S = int(os.getenv("NATS_ACK_WAIT_S", "900"))
DEFAULT_MAX_DELIVER = int(os.getenv("NATS_MAX_DELIVER", "5"))
DEFAULT_HEARTBEAT_S = int(os.getenv("NATS_ACK_HEARTBEAT_S", "60"))


def durable_consumer_config(durable: str, *, filter_subject: str) -> ConsumerConfig:
    """Pull-consumer config: long ack window, finite redelivery (create path only)."""
    return ConsumerConfig(
        durable_name=durable,
        ack_policy=AckPolicy.EXPLICIT,
        deliver_policy=DeliverPolicy.ALL,
        filter_subject=filter_subject,
        ack_wait=DEFAULT_ACK_WAIT_S,
        max_deliver=DEFAULT_MAX_DELIVER,
        max_ack_pending=1,
    )


async def ensure_pull_subscribe(
    js: Any,
    *,
    subject: str,
    durable: str,
    stream: str,
) -> Any:
    """Pull-subscribe; warn if durable safety knobs drift (fix via ensure script)."""
    config = durable_consumer_config(durable, filter_subject=subject)
    try:
        info = await js.consumer_info(stream, durable)
        have_ack = int(info.config.ack_wait or 0)
        have_max = int(info.config.max_deliver or 0)
        if have_ack != DEFAULT_ACK_WAIT_S or have_max != DEFAULT_MAX_DELIVER:
            logger.error(
                "consumer %s/%s drift: ack_wait=%s (want %s) max_deliver=%s (want %s); "
                "run coolify-provisioning/scripts/ensure-jetstream-streams.sh --via-gpu",
                stream,
                durable,
                have_ack,
                DEFAULT_ACK_WAIT_S,
                have_max,
                DEFAULT_MAX_DELIVER,
            )
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        # Durable may not exist yet — pull_subscribe creates it with config.
        if "not found" not in str(exc).lower() and "10014" not in str(exc):
            logger.warning("consumer_info %s: %s", durable, exc)

    return await js.pull_subscribe(
        subject=subject,
        durable=durable,
        stream=stream,
        config=config,
    )


@asynccontextmanager
async def ack_heartbeat(message: Any, *, interval_s: float | None = None) -> AsyncIterator[None]:
    """Extend JetStream ack deadline while a long job runs."""
    period = float(interval_s if interval_s is not None else DEFAULT_HEARTBEAT_S)

    async def _beat() -> None:
        while True:
            await asyncio.sleep(period)
            try:
                await message.in_progress()
            except (OSError, RuntimeError, ValueError, TypeError) as exc:
                logger.warning("in_progress heartbeat failed: %s", exc)
                return

    task = asyncio.create_task(_beat())
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


def is_retryable_error(exc: BaseException) -> bool:
    """Transient IO / broker errors may NAK; application errors should TERM."""
    if isinstance(exc, MemoryError):
        return False
    if isinstance(exc, (OSError, TimeoutError, asyncio.TimeoutError, ConnectionError)):
        return True
    return False
