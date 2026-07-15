"""Unit tests for JetStream ack heartbeat helpers."""

from __future__ import annotations

import asyncio

import pytest

from worker_ack import (
    DEFAULT_ACK_WAIT_S,
    DEFAULT_MAX_DELIVER,
    ack_heartbeat,
    durable_consumer_config,
    is_retryable_error,
)

pytestmark = pytest.mark.unit


def test_durable_consumer_config_matches_yaml_defaults():
    cfg = durable_consumer_config("docling_chunk_worker", filter_subject="docs.chunk.*")
    assert cfg.ack_wait == DEFAULT_ACK_WAIT_S == 900
    assert cfg.max_deliver == DEFAULT_MAX_DELIVER == 5
    assert cfg.max_ack_pending == 1
    assert cfg.durable_name == "docling_chunk_worker"


@pytest.mark.asyncio
async def test_ack_heartbeat_calls_in_progress():
    """Hypothesis: heartbeat invokes message.in_progress while the job runs."""
    calls = []

    class Msg:
        async def in_progress(self):
            calls.append("beat")

    async with ack_heartbeat(Msg(), interval_s=0.05):
        await asyncio.sleep(0.12)

    assert len(calls) >= 1


def test_is_retryable_error_classifies_memory_and_timeout():
    assert is_retryable_error(MemoryError("oom")) is False
    assert is_retryable_error(TimeoutError("t")) is True
    assert is_retryable_error(ValueError("bad")) is False
