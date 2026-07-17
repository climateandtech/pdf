"""Unit: skip-if-done must yield to force_rechunk / min_chunker_version.

Regression tests for the frozen-mega-chunk trap: a result JSON on S3 used to
skip re-chunking unconditionally, so records produced by an older (unbounded)
splitter could never be repaired by republishing a docs.chunk job.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import docling_chunk_worker as dcw
from docling_chunk_worker import (
    DoclingChunkWorker,
    force_rechunk_requested,
    min_chunker_version_requested,
)
from hierarchical_chunker import CHUNKER_VERSION


def _worker() -> DoclingChunkWorker:
    worker = DoclingChunkWorker.__new__(DoclingChunkWorker)
    worker.client = SimpleNamespace()
    worker.s3_config = SimpleNamespace(bucket_name="b")
    worker.nats_config = SimpleNamespace(subject_prefix="docs")
    return worker


class TestForceRechunkRequested:
    def test_top_level_flag(self) -> None:
        assert force_rechunk_requested({"force_rechunk": True}) is True

    def test_string_flag(self) -> None:
        assert force_rechunk_requested({"force_rechunk": "true"}) is True
        assert force_rechunk_requested({"force_rechunk": "0"}) is False

    def test_docling_options_flag(self) -> None:
        assert force_rechunk_requested({"docling_options": {"force_rechunk": 1}}) is True

    def test_absent_defaults_false(self) -> None:
        assert force_rechunk_requested({}) is False
        assert force_rechunk_requested({"docling_options": {}}) is False


class TestMinChunkerVersionRequested:
    def test_absent_is_zero(self) -> None:
        assert min_chunker_version_requested({}) == 0

    def test_top_level(self) -> None:
        assert min_chunker_version_requested({"min_chunker_version": 2}) == 2

    def test_docling_options(self) -> None:
        assert min_chunker_version_requested({"docling_options": {"min_chunker_version": "3"}}) == 3

    def test_garbage_is_zero(self) -> None:
        assert min_chunker_version_requested({"min_chunker_version": "latest"}) == 0

    def test_negative_clamped_to_zero(self) -> None:
        assert min_chunker_version_requested({"min_chunker_version": -1}) == 0


class TestStoredChunkerVersion:
    @pytest.mark.asyncio
    async def test_reads_hierarchical_chunks_stamp(self) -> None:
        worker = _worker()
        envelope = {
            "request_id": "r1",
            "result": {"hierarchical_chunks": {"chunker_version": 2}},
        }
        worker.client.download_result = AsyncMock(
            return_value=json.dumps(envelope).encode("utf-8")
        )
        assert await worker._stored_chunker_version("r1") == 2

    @pytest.mark.asyncio
    async def test_reads_top_level_stamp(self) -> None:
        worker = _worker()
        worker.client.download_result = AsyncMock(
            return_value=json.dumps({"chunker_version": 5}).encode("utf-8")
        )
        assert await worker._stored_chunker_version("r1") == 5

    @pytest.mark.asyncio
    async def test_legacy_envelope_is_version_zero(self) -> None:
        worker = _worker()
        worker.client.download_result = AsyncMock(
            return_value=json.dumps({"request_id": "r1", "result": {}}).encode("utf-8")
        )
        assert await worker._stored_chunker_version("r1") == 0

    @pytest.mark.asyncio
    async def test_download_failure_is_version_zero(self) -> None:
        worker = _worker()
        worker.client.download_result = AsyncMock(side_effect=OSError("boom"))
        assert await worker._stored_chunker_version("r1") == 0


class TestShouldSkipRechunk:
    @pytest.mark.asyncio
    async def test_force_bypasses_s3_check_entirely(self) -> None:
        worker = _worker()
        worker._result_exists = AsyncMock(return_value=True)

        skip = await worker._should_skip_rechunk({"force_rechunk": True}, "r1")

        assert skip is False
        worker._result_exists.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_missing_result_never_skips(self) -> None:
        worker = _worker()
        worker._result_exists = AsyncMock(return_value=False)
        assert await worker._should_skip_rechunk({}, "r1") is False

    @pytest.mark.asyncio
    async def test_existing_result_skips_without_version_demand(self) -> None:
        worker = _worker()
        worker._result_exists = AsyncMock(return_value=True)
        worker._stored_chunker_version = AsyncMock()

        assert await worker._should_skip_rechunk({}, "r1") is True
        worker._stored_chunker_version.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_version_rechunks(self) -> None:
        worker = _worker()
        worker._result_exists = AsyncMock(return_value=True)
        worker._stored_chunker_version = AsyncMock(return_value=0)

        skip = await worker._should_skip_rechunk({"min_chunker_version": CHUNKER_VERSION}, "r1")

        assert skip is False

    @pytest.mark.asyncio
    async def test_current_version_skips(self) -> None:
        worker = _worker()
        worker._result_exists = AsyncMock(return_value=True)
        worker._stored_chunker_version = AsyncMock(return_value=CHUNKER_VERSION)

        skip = await worker._should_skip_rechunk({"min_chunker_version": CHUNKER_VERSION}, "r1")

        assert skip is True


class TestProcessChunkJobForcePath:
    @pytest.mark.asyncio
    async def test_force_rechunk_rechunks_despite_existing_result(self, monkeypatch) -> None:
        """End-to-end worker path: force flag must re-run chunk_hierarchical."""
        worker = _worker()
        worker.client.js = SimpleNamespace(publish=AsyncMock())
        worker._result_exists = AsyncMock(return_value=True)
        worker._republish_existing_result = AsyncMock()

        chunk_result = {
            "records": [{"chunk_index": 0, "text": "t", "embed": True}],
            "tier_counts": {"child": 1},
            "metrics": {},
            "chunker_version": CHUNKER_VERSION,
        }
        chunk_hierarchical = AsyncMock()  # placeholder for call assertion via wrapper

        def _fake_chunk(structured_data):
            chunk_hierarchical(structured_data)
            return chunk_result

        monkeypatch.setattr(dcw, "chunk_hierarchical", _fake_chunk)
        monkeypatch.setattr(
            dcw, "load_parse_artifacts", AsyncMock(return_value=({"texts": []}, "# md"))
        )
        monkeypatch.setattr(
            dcw,
            "upload_hierarchical_records_jsonl",
            AsyncMock(return_value=("results/r1.records.jsonl", 42)),
        )
        publish_mock = AsyncMock(return_value="s3")
        monkeypatch.setattr(dcw, "publish_docling_result", publish_mock)
        monkeypatch.setattr(dcw, "_touch_heartbeat", lambda: None)

        job = {"request_id": "r1", "backend_resource_id": "res-1", "force_rechunk": True}
        message = SimpleNamespace(
            data=json.dumps(job).encode("utf-8"),
            ack=AsyncMock(),
            nak=AsyncMock(),
            term=AsyncMock(),
        )

        await worker.process_chunk_job(message)

        chunk_hierarchical.assert_called_once()
        worker._republish_existing_result.assert_not_awaited()
        message.ack.assert_awaited_once()
        # Published result carries the chunker_version stamp for future skips.
        response = publish_mock.await_args.args[2]
        hier = response["result"]["hierarchical_chunks"]
        assert hier["chunker_version"] == CHUNKER_VERSION

    @pytest.mark.asyncio
    async def test_no_force_still_skips_when_result_exists(self, monkeypatch) -> None:
        worker = _worker()
        worker._result_exists = AsyncMock(return_value=True)
        worker._republish_existing_result = AsyncMock(return_value="s3-skip")
        load_mock = AsyncMock()
        monkeypatch.setattr(dcw, "load_parse_artifacts", load_mock)
        monkeypatch.setattr(dcw, "_touch_heartbeat", lambda: None)

        job = {"request_id": "r1", "backend_resource_id": "res-1"}
        message = SimpleNamespace(
            data=json.dumps(job).encode("utf-8"),
            ack=AsyncMock(),
            nak=AsyncMock(),
            term=AsyncMock(),
        )

        await worker.process_chunk_job(message)

        worker._republish_existing_result.assert_awaited_once()
        load_mock.assert_not_awaited()
        message.ack.assert_awaited_once()
