"""Unit: chunk worker _result_exists must treat S3 404 as miss, not crash."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from botocore.exceptions import ClientError

from docling_chunk_worker import DoclingChunkWorker


def _client_error(code: str, *, http: int | None = None) -> ClientError:
    err: dict = {"Error": {"Code": code, "Message": code}}
    if http is not None:
        err["ResponseMetadata"] = {"HTTPStatusCode": http}
    return ClientError(err, "HeadObject")


@pytest.mark.asyncio
async def test_result_exists_true_on_head_ok() -> None:
    worker = DoclingChunkWorker.__new__(DoclingChunkWorker)
    s3 = AsyncMock()
    s3.head_object = AsyncMock(return_value={})
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=s3)
    ctx.__aexit__ = AsyncMock(return_value=False)
    worker.client = SimpleNamespace(
        s3_client=MagicMock(return_value=ctx),
    )
    worker.s3_config = SimpleNamespace(bucket_name="b")

    assert await worker._result_exists("rid-1") is True
    s3.head_object.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc",
    [
        _client_error("404"),
        _client_error("NoSuchKey"),
        _client_error("NotFound"),
        _client_error("404", http=404),
        _client_error("SomethingElse", http=404),
    ],
)
async def test_result_exists_false_on_missing(exc: ClientError) -> None:
    worker = DoclingChunkWorker.__new__(DoclingChunkWorker)
    s3 = AsyncMock()
    s3.head_object = AsyncMock(side_effect=exc)
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=s3)
    ctx.__aexit__ = AsyncMock(return_value=False)
    worker.client = SimpleNamespace(s3_client=MagicMock(return_value=ctx))
    worker.s3_config = SimpleNamespace(bucket_name="b")

    assert await worker._result_exists("rid-missing") is False


@pytest.mark.asyncio
async def test_result_exists_false_on_other_client_error() -> None:
    """Hypothesis: non-404 ClientError must not crash; proceed as miss."""
    worker = DoclingChunkWorker.__new__(DoclingChunkWorker)
    s3 = AsyncMock()
    s3.head_object = AsyncMock(side_effect=_client_error("500", http=500))
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=s3)
    ctx.__aexit__ = AsyncMock(return_value=False)
    worker.client = SimpleNamespace(s3_client=MagicMock(return_value=ctx))
    worker.s3_config = SimpleNamespace(bucket_name="b")

    assert await worker._result_exists("rid-err") is False
