"""Unit: S3 bucket find/create (no moto — runs in default CI)."""

from __future__ import annotations

import pytest
from botocore.exceptions import ClientError
from unittest.mock import AsyncMock

from s3_bucket import S3BucketEnsureError, bucket_is_accessible, ensure_bucket_exists
from s3_config import S3Config


def _client_error(code: str, op: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": code}}, op)


@pytest.fixture
def s3_config() -> S3Config:
    return S3Config(
        endpoint_url="https://hel1.your-objectstorage.com",
        region_name="hel1",
        bucket_name="ct-storage-test",
        aws_access_key_id="key",
        aws_secret_access_key="secret",
    )


@pytest.mark.asyncio
async def test_bucket_accessible_when_head_succeeds(s3_config):
    s3 = AsyncMock()
    s3.head_bucket.return_value = {}
    assert await bucket_is_accessible(s3, s3_config.bucket_name) is True
    s3.list_buckets.assert_not_called()


@pytest.mark.asyncio
async def test_bucket_accessible_when_head_404_but_listed(s3_config):
    s3 = AsyncMock()
    s3.head_bucket.side_effect = _client_error("404", "HeadBucket")
    s3.list_buckets.return_value = {"Buckets": [{"Name": s3_config.bucket_name}]}
    assert await bucket_is_accessible(s3, s3_config.bucket_name) is True
    s3.create_bucket.assert_not_called()


@pytest.mark.asyncio
async def test_head_non_missing_error_raises(s3_config):
    s3 = AsyncMock()
    s3.head_bucket.side_effect = _client_error("403", "HeadBucket")
    with pytest.raises(S3BucketEnsureError, match="head_bucket failed"):
        await bucket_is_accessible(s3, s3_config.bucket_name)


@pytest.mark.asyncio
async def test_ensure_bucket_creates_when_missing(s3_config):
    s3 = AsyncMock()
    s3.head_bucket.side_effect = _client_error("404", "HeadBucket")
    s3.list_buckets.side_effect = [
        {"Buckets": []},
        {"Buckets": [{"Name": s3_config.bucket_name}]},
    ]

    await ensure_bucket_exists(s3, s3_config)

    s3.create_bucket.assert_called_once()
    assert s3.create_bucket.call_args.kwargs["Bucket"] == s3_config.bucket_name


@pytest.mark.asyncio
async def test_ensure_bucket_create_failure_raises(s3_config):
    s3 = AsyncMock()
    s3.head_bucket.side_effect = _client_error("404", "HeadBucket")
    s3.list_buckets.return_value = {"Buckets": []}
    s3.create_bucket.side_effect = _client_error("AccessDenied", "CreateBucket")

    with pytest.raises(S3BucketEnsureError, match="create_bucket failed"):
        await ensure_bucket_exists(s3, s3_config)


@pytest.mark.asyncio
async def test_ensure_bucket_name_taken_globally_raises(s3_config):
    s3 = AsyncMock()
    s3.head_bucket.side_effect = _client_error("404", "HeadBucket")
    s3.list_buckets.return_value = {"Buckets": []}
    s3.create_bucket.side_effect = _client_error("BucketAlreadyExists", "CreateBucket")

    with pytest.raises(S3BucketEnsureError, match="name is taken"):
        await ensure_bucket_exists(s3, s3_config)


def test_resolve_bucket_name_prefers_s3_bucket(monkeypatch):
    monkeypatch.setenv("S3_BUCKET", "pdf-worker-bucket")
    monkeypatch.setenv("S3_BUCKET_NAME", "platform-bucket")
    from s3_config import resolve_s3_bucket_name

    assert resolve_s3_bucket_name() == "pdf-worker-bucket"
