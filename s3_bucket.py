"""S3 bucket discovery and creation (strict — no silent create failures)."""

from __future__ import annotations

import logging
from typing import Any

from botocore.exceptions import ClientError

from s3_config import S3Config

logger = logging.getLogger(__name__)

_MISSING_BUCKET_CODES = frozenset({"404", "NoSuchBucket", "NotFound"})


class S3BucketEnsureError(RuntimeError):
    """Bucket missing, inaccessible, or create failed."""


def _client_error_code(error: ClientError) -> str:
    return error.response.get("Error", {}).get("Code", "")


async def _list_bucket_names(s3: Any) -> set[str]:
    response = await s3.list_buckets()
    return {item["Name"] for item in response.get("Buckets", [])}


async def bucket_is_accessible(s3: Any, bucket_name: str) -> bool:
    """Return True when this account can use the bucket (head or list)."""
    try:
        await s3.head_bucket(Bucket=bucket_name)
        return True
    except ClientError as head_error:
        code = _client_error_code(head_error)
        if code not in _MISSING_BUCKET_CODES:
            raise S3BucketEnsureError(
                f"head_bucket failed for {bucket_name!r}: {head_error}"
            ) from head_error

    owned = await _list_bucket_names(s3)
    if bucket_name in owned:
        logger.debug("Bucket %s listed for account (head returned missing)", bucket_name)
        return True
    return False


async def _create_bucket(s3: Any, config: S3Config) -> None:
    bucket_name = config.bucket_name
    region = config.region_name
    try:
        if region == "us-east-1":
            await s3.create_bucket(Bucket=bucket_name)
        else:
            await s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
    except ClientError as create_error:
        code = _client_error_code(create_error)
        if code in ("BucketAlreadyExists", "BucketAlreadyOwnedByYou"):
            if bucket_name in await _list_bucket_names(s3):
                return
            raise S3BucketEnsureError(
                f"Cannot create bucket {bucket_name!r}: name is taken "
                f"({code}) and it is not in this account's bucket list"
            ) from create_error
        raise S3BucketEnsureError(
            f"create_bucket failed for {bucket_name!r}: {create_error}"
        ) from create_error

    logger.info("Created S3 bucket: %s", bucket_name)


async def ensure_bucket_exists(s3: Any, config: S3Config) -> None:
    """Find or create the configured bucket; raise on any ambiguous failure."""
    bucket_name = config.bucket_name
    if await bucket_is_accessible(s3, bucket_name):
        logger.debug("Bucket %s is accessible", bucket_name)
        return

    await _create_bucket(s3, config)
    if not await bucket_is_accessible(s3, bucket_name):
        raise S3BucketEnsureError(
            f"Bucket {bucket_name!r} is still not accessible after create_bucket"
        )
