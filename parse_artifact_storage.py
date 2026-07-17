"""Store GPU Docling parse output on S3 for downstream NATS chunk workers."""

from __future__ import annotations

import json
from typing import Any

from result_publish import _strip_binary_blobs


def docling_json_s3_key(request_id: str) -> str:
    """S3 key for serialized DoclingDocument JSON."""
    return f"parsed/{request_id}/docling.json"


def markdown_s3_key(request_id: str) -> str:
    """S3 key for exported markdown."""
    return f"parsed/{request_id}/markdown.md"


async def store_parse_artifacts(
    client: Any,
    request_id: str,
    *,
    structured_data: dict[str, Any],
    markdown: str,
) -> dict[str, Any]:
    """Upload parse artifacts and return pointers for docs.chunk jobs."""
    cleaned = _strip_binary_blobs(structured_data or {})
    await client.upload_bytes(
        docling_json_s3_key(request_id),
        json.dumps(cleaned, ensure_ascii=False).encode("utf-8"),
        content_type="application/json",
    )
    await client.upload_bytes(
        markdown_s3_key(request_id),
        (markdown or "").encode("utf-8"),
        content_type="text/markdown; charset=utf-8",
    )
    return {
        "parse_storage": "s3",
        "parse_s3_bucket": client.s3_config.bucket_name,
        "docling_json_s3_key": docling_json_s3_key(request_id),
        "markdown_s3_key": markdown_s3_key(request_id),
    }


_PARSE_ARTIFACT_KEYS = (
    "parse_storage",
    "parse_s3_bucket",
    "docling_json_s3_key",
    "markdown_s3_key",
)


def parse_artifact_metadata(artifacts: dict[str, Any] | None) -> dict[str, Any]:
    """Return S3 pointers for platform ``resource_metadata.docling_artifacts``."""
    if not isinstance(artifacts, dict):
        return {}
    return {
        key: artifacts[key]
        for key in _PARSE_ARTIFACT_KEYS
        if artifacts.get(key) is not None
    }


async def load_parse_artifacts(client: Any, job: dict[str, Any]) -> tuple[dict[str, Any], str]:
    """Load Docling JSON and markdown referenced by a docs.chunk job.

    Jobs without explicit S3 pointers (e.g. platform-issued force_rechunk jobs
    that only carry the request id) fall back to the conventional
    ``parsed/{request_id}/...`` keys the parse worker always writes.
    """
    json_key = job.get("docling_json_s3_key")
    md_key = job.get("markdown_s3_key")
    if not json_key or not md_key:
        request_id = str(job.get("request_id") or "").strip()
        if not request_id:
            raise ValueError("chunk job missing docling_json_s3_key or markdown_s3_key")
        json_key = json_key or docling_json_s3_key(request_id)
        md_key = md_key or markdown_s3_key(request_id)

    raw_json = await client.download_result(json_key)
    structured = json.loads(raw_json.decode("utf-8"))
    raw_md = await client.download_result(md_key)
    markdown = raw_md.decode("utf-8")
    return structured, markdown
