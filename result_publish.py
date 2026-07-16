"""Publish docling results on NATS without exceeding broker max_payload.

NATS default max_payload is 1MB; JetStream does not remove that limit per message.
Best practice (NATS ADR-20): store large blobs in object storage, send a reference on
the bus. We use S3 (already used for PDF inputs); optional inline body when small.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Tuple

import ijson

logger = logging.getLogger(__name__)

# Stay under default 1MB NATS limit (envelope + subject overhead)
NATS_SAFE_INLINE_BYTES = int(os.getenv("NATS_SAFE_INLINE_BYTES", "900000"))

_HIERARCHICAL_RECORD_PREFIXES = (
    "result.hierarchical_chunks.records.item",
    "hierarchical_chunks.records.item",
)


def _strip_binary_blobs(value: Any) -> Any:
    """Drop embedded data: URIs from export_to_dict (major payload bloat)."""
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, item in value.items():
            if key in ("image", "uri") and isinstance(item, str) and item.startswith("data:"):
                continue
            cleaned[key] = _strip_binary_blobs(item)
        return cleaned
    if isinstance(value, list):
        return [_strip_binary_blobs(item) for item in value]
    return value


def prepare_result_payload(response: Dict[str, Any]) -> Tuple[Dict[str, Any], bytes]:
    """
    Return (payload dict, serialized bytes) for NATS publish attempt.

    Strips binary blobs from structured_data in-place on a shallow-copied
    result dict (worker owns the response object; no deep json round-trip).
    """
    payload = dict(response)
    result = payload.get("result")
    if isinstance(result, dict):
        result = dict(result)
        payload["result"] = result
        if result.get("structured_data") is not None:
            result["structured_data"] = _strip_binary_blobs(result["structured_data"])
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return payload, body


def hierarchical_records_s3_key(request_id: str) -> str:
    """S3 key for JSONL hierarchical chunk records."""
    return f"results/{request_id}.records.jsonl"


def result_envelope_s3_key(request_id: str) -> str:
    """S3 key for slim docs.result envelope metadata (legacy full JSON also used this path)."""
    return f"results/{request_id}.json"


async def upload_hierarchical_records_jsonl(
    client: Any,
    request_id: str,
    records: list[dict[str, Any]],
) -> tuple[str, int]:
    """Upload hierarchical records as JSONL; return (s3_key, byte_size)."""
    lines = [
        json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        for record in records
    ]
    body = ("\n".join(lines) + ("\n" if lines else "")).encode("utf-8")
    key = hierarchical_records_s3_key(request_id)
    await client.upload_bytes(key, body, content_type="application/x-ndjson")
    return key, len(body)


def build_slim_chunk_result(
    *,
    request_id: str,
    backend_resource_id: Any,
    parse_mode: Any,
    docling_options: dict[str, Any] | None,
    parse_artifacts: dict[str, Any],
    hierarchical_chunks: dict[str, Any],
    metadata: dict[str, Any] | None = None,
    markdown: str = "",
) -> Dict[str, Any]:
    """Build docs.result without structured_data; records live on S3 as JSONL."""
    hier = dict(hierarchical_chunks)
    records = hier.pop("records", None) or []
    # Caller should already have uploaded records; keep counts only.
    if "record_count" not in hier:
        hier["record_count"] = len(records)
    return {
        "request_id": request_id,
        "status": "success",
        "backend_resource_id": backend_resource_id,
        "parse_mode": parse_mode,
        "docling_options": docling_options or {},
        "result": {
            "text": "",
            "markdown": markdown if len(markdown.encode("utf-8")) < 200_000 else "",
            "structured_data": {},
            "metadata": metadata or {},
            "parse_artifacts": parse_artifacts,
            "hierarchical_chunks": hier,
        },
    }


def build_s3_envelope(
    response: Dict[str, Any],
    *,
    s3_bucket: str,
    s3_key: str,
    full_bytes: int,
) -> Dict[str, Any]:
    """Slim NATS message: pointer to full JSON in S3."""
    result = response.get("result") or {}
    markdown = result.get("markdown") or result.get("text") or ""
    envelope: Dict[str, Any] = {
        "request_id": response.get("request_id"),
        "status": response.get("status"),
        "backend_resource_id": response.get("backend_resource_id"),
        "result_storage": "s3",
        "result_s3_bucket": s3_bucket,
        "result_s3_key": s3_key,
        "result_bytes": full_bytes,
        "result": {
            "markdown": "",
            "structured_data": {},
            "metadata": result.get("metadata") or {},
        },
    }
    # Inline markdown only when it keeps the envelope under the safe limit
    md_bytes = len(markdown.encode("utf-8"))
    if md_bytes and md_bytes < 200_000:
        trial = dict(envelope)
        trial["result"] = {
            **envelope["result"],
            "markdown": markdown,
            "text": markdown,
        }
        if len(json.dumps(trial, ensure_ascii=False).encode("utf-8")) <= NATS_SAFE_INLINE_BYTES:
            envelope = trial
    return envelope


def _s3_object_size_bytes(client: Any, *, bucket: str, key: str) -> int | None:
    try:
        head = client.head_object(Bucket=bucket, Key=key)
    except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
        logger.warning("head_object failed for s3://%s/%s: %s", bucket, key, exc)
        return None
    return int(head.get("ContentLength") or 0)


def should_stream_hierarchical_from_s3(
    *,
    client: Any,
    bucket: str,
    key: str,
) -> bool:
    """Return True when hierarchical records should be streamed instead of fully loaded."""
    flag = os.getenv("CHUNK_STREAM_S3", "").lower()
    if flag in ("1", "true", "yes"):
        return True
    threshold_mb = int(os.getenv("CHUNK_STREAM_S3_THRESHOLD_MB", "50"))
    size_bytes = _s3_object_size_bytes(client, bucket=bucket, key=key)
    # Fail closed: unknown size must stream (pointer hydrate), never full-load.
    if size_bytes is None:
        return True
    return size_bytes > threshold_mb * 1024 * 1024


def summarize_hierarchical_from_s3(
    *,
    client: Any,
    bucket: str,
    key: str,
) -> dict[str, Any]:
    """Count hierarchical GPU records in S3 without loading the full JSON blob."""
    obj = client.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    tier_counts: dict[str, int] = {}
    record_count = 0
    last_error: Exception | None = None
    for prefix in _HIERARCHICAL_RECORD_PREFIXES:
        try:
            for record in ijson.items(body, prefix):
                if not isinstance(record, dict):
                    continue
                record_count += 1
                level = str(record.get("chunk_level") or "chunk")
                tier_counts[level] = tier_counts.get(level, 0) + 1
            body.close()
            return {
                "record_count": record_count,
                "tier_counts": tier_counts or None,
            }
        except ijson.JSONDecodeError as exc:
            last_error = exc
            body.close()
            obj = client.get_object(Bucket=bucket, Key=key)
            body = obj["Body"]
            continue
    body.close()
    if last_error is not None:
        raise last_error
    raise ValueError(f"No hierarchical_chunks.records found in s3://{bucket}/{key}")


def _hydrate_s3_pointer_only(
    data: Dict[str, Any],
    *,
    client: Any,
    bucket: str,
    key: str,
) -> Dict[str, Any]:
    """Merge S3 locator + hierarchical summary without loading the full result object."""
    summary = summarize_hierarchical_from_s3(client=client, bucket=bucket, key=key)
    merged = dict(data)
    inline = merged.get("result") or {}
    merged["result"] = {
        "markdown": inline.get("markdown") or "",
        "text": inline.get("text") or inline.get("markdown") or "",
        "structured_data": {},
        "metadata": inline.get("metadata") or {},
        "hierarchical_chunks": {
            "tier_counts": summary.get("tier_counts"),
            "record_count": summary.get("record_count", 0),
            "result_s3_bucket": bucket,
            "result_s3_key": key,
        },
        "parse_artifacts": inline.get("parse_artifacts") or {},
    }
    logger.info(
        "PDF result S3 pointer hydrate | key=%s record_count=%s stream=1",
        key,
        summary.get("record_count", 0),
    )
    return merged


def _hydrate_s3_full_payload(
    data: Dict[str, Any],
    *,
    client: Any,
    bucket: str,
    key: str,
) -> Dict[str, Any]:
    """Fetch and merge a small spilled PDF result from S3."""
    obj = client.get_object(Bucket=bucket, Key=key)
    stored = json.loads(obj["Body"].read())
    stored_result = stored.get("result") or {}
    inline = data.get("result") or {}
    merged = dict(data)
    merged["result"] = {
        **stored_result,
        "markdown": stored_result.get("markdown") or inline.get("markdown") or "",
        "text": (
            stored_result.get("text")
            or stored_result.get("markdown")
            or inline.get("text")
            or ""
        ),
        "structured_data": (
            stored_result.get("structured_data") or inline.get("structured_data") or {}
        ),
        "metadata": stored_result.get("metadata") or inline.get("metadata") or {},
        "hierarchical_chunks": (
            stored_result.get("hierarchical_chunks") or inline.get("hierarchical_chunks")
        ),
        "parse_artifacts": (
            stored_result.get("parse_artifacts") or inline.get("parse_artifacts") or {}
        ),
    }
    return merged


def hydrate_docling_result_envelope(
    data: Dict[str, Any],
    *,
    default_bucket: str | None = None,
    s3_client: Any = None,
) -> Dict[str, Any]:
    """Merge full docs.result from S3 when the NATS message is a spill envelope.

    Matches platform ``hydrate_pdf_result_from_storage`` so E2E smokes and
    consumers validate spilled large-PDF results (prod or ct-storage-test).

    Large S3 objects use pointer-only hydration (counts + locators) so chunking can
    stream hierarchical records without loading the full JSON into memory.
    """
    s3_key = data.get("result_s3_key")
    if not s3_key or data.get("result_storage") != "s3":
        return data

    bucket = (
        data.get("result_s3_bucket")
        or default_bucket
        or os.getenv("S3_BUCKET")
        or os.getenv("S3_BUCKET_NAME")
    )
    if not bucket:
        raise ValueError(
            "S3 spill envelope missing result_s3_bucket and no default bucket configured"
        )

    client = s3_client
    if client is None:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            region_name=os.environ.get("AWS_DEFAULT_REGION", "hel1"),
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )

    if should_stream_hierarchical_from_s3(client=client, bucket=bucket, key=s3_key):
        return _hydrate_s3_pointer_only(data, client=client, bucket=bucket, key=s3_key)
    return _hydrate_s3_full_payload(data, client=client, bucket=bucket, key=s3_key)


async def publish_docling_result(client, subject: str, response: Dict[str, Any]) -> str:
    """
    Publish result on JetStream. Returns 'inline' or 's3'.

    Never raises for payload size — spills to S3 when needed.

    Slim hierarchical results always upload the envelope JSON to
    ``results/{request_id}.json`` and set ``result_s3_key`` so platform hydrate
    never falls back to the ``*.records.jsonl`` path.
    """
    response = dict(response)
    result = dict(response.get("result") or {})
    hier = result.get("hierarchical_chunks")
    request_id = str(response.get("request_id") or "unknown")
    if isinstance(hier, dict) and hier.get("records_s3_key"):
        envelope_key = result_envelope_s3_key(request_id)
        hier = dict(hier)
        hier["result_s3_key"] = envelope_key
        hier["result_s3_bucket"] = client.s3_config.bucket_name
        result["hierarchical_chunks"] = hier
        response["result"] = result
        response["result_storage"] = "s3"
        response["result_s3_bucket"] = client.s3_config.bucket_name
        response["result_s3_key"] = envelope_key
        await client.upload_bytes(
            envelope_key,
            json.dumps(response, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )

    payload, body = prepare_result_payload(response)

    if len(body) <= NATS_SAFE_INLINE_BYTES:
        await client.js.publish(subject, body)
        return "inline"

    s3_key = response.get("result_s3_key") or f"results/{request_id}.json"
    if not response.get("result_s3_key"):
        await client.upload_bytes(
            s3_key,
            json.dumps(response, ensure_ascii=False).encode("utf-8"),
            content_type="application/json",
        )
    envelope = build_s3_envelope(
        response,
        s3_bucket=client.s3_config.bucket_name,
        s3_key=s3_key,
        full_bytes=len(body),
    )
    # Preserve hierarchical pointer fields on spilled NATS envelope.
    if isinstance(hier, dict):
        envelope.setdefault("result", {})["hierarchical_chunks"] = hier
        envelope["result"]["parse_artifacts"] = result.get("parse_artifacts") or {}
    envelope_body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    if len(envelope_body) > NATS_SAFE_INLINE_BYTES:
        # Last resort: metadata-only envelope (platform must fetch S3)
        envelope["result"]["markdown"] = ""
        envelope_body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")

    await client.js.publish(subject, envelope_body)
    print(
        f"📦 Result spilled to S3 ({len(body)} bytes → s3://{client.s3_config.bucket_name}/{s3_key})"
    )
    return "s3"
