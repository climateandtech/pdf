"""Publish docling results on NATS without exceeding broker max_payload.

NATS default max_payload is 1MB; JetStream does not remove that limit per message.
Best practice (NATS ADR-20): store large blobs in object storage, send a reference on
the bus. We use S3 (already used for PDF inputs); optional inline body when small.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Tuple

# Stay under default 1MB NATS limit (envelope + subject overhead)
NATS_SAFE_INLINE_BYTES = int(os.getenv("NATS_SAFE_INLINE_BYTES", "900000"))


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

    Strips binary blobs from structured_data before measuring size.
    """
    payload = json.loads(json.dumps(response))  # deep copy
    result = payload.get("result")
    if isinstance(result, dict) and result.get("structured_data") is not None:
        result["structured_data"] = _strip_binary_blobs(result["structured_data"])
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    return payload, body


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


async def publish_docling_result(client, subject: str, response: Dict[str, Any]) -> str:
    """
    Publish result on JetStream. Returns 'inline' or 's3'.

    Never raises for payload size — spills to S3 when needed.
    """
    payload, body = prepare_result_payload(response)

    if len(body) <= NATS_SAFE_INLINE_BYTES:
        await client.js.publish(subject, body)
        return "inline"

    s3_key = f"results/{response.get('request_id', 'unknown')}.json"
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
