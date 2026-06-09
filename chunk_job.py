"""NATS docs.chunk job helpers (parse worker → chunk worker)."""

from __future__ import annotations

import json
from typing import Any


def needs_hierarchical_chunk(docling_options: dict[str, Any] | None) -> bool:
    """Return True when platform requested GPU hierarchical tiers."""
    return bool(isinstance(docling_options, dict) and docling_options.get("hierarchical_chunk"))


def build_chunk_job(
    *,
    request_id: str,
    backend_resource_id: str | None,
    parse_mode: str | None,
    docling_options: dict[str, Any] | None,
    processing_metadata: dict[str, Any],
    artifacts: dict[str, Any],
) -> dict[str, Any]:
    """Assemble a docs.chunk payload with S3 pointers to parse artifacts."""
    return {
        "request_id": request_id,
        "backend_resource_id": backend_resource_id,
        "parse_mode": parse_mode,
        "docling_options": docling_options or {},
        "metadata": processing_metadata,
        **artifacts,
    }


async def publish_chunk_job(client: Any, subject_prefix: str, job: dict[str, Any]) -> None:
    """Publish a chunk job onto JetStream (docs.chunk.{request_id})."""
    request_id = job.get("request_id")
    if not request_id:
        raise ValueError("chunk job missing request_id")
    subject = f"{subject_prefix}.chunk.{request_id}"
    body = json.dumps(job, ensure_ascii=False).encode("utf-8")
    await client.js.publish(subject, body)
