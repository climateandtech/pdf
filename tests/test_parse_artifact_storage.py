"""Unit tests for S3 parse artifact keys and chunk job routing."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock

import pytest

from chunk_job import build_chunk_job, needs_hierarchical_chunk, publish_chunk_job
from parse_artifact_storage import (
    docling_json_s3_key,
    load_parse_artifacts,
    markdown_s3_key,
    parse_artifact_metadata,
    store_parse_artifacts,
)


def test_docling_json_s3_key_uses_request_id():
    assert docling_json_s3_key("abc-123") == "parsed/abc-123/docling.json"


def test_markdown_s3_key_uses_request_id():
    assert markdown_s3_key("abc-123") == "parsed/abc-123/markdown.md"


def test_needs_hierarchical_chunk_when_flag_set():
    assert needs_hierarchical_chunk({"hierarchical_chunk": True}) is True
    assert needs_hierarchical_chunk({"hierarchical_chunk": False}) is False
    assert needs_hierarchical_chunk(None) is False


@pytest.mark.asyncio
async def test_store_parse_artifacts_uploads_json_and_markdown():
    client = AsyncMock()
    client.s3_config.bucket_name = "documents"
    client.upload_bytes = AsyncMock(return_value="parsed/r1/docling.json")

    artifacts = await store_parse_artifacts(
        client,
        "r1",
        structured_data={"texts": []},
        markdown="# Title",
    )

    assert artifacts["parse_storage"] == "s3"
    assert artifacts["docling_json_s3_key"] == "parsed/r1/docling.json"
    assert artifacts["markdown_s3_key"] == "parsed/r1/markdown.md"
    assert client.upload_bytes.await_count == 2


@pytest.mark.asyncio
async def test_load_parse_artifacts_round_trip():
    structured = {"texts": [{"text": "hello"}]}
    client = AsyncMock()
    client.download_result = AsyncMock(
        side_effect=[
            json.dumps(structured).encode("utf-8"),
            b"# Hello",
        ]
    )

    job = {
        "docling_json_s3_key": "parsed/r1/docling.json",
        "markdown_s3_key": "parsed/r1/markdown.md",
    }
    loaded_structured, markdown = await load_parse_artifacts(client, job)

    assert loaded_structured == structured
    assert markdown == "# Hello"


@pytest.mark.asyncio
async def test_load_parse_artifacts_falls_back_to_conventional_keys():
    """force_rechunk jobs may carry only request_id; keys derive from convention."""
    structured = {"texts": []}
    client = AsyncMock()
    client.download_result = AsyncMock(
        side_effect=[json.dumps(structured).encode("utf-8"), b"# md"]
    )

    loaded_structured, markdown = await load_parse_artifacts(client, {"request_id": "r9"})

    assert loaded_structured == structured
    assert markdown == "# md"
    keys = [call.args[0] for call in client.download_result.await_args_list]
    assert keys == ["parsed/r9/docling.json", "parsed/r9/markdown.md"]


@pytest.mark.asyncio
async def test_load_parse_artifacts_without_pointers_or_request_id_raises():
    client = AsyncMock()
    with pytest.raises(ValueError, match="chunk job missing"):
        await load_parse_artifacts(client, {})


def test_parse_artifact_metadata_normalizes_s3_pointers():
    artifacts = {
        "parse_storage": "s3",
        "parse_s3_bucket": "documents",
        "docling_json_s3_key": "parsed/r1/docling.json",
        "markdown_s3_key": "parsed/r1/markdown.md",
        "extra": "ignored",
    }
    assert parse_artifact_metadata(artifacts) == {
        "parse_storage": "s3",
        "parse_s3_bucket": "documents",
        "docling_json_s3_key": "parsed/r1/docling.json",
        "markdown_s3_key": "parsed/r1/markdown.md",
    }


def test_build_chunk_job_includes_metadata_and_artifacts():
    job = build_chunk_job(
        request_id="r1",
        backend_resource_id="res-1",
        parse_mode="fast_text_tables",
        docling_options={"hierarchical_chunk": True},
        processing_metadata={"pages": 3},
        artifacts={"docling_json_s3_key": "parsed/r1/docling.json"},
    )
    assert job["request_id"] == "r1"
    assert job["metadata"]["pages"] == 3
    assert job["docling_json_s3_key"] == "parsed/r1/docling.json"


@pytest.mark.asyncio
async def test_publish_chunk_job_uses_docs_chunk_subject():
    client = AsyncMock()
    client.js.publish = AsyncMock()
    job = {"request_id": "r1", "backend_resource_id": "res-1"}

    await publish_chunk_job(client, "docs", job)

    client.js.publish.assert_awaited_once()
    subject, body = client.js.publish.await_args.args
    assert subject == "docs.chunk.r1"
    assert json.loads(body.decode())["request_id"] == "r1"
