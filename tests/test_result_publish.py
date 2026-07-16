"""Tests for NATS result publish spill-to-S3 logic."""

from __future__ import annotations

import io
import json

import pytest

from result_publish import (
    NATS_SAFE_INLINE_BYTES,
    build_s3_envelope,
    hydrate_docling_result_envelope,
    prepare_result_payload,
)

pytestmark = pytest.mark.unit


def test_prepare_strips_data_uri_blobs():
    response = {
        "request_id": "r1",
        "status": "success",
        "backend_resource_id": "abc",
        "result": {
            "markdown": "hello",
            "structured_data": {
                "pictures": [{"uri": "data:image/png;base64," + ("x" * 2_000_000)}],
            },
        },
    }
    payload, body = prepare_result_payload(response)
    assert len(body) < NATS_SAFE_INLINE_BYTES
    pics = payload["result"]["structured_data"]["pictures"]
    assert pics == [{}]


def test_build_s3_envelope_is_small():
    response = {
        "request_id": "r1",
        "status": "success",
        "backend_resource_id": "abc",
        "result": {"markdown": "x" * 500_000, "metadata": {"pages": 1}},
    }
    env = build_s3_envelope(
        response,
        s3_bucket="documents",
        s3_key="results/r1.json",
        full_bytes=900_000,
    )
    assert env["result_storage"] == "s3"
    assert env["result_s3_key"] == "results/r1.json"
    assert len(json.dumps(env).encode()) <= NATS_SAFE_INLINE_BYTES


def test_hydrate_docling_result_envelope_merges_s3_body(monkeypatch):
    monkeypatch.delenv("CHUNK_STREAM_S3", raising=False)
    monkeypatch.setenv("CHUNK_STREAM_S3_THRESHOLD_MB", "50")
    envelope = {
        "request_id": "r1",
        "status": "success",
        "result_storage": "s3",
        "result_s3_bucket": "ct-storage-test",
        "result_s3_key": "results/r1.json",
        "result": {"markdown": "", "metadata": {"pages": 1}},
    }
    stored = {
        "request_id": "r1",
        "status": "success",
        "result": {
            "markdown": "full body",
            "hierarchical_chunks": {
                "tier_counts": {"parent": 2},
                "records": [{"chunk_level": "parent"}],
            },
        },
    }

    class _Body:
        def read(self):
            return json.dumps(stored).encode()

    class _Client:
        def get_object(self, *, Bucket, Key):
            assert Bucket == "ct-storage-test"
            assert Key == "results/r1.json"
            return {"Body": _Body()}

        def head_object(self, *, Bucket, Key):
            return {"ContentLength": 1024}

    out = hydrate_docling_result_envelope(envelope, s3_client=_Client())
    body = out["result"]
    assert body["markdown"] == "full body"
    assert body["hierarchical_chunks"]["tier_counts"] == {"parent": 2}
    assert len(body["hierarchical_chunks"]["records"]) == 1


def test_hydrate_docling_result_envelope_pointer_only_when_streaming(monkeypatch):
    """Large spilled results hydrate as pointers (matches platform consumers)."""
    monkeypatch.setenv("CHUNK_STREAM_S3", "1")
    envelope = {
        "request_id": "r1",
        "status": "success",
        "result_storage": "s3",
        "result_s3_bucket": "ct-storage-test",
        "result_s3_key": "results/r1.json",
        "result": {"markdown": "", "metadata": {"pages": 1}},
    }
    body = io.BytesIO(
        b'{"result": {"hierarchical_chunks": {"records": ['
        b'{"chunk_level": "parent", "text": "p"}'
        b"]}}}"
    )

    class _Client:
        def get_object(self, *, Bucket, Key):
            assert Bucket == "ct-storage-test"
            assert Key == "results/r1.json"
            return {"Body": body}

        def head_object(self, *, Bucket, Key):
            return {"ContentLength": 100}

    out = hydrate_docling_result_envelope(envelope, s3_client=_Client())
    stub = out["result"]["hierarchical_chunks"]
    assert "records" not in stub
    assert stub["record_count"] == 1
    assert out["result"]["structured_data"] == {}


def test_hydrate_docling_result_envelope_noop_for_inline():
    inline = {"request_id": "r1", "status": "success", "result": {"markdown": "hi"}}
    assert hydrate_docling_result_envelope(inline) == inline


def test_prepare_result_payload_no_deep_json_roundtrip():
    """Hypothesis: prepare strips blobs without json.loads(json.dumps(...))."""
    response = {
        "request_id": "r1",
        "status": "success",
        "result": {
            "markdown": "hello",
            "structured_data": {"pictures": [{"uri": "data:image/png;base64,abc"}]},
        },
    }
    payload, body = prepare_result_payload(response)
    assert isinstance(body, bytes)
    assert payload["result"]["structured_data"]["pictures"] == [{}]
    # Original structured_data object may be replaced, but response dict identity preserved.
    assert payload is not response
    assert "request_id" in payload


def test_should_stream_when_head_object_fails():
    """Hypothesis: head_object failure fails closed to streaming (never full-load)."""
    from result_publish import should_stream_hierarchical_from_s3

    class _Client:
        def head_object(self, *, Bucket, Key):
            raise OSError("boom")

    assert (
        should_stream_hierarchical_from_s3(
            client=_Client(), bucket="b", key="results/x.json"
        )
        is True
    )


def test_build_slim_chunk_result_omits_structured_data_and_records():
    from result_publish import build_slim_chunk_result

    payload = build_slim_chunk_result(
        request_id="r1",
        backend_resource_id="abc",
        parse_mode="fast",
        docling_options={},
        parse_artifacts={"docling_json_s3_key": "parsed/r1/docling.json"},
        hierarchical_chunks={
            "records": [{"chunk_level": "child", "text": "x"}],
            "tier_counts": {"child": 1},
            "records_s3_key": "results/r1.records.jsonl",
        },
        markdown="hi",
    )
    assert payload["result"]["structured_data"] == {}
    hier = payload["result"]["hierarchical_chunks"]
    assert "records" not in hier
    assert hier["record_count"] == 1
    assert hier["records_s3_key"] == "results/r1.records.jsonl"


@pytest.mark.asyncio
async def test_publish_slim_always_uploads_envelope_json():
    """Slim hier with records_s3_key must set result_s3_key to *.json, not JSONL."""
    from result_publish import publish_docling_result

    uploads: list[tuple[str, bytes]] = []
    published: list[bytes] = []

    class _S3Cfg:
        bucket_name = "documents"

    class _JS:
        async def publish(self, subject, body):
            published.append(body)

    class _Client:
        s3_config = _S3Cfg()
        js = _JS()

        async def upload_bytes(self, key, body, content_type=None):
            uploads.append((key, body))

    response = {
        "request_id": "r1",
        "status": "success",
        "backend_resource_id": "abc",
        "result": {
            "markdown": "hi",
            "structured_data": {},
            "hierarchical_chunks": {
                "tier_counts": {"child": 1},
                "record_count": 1,
                "records_s3_key": "results/r1.records.jsonl",
            },
        },
    }
    mode = await publish_docling_result(_Client(), "docs.result", response)
    assert mode == "inline"
    assert uploads and uploads[0][0] == "results/r1.json"
    env = json.loads(uploads[0][1])
    assert env["result_s3_key"] == "results/r1.json"
    assert "records.jsonl" not in env["result_s3_key"]
    nats = json.loads(published[0])
    assert nats["result_s3_key"] == "results/r1.json"
    assert nats["result"]["hierarchical_chunks"]["records_s3_key"] == (
        "results/r1.records.jsonl"
    )
