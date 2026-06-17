"""Tests for NATS result publish spill-to-S3 logic."""

from __future__ import annotations

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


def test_hydrate_docling_result_envelope_merges_s3_body():
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

    out = hydrate_docling_result_envelope(envelope, s3_client=_Client())
    body = out["result"]
    assert body["markdown"] == "full body"
    assert body["hierarchical_chunks"]["tier_counts"] == {"parent": 2}
    assert len(body["hierarchical_chunks"]["records"]) == 1


def test_hydrate_docling_result_envelope_noop_for_inline():
    inline = {"request_id": "r1", "status": "success", "result": {"markdown": "hi"}}
    assert hydrate_docling_result_envelope(inline) == inline
