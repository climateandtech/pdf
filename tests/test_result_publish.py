"""Tests for NATS result publish spill-to-S3 logic."""

from __future__ import annotations

import json

import pytest

from result_publish import (
    NATS_SAFE_INLINE_BYTES,
    build_s3_envelope,
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
