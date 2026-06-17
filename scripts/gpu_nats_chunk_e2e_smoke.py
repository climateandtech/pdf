#!/usr/bin/env python3
"""E2E smoke: docs.process → parse → docs.chunk → chunk worker → docs.result."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

import aioboto3
import nats
from dotenv import load_dotenv

_SCRIPT_ROOT = Path(__file__).resolve().parents[1]
if str(_SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_ROOT))

from result_publish import hydrate_docling_result_envelope  # noqa: E402


def _load_env() -> None:
    for path in (
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[1] / ".env",
        Path("/home/smoldocling/apps/pdf-test/.env"),
        Path("/home/smoldocling/apps/pdf/.env"),
    ):
        if path.is_file():
            load_dotenv(path)
            return


def nats_url() -> str:
    url = os.environ["NATS_URL"]
    token = os.environ.get("NATS_TOKEN")
    if token and "@" not in url.split("://", 1)[-1]:
        return url.replace("nats://", f"nats://{token}@")
    return url


async def main() -> int:
    parser = argparse.ArgumentParser(description="GPU NATS parse+chunk E2E smoke")
    parser.add_argument("pdf", nargs="?", default="tests/fixtures/minimal.pdf")
    parser.add_argument("--hierarchical", action="store_true", help="Request hierarchical_chunk tiers")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()

    _load_env()
    pdf_path = Path(args.pdf)
    if not pdf_path.is_file():
        print(f"Missing PDF: {pdf_path}", file=sys.stderr)
        return 2

    bucket = os.getenv("S3_BUCKET") or os.getenv("S3_BUCKET_NAME", "")
    if not bucket:
        print("S3_BUCKET or S3_BUCKET_NAME required", file=sys.stderr)
        return 2

    test_id = str(uuid.uuid4())
    s3_key = f"e2e-smoke/{test_id}.pdf"
    pdf = pdf_path.read_bytes()
    print(f"broker={os.environ.get('NATS_URL', '').split('@')[-1]} test_id={test_id}")

    session = aioboto3.Session()
    async with session.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
        region_name=os.environ.get("AWS_DEFAULT_REGION", "hel1"),
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    ) as s3:
        await s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=pdf,
            ContentType="application/pdf",
        )
    print(f"uploaded s3://{bucket}/{s3_key}")

    nc = await nats.connect(nats_url())
    js = nc.jetstream()

    try:
        info = await js.stream_info("DOCUMENTS")
        subjects = list(info.config.subjects or [])
        print(f"stream DOCUMENTS subjects={subjects}")
        if not any("docs.chunk" in s for s in subjects):
            print("FAIL: DOCUMENTS missing docs.chunk.* — run scripts/ensure_documents_stream.py")
            return 1
    except Exception as exc:
        print(f"stream DOCUMENTS: {exc}")
        return 1

    done = asyncio.Event()
    result: dict = {}

    async def on_result(msg):
        data = json.loads(msg.data.decode())
        if data.get("request_id") == test_id:
            result.update(data)
            done.set()

    await nc.subscribe(f"docs.result.{test_id}", cb=on_result)

    docling_options: dict = {
        "do_ocr": False,
        "do_table_structure": True,
        "force_backend_text": True,
    }
    if args.hierarchical:
        docling_options["hierarchical_chunk"] = True

    payload = {
        "request_id": test_id,
        "s3_bucket": bucket,
        "s3_key": s3_key,
        "s3_url": f"{os.environ.get('S3_ENDPOINT_URL', '')}/{bucket}/{s3_key}",
        "filename": pdf_path.name,
        "backend_resource_id": str(uuid.uuid4()),
        "source": "gpu_nats_chunk_e2e_smoke",
        "parse_mode": "fast_text_tables",
        "docling_options": docling_options,
    }
    subject = f"docs.process.{test_id}"
    ack = await js.publish(subject, json.dumps(payload).encode())
    print(f"published {subject} hierarchical={args.hierarchical} seq={ack.seq}")

    try:
        await asyncio.wait_for(done.wait(), timeout=args.timeout)
    except asyncio.TimeoutError:
        print(f"FAIL: no docs.result within {args.timeout}s")
        await nc.close()
        return 1

    if result.get("result_storage") == "s3":
        print(
            "hydrating spill envelope "
            f"s3://{result.get('result_s3_bucket')}/{result.get('result_s3_key')} "
            f"({result.get('result_bytes')} bytes)"
        )
        result = await asyncio.to_thread(
            hydrate_docling_result_envelope,
            result,
            default_bucket=bucket,
        )

    status = result.get("status")
    body = result.get("result") or {}
    md = body.get("markdown", "")
    tiers = body.get("hierarchical_chunks")
    print(f"status={status} markdown_len={len(md)}")
    if tiers:
        print(f"hierarchical_chunks tier_counts={tiers.get('tier_counts')}")
    if result.get("error"):
        print(f"error={result['error']}")
    await nc.close()

    if status != "success":
        return 1
    if args.hierarchical:
        if not tiers or not tiers.get("records"):
            print("FAIL: hierarchical_chunk=true but no hierarchical_chunks in result")
            return 1
        print("OK hierarchical chunk pipeline")
    elif len(md) == 0:
        print("WARN: empty markdown")
    else:
        print("OK direct parse pipeline")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
