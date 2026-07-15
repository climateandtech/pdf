#!/usr/bin/env python3
"""CPU Docling chunk worker: docs.chunk.* → HybridChunker → docs.result.*."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

from config import NatsConfig
from hierarchical_chunker import chunk_hierarchical, warmup_chunk_tokenizer
from parse_artifact_storage import load_parse_artifacts, parse_artifact_metadata
from result_publish import (
    build_s3_envelope,
    build_slim_chunk_result,
    hierarchical_records_s3_key,
    publish_docling_result,
    result_envelope_s3_key,
    upload_hierarchical_records_jsonl,
)
from s3_client import S3DocumentClient
from s3_config import S3Config
from worker_ack import ack_heartbeat, ensure_pull_subscribe, is_retryable_error

logger = logging.getLogger(__name__)

CHUNK_JOB_TIMEOUT_S = int(os.getenv("CHUNK_JOB_TIMEOUT_S", "3600"))
HEARTBEAT_PATH = Path(
    os.getenv(
        "CHUNK_WORKER_HEARTBEAT_PATH",
        "/home/smoldocling/apps/pdf/chunk-worker.heartbeat",
    )
)


def _touch_heartbeat() -> None:
    """Liveness signal for systemd watchdog / external monitors."""
    try:
        HEARTBEAT_PATH.write_text(str(time.time()), encoding="utf-8")
    except OSError as exc:
        logger.warning("heartbeat write failed: %s", exc)


class DoclingChunkWorker:
    """Runs Docling HybridChunker on S3-stored parse JSON (no CUDA)."""

    def __init__(self) -> None:
        self.s3_config = S3Config()
        self.nats_config = NatsConfig()
        self.client = S3DocumentClient(self.s3_config, self.nats_config)

    async def setup(self) -> None:
        """Initialize NATS and S3 clients."""
        await self.client.setup()
        print(f"✅ Chunk Worker connected to NATS: {self.nats_config.url}")
        print(f"✅ Chunk Worker connected to S3: {self.s3_config.bucket_name}")

    async def _result_exists(self, request_id: str) -> bool:
        """Return True when results/{request_id}.json already exists on S3."""
        key = result_envelope_s3_key(request_id)
        try:
            async with self.client.s3_client() as s3:
                await s3.head_object(Bucket=self.s3_config.bucket_name, Key=key)
            return True
        except (OSError, RuntimeError, ValueError, KeyError, TypeError) as exc:
            logger.debug("result head_object miss for %s: %s", key, exc)
            return False

    async def _republish_existing_result(self, job: dict[str, Any], request_id: str) -> str:
        """Republish S3 envelope for an already-chunked result without re-chunking."""
        s3_key = result_envelope_s3_key(request_id)
        records_key = hierarchical_records_s3_key(request_id)
        response = {
            "request_id": request_id,
            "status": "success",
            "backend_resource_id": job.get("backend_resource_id"),
            "result_storage": "s3",
            "result_s3_bucket": self.s3_config.bucket_name,
            "result_s3_key": s3_key,
            "result": {
                "markdown": "",
                "structured_data": {},
                "metadata": job.get("metadata") or {},
                "parse_artifacts": parse_artifact_metadata(job),
                "hierarchical_chunks": {
                    "records_s3_key": records_key,
                    "result_s3_bucket": self.s3_config.bucket_name,
                    "result_s3_key": s3_key,
                },
            },
        }
        # Prefer existing full/slim object: publish pointer envelope only.
        envelope = build_s3_envelope(
            response,
            s3_bucket=self.s3_config.bucket_name,
            s3_key=s3_key,
            full_bytes=0,
        )
        envelope["result"]["parse_artifacts"] = parse_artifact_metadata(job)
        envelope["result"]["hierarchical_chunks"] = response["result"]["hierarchical_chunks"]
        subject = f"{self.nats_config.subject_prefix}.result.{request_id}"
        body = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
        await self.client.js.publish(subject, body)
        return "s3-skip"

    async def _publish_error(
        self,
        *,
        request_id: str,
        job: dict[str, Any],
        error: str,
    ) -> None:
        error_response = {
            "request_id": request_id,
            "status": "error",
            "backend_resource_id": job.get("backend_resource_id"),
            "error": error,
        }
        subject = f"{self.nats_config.subject_prefix}.result.{request_id}"
        await publish_docling_result(self.client, subject, error_response)

    async def process_chunk_job(self, message: Any) -> None:
        """Load parse artifacts, chunk, and publish docs.result."""
        request_id = "unknown"
        job: dict[str, Any] = {}
        try:
            job = json.loads(message.data.decode())
            request_id = str(job.get("request_id") or "unknown")
            print(f"📨 Chunk Worker: job {request_id}")
            _touch_heartbeat()

            existing = await self._result_exists(request_id)
            if existing:
                mode = await self._republish_existing_result(job, request_id)
                print(f"📤 Chunk Worker: skipped re-chunk for {request_id} ({mode})")
                await message.ack()
                return

            structured_data, markdown = await load_parse_artifacts(self.client, job)
            _touch_heartbeat()
            hierarchical_chunks = await asyncio.wait_for(
                asyncio.to_thread(chunk_hierarchical, structured_data),
                timeout=CHUNK_JOB_TIMEOUT_S,
            )
            print(
                "📚 Chunk Worker: hierarchical tiers "
                f"{hierarchical_chunks.get('tier_counts')}"
            )
            _touch_heartbeat()

            records = hierarchical_chunks.get("records") or []
            records_key, records_bytes = await upload_hierarchical_records_jsonl(
                self.client,
                request_id,
                records,
            )
            hier_pointer = {
                "tier_counts": hierarchical_chunks.get("tier_counts"),
                "record_count": len(records),
                "metrics": hierarchical_chunks.get("metrics"),
                "records_s3_key": records_key,
                "records_bytes": records_bytes,
                "result_s3_bucket": self.s3_config.bucket_name,
            }
            # Drop in-memory records before building the NATS/S3 envelope.
            del records
            hierarchical_chunks = hier_pointer

            parse_artifacts = parse_artifact_metadata(job)
            response = build_slim_chunk_result(
                request_id=request_id,
                backend_resource_id=job.get("backend_resource_id"),
                parse_mode=job.get("parse_mode"),
                docling_options=job.get("docling_options") or {},
                parse_artifacts=parse_artifacts,
                hierarchical_chunks=hierarchical_chunks,
                metadata=job.get("metadata") or {},
                markdown=markdown,
            )
            # Free markdown / structured parse payload before spill serialize.
            del structured_data
            del markdown

            subject = f"{self.nats_config.subject_prefix}.result.{request_id}"
            mode = await publish_docling_result(self.client, subject, response)
            print(f"📤 Chunk Worker: published docs.result for {request_id} ({mode})")
            await message.ack()
        except asyncio.TimeoutError:
            print(f"❌ Chunk Worker: timeout {request_id} after {CHUNK_JOB_TIMEOUT_S}s")
            try:
                await self._publish_error(
                    request_id=request_id,
                    job=job,
                    error=f"chunk_hierarchical timed out after {CHUNK_JOB_TIMEOUT_S}s",
                )
            except Exception:
                logger.exception("chunk worker could not publish timeout result")
            await message.term()
        except MemoryError as exc:
            print(f"❌ Chunk Worker: OOM {request_id}: {exc}")
            try:
                await self._publish_error(
                    request_id=request_id, job=job, error=f"MemoryError: {exc}"
                )
            except Exception:
                logger.exception("chunk worker could not publish OOM result")
            await message.term()
        except (
            json.JSONDecodeError,
            TypeError,
            ValueError,
            KeyError,
            RuntimeError,
        ) as exc:
            print(f"❌ Chunk Worker: failed {request_id}: {exc}")
            try:
                await self._publish_error(request_id=request_id, job=job, error=str(exc))
            except Exception:
                logger.exception("chunk worker could not publish error result")
            await message.term()
        except OSError as exc:
            print(f"❌ Chunk Worker: transient {request_id}: {exc}")
            try:
                await self._publish_error(request_id=request_id, job=job, error=str(exc))
            except Exception:
                logger.exception("chunk worker could not publish error result")
            if is_retryable_error(exc):
                await message.nak()
            else:
                await message.term()

    async def start_listening(self) -> None:
        """Pull docs.chunk.* jobs from JetStream."""
        prefix = self.nats_config.subject_prefix
        chunk_subject = f"{prefix}.chunk.*"
        print(f"🎧 Chunk Worker: listening on '{chunk_subject}'")

        subscription = await ensure_pull_subscribe(
            self.client.js,
            subject=chunk_subject,
            durable="docling_chunk_worker",
            stream=self.nats_config.stream_name,
        )

        processed_count = 0
        try:
            while True:
                _touch_heartbeat()
                try:
                    messages = await subscription.fetch(batch=1, timeout=10)
                    if messages:
                        for message in messages:
                            async with ack_heartbeat(message):
                                await self.process_chunk_job(message)
                            processed_count += 1
                            print(f"📈 Chunk Worker: processed {processed_count} jobs")
                            _touch_heartbeat()
                    else:
                        print("⏱️  Chunk Worker: no messages, waiting...")
                except asyncio.TimeoutError:
                    continue
                except Exception as loop_err:
                    from nats.errors import ConnectionClosedError

                    if isinstance(loop_err, ConnectionClosedError):
                        print(f"⚠️  NATS connection closed: {loop_err} — reconnecting...")
                        await self.client.close()
                        await self.client.setup()
                        subscription = await ensure_pull_subscribe(
                            self.client.js,
                            subject=chunk_subject,
                            durable="docling_chunk_worker",
                            stream=self.nats_config.stream_name,
                        )
                        continue
                    raise
        except KeyboardInterrupt:
            print("\n👋 Chunk Worker: interrupted")
        finally:
            print(f"📊 Chunk Worker: final count {processed_count}")
            await self.client.close()


async def main() -> None:
    """Entry point for GPU-host chunk worker (bge-m3 tokenizer, CPU HybridChunker)."""
    print("🚀 Starting Docling Chunk Worker (GPU host, bge-m3 tokenizer)")
    print("=" * 50)
    model_name = warmup_chunk_tokenizer()
    print(f"✅ Chunk tokenizer ready: {model_name}")
    _touch_heartbeat()
    worker = DoclingChunkWorker()
    await worker.setup()
    await worker.start_listening()


if __name__ == "__main__":
    asyncio.run(main())
