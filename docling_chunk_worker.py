#!/usr/bin/env python3
"""CPU Docling chunk worker: docs.chunk.* → HybridChunker → docs.result.*."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from config import NatsConfig
from hierarchical_chunker import chunk_hierarchical
from parse_artifact_storage import load_parse_artifacts, parse_artifact_metadata
from result_publish import publish_docling_result
from s3_client import S3DocumentClient
from s3_config import S3Config

logger = logging.getLogger(__name__)


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

    async def process_chunk_job(self, message: Any) -> None:
        """Load parse artifacts, chunk, and publish docs.result."""
        request_id = "unknown"
        job: dict[str, Any] = {}
        try:
            job = json.loads(message.data.decode())
            request_id = str(job.get("request_id") or "unknown")
            print(f"📨 Chunk Worker: job {request_id}")

            structured_data, markdown = await load_parse_artifacts(self.client, job)
            hierarchical_chunks = await asyncio.to_thread(
                chunk_hierarchical,
                structured_data,
            )
            print(
                "📚 Chunk Worker: hierarchical tiers "
                f"{hierarchical_chunks.get('tier_counts')}"
            )

            parse_artifacts = parse_artifact_metadata(job)
            response = {
                "request_id": request_id,
                "status": "success",
                "backend_resource_id": job.get("backend_resource_id"),
                "parse_mode": job.get("parse_mode"),
                "docling_options": job.get("docling_options") or {},
                "result": {
                    "text": markdown,
                    "markdown": markdown,
                    "structured_data": structured_data,
                    "metadata": job.get("metadata") or {},
                    "parse_artifacts": parse_artifacts,
                    "hierarchical_chunks": hierarchical_chunks,
                },
            }
            subject = f"{self.nats_config.subject_prefix}.result.{request_id}"
            mode = await publish_docling_result(self.client, subject, response)
            print(f"📤 Chunk Worker: published docs.result for {request_id} ({mode})")
            await message.ack()
        except (
            json.JSONDecodeError,
            OSError,
            RuntimeError,
            TypeError,
            ValueError,
            KeyError,
        ) as exc:
            print(f"❌ Chunk Worker: failed {request_id}: {exc}")
            error_response = {
                "request_id": request_id,
                "status": "error",
                "backend_resource_id": job.get("backend_resource_id"),
                "error": str(exc),
            }
            try:
                subject = f"{self.nats_config.subject_prefix}.result.{request_id}"
                await publish_docling_result(self.client, subject, error_response)
            except Exception:
                logger.exception("chunk worker could not publish error result")
            await message.nak()

    async def start_listening(self) -> None:
        """Pull docs.chunk.* jobs from JetStream."""
        prefix = self.nats_config.subject_prefix
        chunk_subject = f"{prefix}.chunk.*"
        print(f"🎧 Chunk Worker: listening on '{chunk_subject}'")

        subscription = await self.client.js.pull_subscribe(
            subject=chunk_subject,
            durable="docling_chunk_worker",
            stream=self.nats_config.stream_name,
        )

        processed_count = 0
        try:
            while True:
                try:
                    messages = await subscription.fetch(batch=1, timeout=10)
                    if messages:
                        for message in messages:
                            await self.process_chunk_job(message)
                            processed_count += 1
                            print(f"📈 Chunk Worker: processed {processed_count} jobs")
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
                        subscription = await self.client.js.pull_subscribe(
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
    """Entry point for CPU chunk worker."""
    print("🚀 Starting Docling Chunk Worker (CPU)")
    print("=" * 50)
    worker = DoclingChunkWorker()
    await worker.setup()
    await worker.start_listening()


if __name__ == "__main__":
    asyncio.run(main())
