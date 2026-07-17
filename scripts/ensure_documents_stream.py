#!/usr/bin/env python3
"""Ensure DOCUMENTS JetStream includes docs.chunk.*, docs.embed.*, and other pipeline subjects."""

from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import nats
from dotenv import load_dotenv
from nats.js.api import RetentionPolicy, StorageType, StreamConfig

DOCUMENTS_SUBJECTS = [
    "docs.upload.*",
    "docs.process.*",
    "docs.chunk.*",
    "docs.embed.*",
    "docs.embed.start.*",
    "docs.result.*",
    "document.*",
]


def _load_env() -> None:
    for path in (
        Path.cwd() / ".env",
        Path(__file__).resolve().parents[1] / ".env",
    ):
        if path.is_file():
            load_dotenv(path)
            return


def _connection_url() -> str:
    url = os.environ["NATS_URL"]
    token = os.environ.get("NATS_TOKEN")
    if token and "@" not in url.split("://", 1)[-1]:
        return url.replace("nats://", f"nats://{token}@")
    return url


async def main() -> int:
    _load_env()
    if not os.getenv("NATS_URL"):
        print("NATS_URL required", file=sys.stderr)
        return 2

    nc = await nats.connect(_connection_url())
    js = nc.jetstream()
    cfg = StreamConfig(
        name="DOCUMENTS",
        subjects=DOCUMENTS_SUBJECTS,
        storage=StorageType.FILE,
        retention=RetentionPolicy.LIMITS,
        max_age=86400,
        max_msgs=100_000,
    )
    try:
        info = await js.stream_info("DOCUMENTS")
        print("before:", info.config.subjects)
        await js.update_stream(cfg)
        print("updated DOCUMENTS stream")
    except Exception as exc:
        if "not found" in str(exc).lower() or "10059" in str(exc):
            await js.add_stream(cfg)
            print("created DOCUMENTS stream")
        else:
            raise

    info = await js.stream_info("DOCUMENTS")
    print("after:", info.config.subjects, "msgs=", info.state.messages)
    await nc.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
