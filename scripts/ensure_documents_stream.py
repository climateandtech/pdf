#!/usr/bin/env python3
"""Ensure DOCUMENTS JetStream subjects from config/nats_streams.yaml.

Treat subject-list changes like DB migrations:

* Default (deploy/recover): **additive only** — union yaml desired subjects with
  whatever is already live. Never strip a subject that production already has.
* ``--prune``: replace with yaml exactly (can drop subjects). Requires
  ``NATS_STREAM_PRUNE_OK=1`` — same class of footgun as ``git reset --hard``.

Desired subjects come from ``config/nats_streams.yaml`` (kept in sync with
ct-platform). Hardcoded fallback matches the yaml DOCUMENTS list if yaml is
missing.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Any

import nats
from dotenv import load_dotenv
from nats.js.api import RetentionPolicy, StorageType, StreamConfig

_FALLBACK_DOCUMENTS_SUBJECTS = [
    "docs.upload.*",
    "docs.process.*",
    "docs.chunk.*",
    "docs.embed.*",
    "docs.embed.start.*",
    "docs.result.*",
    "document.*",
]


def merge_stream_subjects(
    desired: list[str],
    live: list[str] | None,
    *,
    prune: bool,
) -> list[str]:
    """Compute the subject list to apply.

    Additive (prune=False): desired first, then any live-only subjects kept.
    Prune (prune=True): exactly desired — live-only subjects are dropped.
    """
    desired_unique = list(dict.fromkeys(desired))
    if prune or not live:
        return desired_unique
    live_unique = list(dict.fromkeys(live))
    extras = [s for s in live_unique if s not in desired_unique]
    return desired_unique + extras


def load_documents_subjects(repo_root: Path | None = None) -> list[str]:
    """Load DOCUMENTS subjects from config/nats_streams.yaml (SoT)."""
    root = repo_root or Path(__file__).resolve().parents[1]
    yaml_path = root / "config" / "nats_streams.yaml"
    if not yaml_path.is_file():
        print(f"WARN: missing {yaml_path}; using fallback subjects", file=sys.stderr)
        return list(_FALLBACK_DOCUMENTS_SUBJECTS)
    try:
        import yaml
    except ImportError:
        print("WARN: PyYAML missing; using fallback subjects", file=sys.stderr)
        return list(_FALLBACK_DOCUMENTS_SUBJECTS)

    data: dict[str, Any] = yaml.safe_load(yaml_path.read_text(encoding="utf-8")) or {}
    subjects = ((data.get("streams") or {}).get("DOCUMENTS") or {}).get("subjects")
    if not isinstance(subjects, list) or not subjects:
        print(f"WARN: no DOCUMENTS.subjects in {yaml_path}; using fallback", file=sys.stderr)
        return list(_FALLBACK_DOCUMENTS_SUBJECTS)
    return [str(s) for s in subjects]


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


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prune",
        action="store_true",
        help="Replace subjects with yaml exactly (drops live-only). Needs NATS_STREAM_PRUNE_OK=1",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Print desired vs live; exit 1 if yaml subjects are missing from live",
    )
    return parser.parse_args(argv)


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.prune and os.environ.get("NATS_STREAM_PRUNE_OK") != "1":
        print(
            "ERROR: --prune drops live stream subjects. "
            "Confirm with NATS_STREAM_PRUNE_OK=1 (treat like a DB down-migration).",
            file=sys.stderr,
        )
        return 2

    _load_env()
    if not os.getenv("NATS_URL"):
        print("NATS_URL required", file=sys.stderr)
        return 2

    desired = load_documents_subjects()
    nc = await nats.connect(_connection_url())
    js = nc.jetstream()
    try:
        try:
            info = await js.stream_info("DOCUMENTS")
            live = list(info.config.subjects or [])
            print("before:", live, "msgs=", info.state.messages)
        except Exception as exc:
            if "not found" in str(exc).lower() or "10059" in str(exc):
                live = []
                info = None
            else:
                raise

        if args.verify_only:
            missing = [s for s in desired if s not in live]
            print("desired:", desired)
            if missing:
                print("MISSING from live:", missing, file=sys.stderr)
                return 1
            extras = [s for s in live if s not in desired]
            if extras:
                print("live-only (kept under additive ensure):", extras)
            print("verify OK")
            return 0

        subjects = merge_stream_subjects(desired, live, prune=args.prune)
        if live and subjects == live:
            print("after:", live, "(no change)")
            return 0

        cfg = StreamConfig(
            name="DOCUMENTS",
            subjects=subjects,
            storage=StorageType.FILE,
            retention=RetentionPolicy.LIMITS,
            max_age=86400,
            max_msgs=100_000,
        )
        if info is None:
            await js.add_stream(cfg)
            print("created DOCUMENTS stream")
        else:
            await js.update_stream(cfg)
            mode = "prune" if args.prune else "additive"
            print(f"updated DOCUMENTS stream ({mode})")

        info2 = await js.stream_info("DOCUMENTS")
        print("after:", info2.config.subjects, "msgs=", info2.state.messages)
        return 0
    finally:
        await nc.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
