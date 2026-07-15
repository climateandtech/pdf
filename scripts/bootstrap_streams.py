#!/usr/bin/env python3
"""Bootstrap JetStream on ct-nats from config/nats_streams.yaml (GPU / prod :4222)."""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path

import nats
import yaml
from dotenv import load_dotenv
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    DeliverPolicy,
    DiscardPolicy,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_YAML = ROOT / "config" / "nats_streams.yaml"

LEGACY_STREAM_NAMES = frozenset({"DOCUMENTS_results"})


def load_yaml(path: Path) -> dict:
    with path.open() as f:
        return yaml.safe_load(f)


def nats_connect_url_and_kwargs() -> tuple[str, dict]:
    url = os.environ["NATS_URL"]
    if "@" in url.split("://", 1)[-1]:
        scheme, rest = url.split("://", 1)
        url = f"{scheme}://{rest.split('@', 1)[-1]}"
    token = os.environ.get("NATS_TOKEN")
    kwargs: dict = {
        "connect_timeout": int(os.environ.get("NATS_CONNECT_TIMEOUT", "10")),
        "max_reconnect_attempts": 0,
    }
    if token:
        kwargs["token"] = token
    return url, kwargs


async def connect_nats():
    url, kwargs = nats_connect_url_and_kwargs()
    return await nats.connect(url, **kwargs)


def retention_from_yaml(value: str) -> RetentionPolicy:
    mapping = {
        "limits": RetentionPolicy.LIMITS,
        "interest": RetentionPolicy.INTEREST,
        "workqueue": RetentionPolicy.WORK_QUEUE,
    }
    return mapping.get(value.lower(), RetentionPolicy.LIMITS)


def storage_from_yaml(value: str) -> StorageType:
    return StorageType.MEMORY if value.lower() == "memory" else StorageType.FILE


def stream_config_from_yaml(name: str, spec: dict) -> StreamConfig:
    return StreamConfig(
        name=spec.get("name", name),
        subjects=spec["subjects"],
        storage=storage_from_yaml(spec.get("storage", "file")),
        retention=retention_from_yaml(spec.get("retention", "limits")),
        max_age=spec.get("max_age", 3600),
        max_msgs=spec.get("max_msgs", 100_000),
        discard=DiscardPolicy.OLD,
        description=spec.get("description", ""),
    )


async def list_stream_names(js) -> list[str]:
    names: list[str] = []
    async for info in js.streams_info():
        names.append(info.config.name)
    return sorted(names)


async def delete_stream(js, name: str, *, dry_run: bool) -> None:
    if dry_run:
        print(f"  [dry-run] delete stream {name}")
        return
    await js.delete_stream(name)
    print(f"  deleted stream {name}")


async def reset_streams(
    js,
    yaml_streams: dict,
    *,
    purge_all: bool,
    dry_run: bool,
) -> None:
    existing = await list_stream_names(js)
    print("existing streams:", existing or "(none)")

    to_delete: set[str] = set()
    if purge_all:
        to_delete.update(existing)
    else:
        to_delete.update(LEGACY_STREAM_NAMES & set(existing))
        for key in yaml_streams:
            cfg = yaml_streams[key]
            name = cfg.get("name", key)
            if name in existing:
                to_delete.add(name)

    for name in sorted(to_delete):
        await delete_stream(js, name, dry_run=dry_run)

    for key, spec in yaml_streams.items():
        cfg = stream_config_from_yaml(key, spec)
        print(f"create stream {cfg.name}: subjects={cfg.subjects} retention={cfg.retention}")
        if dry_run:
            continue
        try:
            await js.add_stream(cfg)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                await js.update_stream(cfg)
                print(f"  updated stream {cfg.name}")
            else:
                raise


async def verify_streams(js, yaml_streams: dict) -> bool:
    ok = True
    for key, spec in yaml_streams.items():
        name = spec.get("name", key)
        try:
            info = await js.stream_info(name)
        except Exception as exc:  # noqa: BLE001 — nats APIError hierarchy
            print(f"FAIL missing stream {name}: {exc}")
            ok = False
            continue
        want = set(spec["subjects"])
        have = set(info.config.subjects or [])
        if want != have:
            print(f"FAIL {name} subjects: have {have} want {want}")
            ok = False
        else:
            print(f"OK   {name} subjects={have} msgs={info.state.messages}")
    return ok


def provisioned_consumers(yaml_consumers: dict) -> list[dict]:
    out: list[dict] = []
    for group in (yaml_consumers or {}).values():
        if not isinstance(group, list):
            continue
        for spec in group:
            if not isinstance(spec, dict):
                continue
            if not (
                spec.get("durable")
                or "ack_wait" in spec
                or "max_deliver" in spec
                or "max_ack_pending" in spec
            ):
                continue
            if not spec.get("stream") or not spec.get("filter_subject"):
                continue
            out.append(spec)
    return out


def consumer_config_from_yaml(spec: dict) -> ConsumerConfig:
    durable = str(spec.get("durable") or spec["name"])
    return ConsumerConfig(
        durable_name=durable,
        ack_policy=AckPolicy.EXPLICIT,
        deliver_policy=DeliverPolicy.ALL,
        filter_subject=spec["filter_subject"],
        ack_wait=int(spec.get("ack_wait", 900)),
        max_deliver=int(spec.get("max_deliver", 5)),
        max_ack_pending=int(spec.get("max_ack_pending", 1)),
        description=str(spec.get("description") or ""),
    )


def _consumer_drift(info, want: ConsumerConfig) -> list[str]:
    have = info.config
    diffs: list[str] = []
    if int(have.ack_wait or 0) != int(want.ack_wait or 0):
        diffs.append(f"ack_wait have={have.ack_wait} want={want.ack_wait}")
    if int(have.max_deliver or 0) != int(want.max_deliver or 0):
        diffs.append(f"max_deliver have={have.max_deliver} want={want.max_deliver}")
    if int(have.max_ack_pending or 0) != int(want.max_ack_pending or 0):
        diffs.append(f"max_ack_pending have={have.max_ack_pending} want={want.max_ack_pending}")
    if (have.filter_subject or "") != (want.filter_subject or ""):
        diffs.append(
            f"filter_subject have={have.filter_subject!r} want={want.filter_subject!r}"
        )
    return diffs


async def ensure_consumer(js, spec: dict, *, dry_run: bool) -> None:
    stream = spec["stream"]
    config = consumer_config_from_yaml(spec)
    durable = config.durable_name
    assert durable
    if dry_run:
        print(f"ensure consumer {stream}/{durable}")
        return
    try:
        info = await js.consumer_info(stream, durable)
    except Exception as exc:
        if "not found" not in str(exc).lower() and "10014" not in str(exc):
            raise
        await js.add_consumer(stream, config)
        print(f"created consumer {stream}/{durable}")
        return
    diffs = _consumer_drift(info, config)
    if not diffs:
        print(f"OK   consumer {stream}/{durable}")
        return
    print(f"recreate consumer {stream}/{durable}: {'; '.join(diffs)}")
    await js.delete_consumer(stream, durable)
    await js.add_consumer(stream, config)
    print(f"recreated consumer {stream}/{durable}")


async def verify_consumers(js, yaml_consumers: dict) -> bool:
    ok = True
    for spec in provisioned_consumers(yaml_consumers):
        stream = spec["stream"]
        want = consumer_config_from_yaml(spec)
        durable = want.durable_name
        assert durable
        try:
            info = await js.consumer_info(stream, durable)
        except Exception as exc:  # noqa: BLE001 — nats APIError hierarchy
            print(f"FAIL missing consumer {stream}/{durable}: {exc}")
            ok = False
            continue
        diffs = _consumer_drift(info, want)
        if diffs:
            print(f"FAIL consumer {stream}/{durable}: {'; '.join(diffs)}")
            ok = False
        else:
            print(f"OK   consumer {stream}/{durable}")
    return ok


async def main() -> int:
    load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(description="Bootstrap JetStream from config/nats_streams.yaml")
    parser.add_argument("--config", type=Path, default=None)
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("--purge-all", action="store_true")
    parser.add_argument("--yes", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-only", action="store_true")
    args = parser.parse_args()

    config_path = args.config or DEFAULT_YAML
    if not config_path.is_file():
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 2
    if not os.getenv("NATS_URL"):
        print("Set NATS_URL (and NATS_TOKEN) in .env", file=sys.stderr)
        return 2

    cfg = load_yaml(config_path)
    yaml_streams = cfg.get("streams", {})
    if not yaml_streams:
        print("No streams in yaml", file=sys.stderr)
        return 2
    yaml_consumers = cfg.get("consumers", {})

    nc = await connect_nats()
    js = nc.jetstream()

    if args.verify_only:
        ok = await verify_streams(js, yaml_streams)
        ok = await verify_consumers(js, yaml_consumers) and ok
        await nc.close()
        return 0 if ok else 1

    if args.reset:
        if not args.yes and not args.dry_run:
            print("Refusing --reset without --yes (or use --dry-run)", file=sys.stderr)
            await nc.close()
            return 2
        await reset_streams(js, yaml_streams, purge_all=args.purge_all, dry_run=args.dry_run)
    else:
        for key, spec in yaml_streams.items():
            sc = stream_config_from_yaml(key, spec)
            if args.dry_run:
                print(f"ensure {sc.name}")
                continue
            try:
                await js.add_stream(sc)
                print(f"created {sc.name}")
            except Exception:  # noqa: BLE001 — exists / conflict
                await js.update_stream(sc)
                print(f"updated {sc.name}")

    for spec in provisioned_consumers(yaml_consumers):
        await ensure_consumer(js, spec, dry_run=args.dry_run)

    ok = True if args.dry_run else (
        await verify_streams(js, yaml_streams)
        and await verify_consumers(js, yaml_consumers)
    )
    await nc.close()
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
