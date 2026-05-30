#!/usr/bin/env python3
"""
Tail worker.log and push lines to Loki (optional GPU visibility).

Set in .env on GPU:
  LOKI_PUSH_URL=https://<grafana-host>/loki/api/v1/push   # or direct Loki :3100
  LOKI_PUSH_LABELS=job=gpu-docling,host=176.9.98.94

Run via systemd or cron every minute, or as background from start_worker.sh.
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

LOG_FILE = Path(os.getenv("GPU_WORKER_LOG", Path(__file__).resolve().parents[1] / "worker.log"))
STATE_FILE = Path(os.getenv("LOKI_SHIP_STATE", LOG_FILE.with_suffix(".loki.offset")))
BATCH_LINES = int(os.getenv("LOKI_SHIP_BATCH", "50"))


def push_lines(lines: list[str]) -> None:
    url = (os.getenv("LOKI_PUSH_URL") or "").strip()
    if not url or not lines:
        return

    labels_raw = os.getenv("LOKI_PUSH_LABELS", "job=gpu-docling,service=docling_worker")
    labels = {}
    for part in labels_raw.split(","):
        if "=" in part:
            k, v = part.split("=", 1)
            labels[k.strip()] = v.strip()

    now_ns = str(int(time.time() * 1_000_000_000))
    streams = [
        {
            "stream": labels,
            "values": [[str(int(time.time() * 1_000_000_000) + i), line] for i, line in enumerate(lines)],
        }
    ]
    body = json.dumps({"streams": streams}).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            resp.read()
    except urllib.error.HTTPError as exc:
        print(f"Loki push failed: {exc.code} {exc.read()[:200]}", file=sys.stderr)


def main() -> int:
    follow = "--follow" in sys.argv or os.getenv("LOKI_SHIP_FOLLOW", "").lower() in ("1", "true", "yes")
    if follow:
        interval = float(os.getenv("LOKI_SHIP_INTERVAL", "5"))
        while True:
            run_once()
            time.sleep(interval)
        return 0
    return run_once()


def run_once() -> int:
    if not LOG_FILE.is_file():
        return 0

    offset = int(STATE_FILE.read_text()) if STATE_FILE.is_file() else 0
    with LOG_FILE.open() as fh:
        fh.seek(offset)
        chunk = fh.readlines()
        new_offset = fh.tell()

    if not chunk:
        return 0

    lines = [ln.rstrip("\n") for ln in chunk if ln.strip()]
    for i in range(0, len(lines), BATCH_LINES):
        push_lines(lines[i : i + BATCH_LINES])
    STATE_FILE.write_text(str(new_offset))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
