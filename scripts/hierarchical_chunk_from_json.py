#!/usr/bin/env python3
"""Run GPU Docling hierarchical chunking on exported docling_json fixtures."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from hierarchical_chunker import chunk_hierarchical  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Chunk stored Docling JSON with GPU-native hierarchical chunkers"
    )
    parser.add_argument("--json", type=Path, required=True, help="docling_json fixture path")
    parser.add_argument("--output", type=Path, required=True, help="hierarchical_chunks.json output")
    parser.add_argument("--micro-tokens", type=int, default=150)
    parser.add_argument("--child-tokens", type=int, default=512)
    parser.add_argument("--parent-max-tokens", type=int, default=2000)
    args = parser.parse_args()

    if not args.json.is_file():
        print(f"Missing fixture: {args.json}", file=sys.stderr)
        return 2

    structured = json.loads(args.json.read_text(encoding="utf-8"))
    payload = chunk_hierarchical(
        structured,
        micro_tokens=args.micro_tokens,
        child_tokens=args.child_tokens,
        parent_max_tokens=args.parent_max_tokens,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"Wrote {args.output} "
        f"(tiers={payload['tier_counts']}, embed={payload['metrics']['embed_vector_count']})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
