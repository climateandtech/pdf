#!/usr/bin/env python3
"""Verify torch/torchvision/docling import order (run on GPU after pip install)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from worker_runtime import bootstrap_gpu


def main() -> int:
    profile = bootstrap_gpu()
    from docling.document_converter import DocumentConverter  # noqa: WPS433

    DocumentConverter()
    print(f"OK: DocumentConverter loads with profile={profile}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
