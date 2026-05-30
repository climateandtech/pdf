#!/usr/bin/env python3
"""Verify torch/torchvision/docling import order (run on GPU after pip install)."""
from worker_runtime import bootstrap_gpu


def main() -> int:
    profile = bootstrap_gpu()
    from docling.document_converter import DocumentConverter  # noqa: WPS433

    DocumentConverter()
    print(f"OK: DocumentConverter loads with profile={profile}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
