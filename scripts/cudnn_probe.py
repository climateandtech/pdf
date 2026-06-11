#!/usr/bin/env python3
"""Verify CUDA/cuDNN conv2d works (run on GPU host after pip install)."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from worker_runtime import verify_cudnn_conv2d  # noqa: E402


def main() -> int:
    import torch

    print(f"torch={torch.__version__} cuda={torch.version.cuda} cudnn={torch.backends.cudnn.version()}")
    verify_cudnn_conv2d()
    print("OK: cuDNN conv2d probe passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
