"""CUDA is the production device. CPU is an explicit opt-in."""

from __future__ import annotations

import os


def requested_device() -> str:
    """Return ``cuda`` or ``cpu`` from ``FORECAST_DEVICE`` (default cuda)."""
    raw = os.getenv("FORECAST_DEVICE", "cuda").strip().lower() or "cuda"
    if raw in {"cuda", "gpu"}:
        return "cuda"
    if raw == "cpu":
        return "cpu"
    raise RuntimeError("FORECAST_DEVICE must be cuda or cpu")


def resolve_device() -> str:
    """Return the torch device. Missing CUDA is an error unless CPU was requested."""
    choice = requested_device()
    if choice == "cpu":
        return "cpu"
    import torch

    if not torch.cuda.is_available():
        raise RuntimeError(
            "CUDA is not available. Set FORECAST_DEVICE=cpu to opt in to CPU."
        )
    return "cuda"
