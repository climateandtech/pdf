"""GPU bootstrap, memory profiles, and torch/torchvision import order for Docling."""

from __future__ import annotations

import os
from typing import Optional

# full / nats  → production capped path (8GB Docling + CPU fallback via vram_policy)
# capped_5gb / b / 5gb → benchmark (~5GB VRAM cap via set_per_process_memory_fraction)
PROFILE_ALIASES = {
    "full": "20gb_capped",
    "a": "20gb_capped",
    "nats": "20gb_capped",
    "20gb_nats": "20gb_nats",
    "capped_5gb": "capped_5gb",
    "b": "capped_5gb",
    "5gb": "capped_5gb",
}


def resolve_profile_name(raw: Optional[str] = None) -> str:
    value = (raw or os.getenv("DOCLING_GPU_PROFILE", "full")).strip().lower()
    return PROFILE_ALIASES.get(value, value)


def bootstrap_gpu(profile: Optional[str] = None) -> str:
    """
    Import torch/torchvision before Docling (avoids circular import) and apply VRAM profile.

    Returns the gpu_memory_config preset name applied.
    """
    os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")

    import torch  # noqa: WPS433 — must load before docling / easyocr
    import torchvision  # noqa: F401, WPS433

    config_name = resolve_profile_name(profile)
    from gpu_memory_config import GPUMemoryOptimizer

    if config_name in ("capped_5gb", "20gb_capped"):
        default_cap = "5" if config_name == "capped_5gb" else "8"
        cap_gb = float(os.getenv("DOCLING_GPU_CAP_GB", default_cap))
        if torch.cuda.is_available():
            total_gb = torch.cuda.get_device_properties(0).total_memory / (1024**3)
            fraction = min(0.95, cap_gb / total_gb)
            torch.cuda.set_per_process_memory_fraction(fraction)
            print(f"🎛️ GPU cap: {cap_gb}GB ({fraction:.0%} of {total_gb:.1f}GB total VRAM)")

    if config_name not in GPUMemoryOptimizer.CONFIGS:
        config_name = GPUMemoryOptimizer.detect_optimal_config()

    GPUMemoryOptimizer.apply_config(config_name)
    GPUMemoryOptimizer.print_memory_status()
    print(f"✅ DOCLING_GPU_PROFILE → {config_name}")
    return config_name


def cleanup_gpu_memory(force: bool = False) -> None:
    """Release cached CUDA allocations after each document."""
    import gc

    import torch

    gc.collect()
    if torch.cuda.is_available():
        torch.cuda.empty_cache()
        if force:
            torch.cuda.synchronize()
    gc.collect()
