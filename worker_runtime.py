"""GPU bootstrap, memory profiles, and torch/torchvision import order for Docling."""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

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


def verify_cudnn_conv2d() -> None:
    """Raise when bundled cuDNN cannot execute a minimal CUDA conv2d."""
    import torch

    if not torch.cuda.is_available():
        raise RuntimeError("CUDA is not available")
    torch.backends.cudnn.enabled = True
    torch.backends.cudnn.benchmark = False
    torch.nn.functional.conv2d(
        torch.zeros(1, 3, 8, 8, device="cuda"),
        torch.randn(4, 3, 3, 3, device="cuda"),
    )
    torch.cuda.synchronize()


def warmup_cuda_cudnn() -> bool:
    """Prime CUDA/cuDNN before Docling layout models; return False when probe fails."""
    import torch

    if not torch.cuda.is_available():
        return True
    try:
        verify_cudnn_conv2d()
    except RuntimeError as exc:
        logger.warning("cuDNN warmup failed (%s); Docling may CPU-fallback or fail CUDA parse", exc)
        print(f"⚠️  cuDNN warmup failed: {exc}")
        return False
    return True


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
    warmup_cuda_cudnn()
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
