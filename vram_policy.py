"""VRAM policy: Ollama reserve gate, CUDA OOM detection, CPU fallback options."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
from typing import Any

_CUDA_OOM_MARKERS = (
    "cuda out of memory",
    "cudnn",
    "cudnn_status",
    "out of memory",
    "allocat",
)

_DEFAULT_BATCH_LIMITS = {
    "layout_batch_size": 1,
    "ocr_batch_size": 1,
    "table_batch_size": 1,
    "queue_max_size": 1,
}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name, str(default)).strip()
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, str(default)).strip()
    try:
        return int(raw)
    except ValueError:
        return default


def get_gpu_vram_stats_gb() -> dict[str, float]:
    """Return total, used, and free GPU memory in GB."""
    try:
        import torch

        if torch.cuda.is_available():
            free_b, total_b = torch.cuda.mem_get_info()
            total_gb = total_b / (1024**3)
            free_gb = free_b / (1024**3)
            return {
                "total_gb": total_gb,
                "used_gb": total_gb - free_gb,
                "free_gb": free_gb,
            }
    except (ImportError, RuntimeError, OSError):
        pass

    try:
        nvidia_smi = shutil.which("nvidia-smi") or "nvidia-smi"
        out = subprocess.check_output(
            [
                nvidia_smi,
                "--query-gpu=memory.total,memory.used,memory.free",
                "--format=csv,noheader,nounits",
            ],
            text=True,
            timeout=5,
        ).strip()
        total_mib, used_mib, free_mib = (float(x.strip()) for x in out.split(",")[:3])
        return {
            "total_gb": total_mib / 1024,
            "used_gb": used_mib / 1024,
            "free_gb": free_mib / 1024,
        }
    except (subprocess.SubprocessError, OSError, ValueError):
        return {"total_gb": 20.0, "used_gb": 0.0, "free_gb": 20.0}


def is_cuda_gpu_failure(exc: BaseException) -> bool:
    """Return True when an exception indicates CUDA OOM or cuDNN init failure."""
    if exc.__class__.__name__ == "OutOfMemoryError":
        return True
    msg = str(exc).lower()
    return any(marker in msg for marker in _CUDA_OOM_MARKERS)


def stable_options_hash(options: dict[str, Any] | None) -> str:
    """Hash docling options for converter cache keys."""
    payload = json.dumps(options or {}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def merge_batch_limits(options: dict[str, Any]) -> dict[str, Any]:
    """Apply threaded-pipeline batch=1 defaults unless caller overrides."""
    merged = dict(options)
    for key, value in _DEFAULT_BATCH_LIMITS.items():
        merged.setdefault(key, value)
    return merged


def cpu_fallback_options(options: dict[str, Any]) -> dict[str, Any]:
    """Build CPU retry options with configured thread count."""
    merged = merge_batch_limits(dict(options))
    merged["accelerator_device"] = "cpu"
    merged["num_threads"] = _env_int("DOCLING_CPU_NUM_THREADS", 8)
    merged["device_reason"] = "oom_retry"
    merged["oom_retried"] = True
    return merged


def resolve_accelerator_device(options: dict[str, Any] | None) -> dict[str, Any]:
    """Choose cuda vs cpu before building DocumentConverter (Ollama reserve gate)."""
    merged = merge_batch_limits(dict(options or {}))
    preference = os.getenv("DOCLING_ACCELERATOR_PREFERENCE", "auto").strip().lower()

    if preference == "cpu":
        merged["accelerator_device"] = "cpu"
        merged["num_threads"] = _env_int("DOCLING_CPU_NUM_THREADS", 8)
        merged["device_reason"] = "forced_cpu"
        return merged

    if preference == "cuda":
        merged["accelerator_device"] = "cuda"
        merged["device_reason"] = "forced_cuda"
        return merged

    stats = get_gpu_vram_stats_gb()
    total_gb = stats["total_gb"]
    used_gb = stats["used_gb"]
    ollama_reserve_gb = _env_float("DOCLING_OLLAMA_RESERVE_GB", 12.0)
    cold_cuda_gb = _env_float("DOCLING_VRAM_COLD_CUDA_GB", 8.0)
    docling_cap_gb = _env_float("DOCLING_GPU_CAP_GB", 8.0)

    max_docling_physical = max(0.0, total_gb - ollama_reserve_gb)
    projected_used = used_gb + cold_cuda_gb

    merged["vram_total_gb"] = round(total_gb, 2)
    merged["vram_used_gb"] = round(used_gb, 2)
    merged["vram_free_gb"] = round(stats["free_gb"], 2)

    if cold_cuda_gb > max_docling_physical or projected_used > total_gb - 0.5:
        merged["accelerator_device"] = "cpu"
        merged["num_threads"] = _env_int("DOCLING_CPU_NUM_THREADS", 8)
        merged["device_reason"] = "ollama_reserve"
        return merged

    if cold_cuda_gb > docling_cap_gb + 0.5:
        merged["accelerator_device"] = "cpu"
        merged["num_threads"] = _env_int("DOCLING_CPU_NUM_THREADS", 8)
        merged["device_reason"] = "exceeds_cap"
        return merged

    merged["accelerator_device"] = "cuda"
    merged["device_reason"] = "vram_ok"
    return merged
