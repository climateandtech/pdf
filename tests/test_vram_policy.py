"""Unit tests for VRAM policy helpers."""

from unittest.mock import patch

import pytest

from vram_policy import (
    cpu_fallback_options,
    is_cuda_gpu_failure,
    merge_batch_limits,
    resolve_accelerator_device,
)


@pytest.mark.unit
class TestVramPolicy:
    def test_merge_batch_limits_defaults_to_one(self):
        opts = merge_batch_limits({"do_table_structure": True})
        assert opts["layout_batch_size"] == 1
        assert opts["queue_max_size"] == 1

    def test_resolve_uses_cpu_when_ollama_reserve_violated(self, monkeypatch):
        monkeypatch.setenv("DOCLING_OLLAMA_RESERVE_GB", "12")
        monkeypatch.setenv("DOCLING_VRAM_COLD_CUDA_GB", "8")
        with patch(
            "vram_policy.get_gpu_vram_stats_gb",
            return_value={"total_gb": 20.0, "used_gb": 13.0, "free_gb": 7.0},
        ):
            opts = resolve_accelerator_device({"do_table_structure": True})
        assert opts["accelerator_device"] == "cpu"
        assert opts["device_reason"] == "ollama_reserve"
        assert opts["num_threads"] == 8

    def test_resolve_uses_cuda_when_headroom_ok(self, monkeypatch):
        monkeypatch.setenv("DOCLING_OLLAMA_RESERVE_GB", "12")
        monkeypatch.setenv("DOCLING_VRAM_COLD_CUDA_GB", "8")
        with patch(
            "vram_policy.get_gpu_vram_stats_gb",
            return_value={"total_gb": 20.0, "used_gb": 2.0, "free_gb": 18.0},
        ):
            opts = resolve_accelerator_device({"do_table_structure": True})
        assert opts["accelerator_device"] == "cuda"
        assert opts["device_reason"] == "vram_ok"

    def test_resolve_cuda_with_gemma_qat_and_measured_cold_spike(self, monkeypatch):
        """20GB card + ~5.8GB Ollama must not be blocked by an 8GB cold-spike budget."""
        monkeypatch.delenv("DOCLING_VRAM_COLD_CUDA_GB", raising=False)
        monkeypatch.setenv("DOCLING_OLLAMA_RESERVE_GB", "12")
        with patch(
            "vram_policy.get_gpu_vram_stats_gb",
            return_value={"total_gb": 19.58, "used_gb": 5.77, "free_gb": 13.81},
        ):
            opts = resolve_accelerator_device({"do_table_structure": True})
        assert opts["accelerator_device"] == "cuda"
        assert opts["device_reason"] == "vram_ok"

    def test_legacy_8gb_cold_budget_blocks_cuda_on_20gb_card(self, monkeypatch):
        monkeypatch.setenv("DOCLING_OLLAMA_RESERVE_GB", "12")
        monkeypatch.setenv("DOCLING_VRAM_COLD_CUDA_GB", "8")
        with patch(
            "vram_policy.get_gpu_vram_stats_gb",
            return_value={"total_gb": 19.58, "used_gb": 5.77, "free_gb": 13.81},
        ):
            opts = resolve_accelerator_device({})
        assert opts["accelerator_device"] == "cpu"
        assert opts["device_reason"] == "ollama_reserve"

    def test_forced_cpu_preference(self, monkeypatch):
        monkeypatch.setenv("DOCLING_ACCELERATOR_PREFERENCE", "cpu")
        opts = resolve_accelerator_device({})
        assert opts["accelerator_device"] == "cpu"
        assert opts["device_reason"] == "forced_cpu"

    def test_cpu_fallback_sets_oom_retry(self, monkeypatch):
        monkeypatch.setenv("DOCLING_CPU_NUM_THREADS", "8")
        opts = cpu_fallback_options({"accelerator_device": "cuda", "device_reason": "vram_ok"})
        assert opts["accelerator_device"] == "cpu"
        assert opts["oom_retried"] is True
        assert opts["device_reason"] == "oom_retry"
        assert opts["num_threads"] == 8

    def test_is_cuda_gpu_failure_detects_cudnn(self):
        assert is_cuda_gpu_failure(RuntimeError("cuDNN error: CUDNN_STATUS_NOT_INITIALIZED"))

    def test_is_cuda_gpu_failure_ignores_generic(self):
        assert not is_cuda_gpu_failure(ValueError("bad pdf"))
