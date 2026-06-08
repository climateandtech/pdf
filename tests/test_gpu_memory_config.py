"""Unit tests for GPU memory presets (no CUDA required)."""

import pytest

from gpu_memory_config import GPUMemoryOptimizer, MemoryConfig, setup_gpu_optimization


@pytest.mark.unit
class TestGPUMemoryConfigs:
    def test_production_presets_exist(self):
        assert "20gb_nats" in GPUMemoryOptimizer.CONFIGS
        assert "20gb_capped" in GPUMemoryOptimizer.CONFIGS
        assert "capped_5gb" in GPUMemoryOptimizer.CONFIGS

    def test_20gb_nats_leaves_headroom_for_ollama(self):
        cfg = GPUMemoryOptimizer.CONFIGS["20gb_nats"]
        assert cfg.memory_fraction <= 0.5
        assert cfg.max_batch_size >= 1

    def test_capped_5gb_uses_minimal_batches(self):
        cfg = GPUMemoryOptimizer.CONFIGS["capped_5gb"]
        assert cfg.max_batch_size == 1
        assert cfg.vlm_batch_size == 1
        assert cfg.gradient_checkpointing is True

    def test_apply_config_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown config"):
            GPUMemoryOptimizer.apply_config("not_a_real_preset")

    def test_get_optimal_docling_options_caps_vlm_batch(self):
        cfg = GPUMemoryOptimizer.CONFIGS["capped_5gb"]
        opts = GPUMemoryOptimizer.get_optimal_docling_options(
            cfg, user_options={"vlm_batch_size": 99}
        )
        assert opts["vlm_batch_size"] == 1
        assert opts["layout_batch_size"] == 1
        assert opts["generate_picture_images"] is False

    def test_get_optimal_docling_options_preserves_cpu_device(self):
        cfg = GPUMemoryOptimizer.CONFIGS["20gb_capped"]
        opts = GPUMemoryOptimizer.get_optimal_docling_options(
            cfg, user_options={"accelerator_device": "cpu", "num_threads": 8}
        )
        assert opts["accelerator_device"] == "cpu"
        assert opts["num_threads"] == 8

    def test_detect_optimal_config_without_cuda(self, monkeypatch):
        monkeypatch.setattr(
            "gpu_memory_config.torch.cuda.is_available", lambda: False
        )
        assert GPUMemoryOptimizer.detect_optimal_config() == "12gb_minimal"

    def test_detect_optimal_config_20gb_card(self, monkeypatch):
        class FakeProps:
            total_memory = 20 * 1024**3

        monkeypatch.setattr(
            "gpu_memory_config.torch.cuda.is_available", lambda: True
        )
        monkeypatch.setattr(
            "gpu_memory_config.torch.cuda.current_device", lambda: 0
        )
        monkeypatch.setattr(
            "gpu_memory_config.torch.cuda.get_device_properties",
            lambda _device: FakeProps(),
        )
        assert GPUMemoryOptimizer.detect_optimal_config() == "16gb_optimized"

    def test_setup_gpu_optimization_returns_docling_options(self, monkeypatch):
        cfg = GPUMemoryOptimizer.CONFIGS["20gb_nats"]

        monkeypatch.setattr(
            GPUMemoryOptimizer,
            "detect_optimal_config",
            classmethod(lambda cls: "20gb_nats"),
        )
        monkeypatch.setattr(
            GPUMemoryOptimizer, "apply_config", classmethod(lambda cls, name: cfg)
        )
        monkeypatch.setattr(
            GPUMemoryOptimizer, "print_memory_status", classmethod(lambda cls: None)
        )

        opts = setup_gpu_optimization(config_name="20gb_nats")
        assert opts["vlm_batch_size"] == cfg.vlm_batch_size
        assert isinstance(opts, dict)
