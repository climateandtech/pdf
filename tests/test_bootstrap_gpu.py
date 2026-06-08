"""Tests for worker_runtime bootstrap (mocked torch/CUDA)."""

from unittest.mock import MagicMock, patch

import pytest

from worker_runtime import bootstrap_gpu, cleanup_gpu_memory, resolve_profile_name


@pytest.mark.unit
class TestBootstrapGpu:
    def test_bootstrap_applies_20gb_capped_profile(self, monkeypatch):
        monkeypatch.delenv("DOCLING_GPU_PROFILE", raising=False)

        fake_torch = MagicMock()
        fake_torch.cuda.is_available.return_value = True
        fake_props = MagicMock()
        fake_props.total_memory = 20 * 1024**3
        fake_torch.cuda.get_device_properties.return_value = fake_props

        with patch.dict("sys.modules", {"torch": fake_torch, "torchvision": MagicMock()}):
            with patch("gpu_memory_config.GPUMemoryOptimizer.apply_config") as apply:
                with patch(
                    "gpu_memory_config.GPUMemoryOptimizer.print_memory_status"
                ):
                    name = bootstrap_gpu("full")

        assert name == "20gb_capped"
        apply.assert_called_once_with("20gb_capped")

    def test_bootstrap_capped_5gb_sets_memory_fraction(self, monkeypatch):
        monkeypatch.setenv("DOCLING_GPU_CAP_GB", "5")

        fake_torch = MagicMock()
        fake_torch.cuda.is_available.return_value = True
        fake_props = MagicMock()
        fake_props.total_memory = 20 * 1024**3
        fake_torch.cuda.get_device_properties.return_value = fake_props

        with patch.dict("sys.modules", {"torch": fake_torch, "torchvision": MagicMock()}):
            with patch("gpu_memory_config.GPUMemoryOptimizer.apply_config") as apply:
                with patch(
                    "gpu_memory_config.GPUMemoryOptimizer.print_memory_status"
                ):
                    name = bootstrap_gpu("capped_5gb")

        fake_torch.cuda.set_per_process_memory_fraction.assert_called_once()
        fraction = fake_torch.cuda.set_per_process_memory_fraction.call_args[0][0]
        assert abs(fraction - 0.25) < 0.01
        assert name == "capped_5gb"
        apply.assert_called_once_with("capped_5gb")

    def test_bootstrap_20gb_capped_sets_memory_fraction(self, monkeypatch):
        monkeypatch.setenv("DOCLING_GPU_CAP_GB", "8")

        fake_torch = MagicMock()
        fake_torch.cuda.is_available.return_value = True
        fake_props = MagicMock()
        fake_props.total_memory = 20 * 1024**3
        fake_torch.cuda.get_device_properties.return_value = fake_props

        with patch.dict("sys.modules", {"torch": fake_torch, "torchvision": MagicMock()}):
            with patch("gpu_memory_config.GPUMemoryOptimizer.apply_config") as apply:
                with patch(
                    "gpu_memory_config.GPUMemoryOptimizer.print_memory_status"
                ):
                    name = bootstrap_gpu("20gb_capped")

        fake_torch.cuda.set_per_process_memory_fraction.assert_called_once()
        fraction = fake_torch.cuda.set_per_process_memory_fraction.call_args[0][0]
        assert abs(fraction - 0.4) < 0.01
        assert name == "20gb_capped"
        apply.assert_called_once_with("20gb_capped")

    def test_bootstrap_unknown_profile_falls_back_to_detect(self, monkeypatch):
        fake_torch = MagicMock()
        fake_torch.cuda.is_available.return_value = False

        with patch.dict("sys.modules", {"torch": fake_torch, "torchvision": MagicMock()}):
            with patch(
                "gpu_memory_config.GPUMemoryOptimizer.detect_optimal_config",
                return_value="12gb_minimal",
            ):
                with patch("gpu_memory_config.GPUMemoryOptimizer.apply_config") as apply:
                    with patch(
                        "gpu_memory_config.GPUMemoryOptimizer.print_memory_status"
                    ):
                        name = bootstrap_gpu("custom_unknown")

        assert name == "12gb_minimal"
        apply.assert_called_once_with("12gb_minimal")

    def test_cleanup_gpu_memory_no_cuda(self, monkeypatch):
        fake_torch = MagicMock()
        fake_torch.cuda.is_available.return_value = False

        with patch.dict("sys.modules", {"torch": fake_torch}):
            cleanup_gpu_memory(force=True)

        fake_torch.cuda.empty_cache.assert_not_called()

    def test_cleanup_gpu_memory_with_cuda_force(self):
        fake_torch = MagicMock()
        fake_torch.cuda.is_available.return_value = True

        with patch.dict("sys.modules", {"torch": fake_torch}):
            cleanup_gpu_memory(force=True)

        fake_torch.cuda.empty_cache.assert_called_once()
        fake_torch.cuda.synchronize.assert_called_once()


@pytest.mark.unit
def test_resolve_profile_from_env(monkeypatch):
    monkeypatch.setenv("DOCLING_GPU_PROFILE", "b")
    assert resolve_profile_name() == "capped_5gb"


@pytest.mark.unit
def test_resolve_profile_full_maps_to_capped():
    assert resolve_profile_name("full") == "20gb_capped"
