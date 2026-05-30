"""Smoke-test script for post-install verification (mocked on CI)."""

import sys
from unittest.mock import MagicMock, patch

import pytest


@pytest.mark.unit
def test_verify_torch_import_main_success():
    mock_conv_mod = MagicMock()
    mock_dc = MagicMock()
    mock_conv_mod.DocumentConverter = mock_dc

    with patch("worker_runtime.bootstrap_gpu", return_value="20gb_nats") as boot:
        with patch.dict(
            sys.modules,
            {
                "docling": MagicMock(),
                "docling.document_converter": mock_conv_mod,
            },
        ):
            from scripts.verify_torch_import import main

            assert main() == 0
            boot.assert_called_once()
            mock_dc.assert_called_once()
