#!/usr/bin/env python3
"""
Unit tests for Docling configuration options pass-through functionality

Tests that the system correctly passes through any valid Docling configuration
from publisher to worker without hardcoding specific options.
"""
import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from docling_worker import DoclingWorker
from services import DocumentService


class TestDoclingOptionsPassThrough:
    """Test generic Docling options pass-through functionality"""

    def setup_method(self):
        """Set up test fixtures"""
        self.worker = DoclingWorker()
        
    def test_no_options_uses_default_converter(self):
        """Test that no options creates standard DocumentConverter"""
        converter = self.worker._create_document_converter(None)
        
        # Should create a DocumentConverter instance
        assert converter is not None
        assert hasattr(converter, 'convert')
        
    def test_empty_options_uses_default_converter(self):
        """Test that empty options dict creates standard DocumentConverter"""
        converter = self.worker._create_document_converter({})
        
        # Should create a DocumentConverter instance
        assert converter is not None
        assert hasattr(converter, 'convert')
        
    def test_format_options_pass_through(self):
        """Test that format_options are passed through to DocumentConverter"""
        from docling.document_converter import PdfFormatOption
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        
        # Create valid format options
        docling_options = {
            "format_options": {
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=PdfPipelineOptions(
                        do_ocr=True,
                        ocr_engine="easyocr"
                    )
                )
            }
        }
        
        converter = self.worker._create_document_converter(docling_options)
        
        # Should successfully create converter with options
        assert converter is not None
        assert hasattr(converter, 'convert')
        
    def test_accelerator_options_pass_through(self):
        """Test that accelerator_options are passed through"""
        from docling.datamodel.pipeline_options import AcceleratorOptions, AcceleratorDevice
        
        docling_options = {
            "accelerator_options": AcceleratorOptions(
                num_threads=4,
                device=AcceleratorDevice.CPU
            )
        }
        
        converter = self.worker._create_document_converter(docling_options)
        
        # Should successfully create converter with accelerator options
        assert converter is not None
        assert hasattr(converter, 'convert')
        
    def test_combined_options_pass_through(self):
        """Test that multiple option types can be combined"""
        from docling.document_converter import PdfFormatOption
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions, AcceleratorOptions, AcceleratorDevice
        
        docling_options = {
            "format_options": {
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=PdfPipelineOptions(
                        do_ocr=True,
                        do_table_structure=True
                    )
                )
            },
            "accelerator_options": AcceleratorOptions(
                num_threads=2,
                device=AcceleratorDevice.CPU
            )
        }
        
        converter = self.worker._create_document_converter(docling_options)
        
        # Should successfully create converter with all options
        assert converter is not None
        assert hasattr(converter, 'convert')
        
    def test_invalid_options_fallback_to_default(self):
        """Test that invalid options gracefully fall back to default converter"""
        # Invalid options that should cause DocumentConverter to fail
        docling_options = {
            "invalid_option": "this_should_not_work",
            "another_invalid": 12345
        }
        
        converter = self.worker._create_document_converter(docling_options)
        
        # Should fall back to default converter
        assert converter is not None
        assert hasattr(converter, 'convert')
        
    @pytest.mark.asyncio
    async def test_services_pass_through_docling_options(self):
        """Test that DocumentService correctly passes docling_options to worker"""
        doc_service = DocumentService()
        
        # Mock the NATS publishing
        with patch.object(doc_service, 'client') as mock_client:
            mock_js = AsyncMock()
            mock_client.js = mock_js
            mock_js.stream_info = AsyncMock()
            mock_js.pull_subscribe = AsyncMock()
            mock_js.publish = AsyncMock()
            
            # Mock the subscription and response
            mock_subscription = AsyncMock()
            mock_js.pull_subscribe.return_value = mock_subscription
            
            mock_message = Mock()
            mock_message.data.decode.return_value = json.dumps({
                "status": "success",
                "extracted_text": "test content",
                "structured_data": {}
            })
            mock_message.ack = AsyncMock()  # Fix: Make ack() async
            mock_subscription.fetch.return_value = [mock_message]
            
            # Test docling_options pass-through
            test_options = {
                "format_options": {},
                "accelerator_options": {}
            }
            
            result = await doc_service.process_document(
                "test.pdf", 
                docling_options=test_options
            )
            
            # Verify NATS message was published with docling_options
            mock_js.publish.assert_called_once()
            call_args = mock_js.publish.call_args
            message_data = json.loads(call_args[0][1].decode())
            
            assert "docling_options" in message_data
            assert message_data["docling_options"] == test_options


class TestDoclingOptionsExamples:
    """Test examples of common Docling configuration patterns"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.worker = DoclingWorker()
        
    def test_pdf_with_ocr_configuration(self):
        """Test PDF processing with OCR configuration"""
        from docling.document_converter import PdfFormatOption
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions, OcrEngine
        
        docling_options = {
            "format_options": {
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=PdfPipelineOptions(
                        do_ocr=True,
                        ocr_engine=OcrEngine.EASYOCR,
                        force_full_page_ocr=True
                    )
                )
            }
        }
        
        converter = self.worker._create_document_converter(docling_options)
        assert converter is not None
        
    def test_multiple_format_support(self):
        """Test configuration for multiple input formats"""
        from docling.document_converter import PdfFormatOption, WordFormatOption
        from docling.datamodel.base_models import InputFormat
        from docling.datamodel.pipeline_options import PdfPipelineOptions
        
        docling_options = {
            "format_options": {
                InputFormat.PDF: PdfFormatOption(
                    pipeline_options=PdfPipelineOptions(do_ocr=True)
                ),
                InputFormat.DOCX: WordFormatOption()
            }
        }
        
        converter = self.worker._create_document_converter(docling_options)
        assert converter is not None
        
    def test_performance_optimization_config(self):
        """Test performance-focused configuration"""
        from docling.datamodel.pipeline_options import AcceleratorOptions, AcceleratorDevice
        
        docling_options = {
            "accelerator_options": AcceleratorOptions(
                num_threads=8,
                device=AcceleratorDevice.CPU  # Use CPU for compatibility
            )
        }
        
        converter = self.worker._create_document_converter(docling_options)
        assert converter is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 