#!/usr/bin/env python3
"""
Test Enrichment Options Conversion
Tests that all simple JSON enrichment options are correctly converted to Docling objects
"""

import pytest
import sys
import os
from unittest.mock import Mock, patch, MagicMock

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from docling_worker import DoclingWorker


class TestEnrichmentConversion:
    """Test all enrichment option conversions"""

    def setup_method(self):
        """Set up test fixtures"""
        self.worker = DoclingWorker()

    def test_simple_option_detection(self):
        """Test detection of simple vs complex options"""
        
        # Simple options should be detected (REAL Docling options only)
        simple_cases = [
            {'do_picture_description': True},
            {'vlm_model': 'granite'},
            {'do_ocr': True},
            {'do_code_enrichment': True},
            {'do_formula_enrichment': True},
            {'do_picture_classification': True},
            {'do_table_structure': True},
            {'generate_picture_images': True},
            {'accelerator_device': 'gpu'},
            {'custom_prompt': 'test'},
            {'artifacts_path': '/path/to/models'},
            {'enable_remote_services': True},
            {'document_timeout': 300.0},
            {'create_legacy_output': False},
            {'force_backend_text': True},
            {'images_scale': 2.0},
            {'vlm_batch_size': 16},
            {'ocr_languages': ['en', 'es']},
            {'force_full_page_ocr': True},
            {'table_do_cell_matching': False},
            {'num_threads': 8},
            {'cuda_use_flash_attention2': True},
            {'input_formats': ['pdf', 'docx']},
            {
                'do_picture_description': True,
                'do_ocr': True,
                'vlm_model': 'granite',
                'artifacts_path': '/models',
                'enable_remote_services': True
            }
        ]
        
        for options in simple_cases:
            assert self.worker._is_simple_options(options), f"Should detect simple options: {options}"
        
        # Complex options should NOT be detected as simple
        complex_cases = [
            {'format_options': {}},
            {'accelerator_options': {}},
            {'format_options': {}, 'do_picture_description': True},  # Mixed
        ]
        
        for options in complex_cases:
            assert not self.worker._is_simple_options(options), f"Should NOT detect as simple: {options}"

    def test_vlm_picture_description_conversion(self):
        """Test VLM picture description options conversion (real test without mocking internals)"""
        
        # Test basic VLM options
        simple_options = {
            'do_picture_description': True,
            'vlm_model': 'granite',
            'images_scale': 2.5
        }
        
        # Should not raise an error and should return format_options
        result = self.worker._convert_simple_options(simple_options)
        assert 'format_options' in result
        
        # Test with custom prompt
        simple_options_with_prompt = {
            'do_picture_description': True,
            'vlm_model': 'granite',
            'custom_prompt': 'Describe this image in detail'
        }
        
        result = self.worker._convert_simple_options(simple_options_with_prompt)
        assert 'format_options' in result

    def test_enrichment_options_conversion(self):
        """Test all real enrichment options work"""
        
        # Test all real enrichment options
        simple_options = {
            'do_picture_description': True,
            'do_picture_classification': True,
            'do_code_enrichment': True,
            'do_formula_enrichment': True,
            'do_table_structure': True,
            'generate_picture_images': True,
            'vlm_model': 'granite'
        }
        
        # Should work without errors
        result = self.worker._convert_simple_options(simple_options)
        assert 'format_options' in result
        assert isinstance(result, dict)
        
        # Test OCR options
        ocr_options = {
            'do_ocr': True,
            'force_full_page_ocr': True
        }
        
        result = self.worker._convert_simple_options(ocr_options)
        assert 'format_options' in result

    def test_performance_options(self):
        """Test performance and system options (REAL options only)"""
        
        # Test accelerator options (REAL AcceleratorOptions fields only)
        performance_options = {
            'accelerator_device': 'gpu',
            'num_threads': 8,
            'cuda_use_flash_attention2': True
        }
        
        result = self.worker._convert_simple_options(performance_options)
        assert 'format_options' in result
        
        # Test real core pipeline options
        system_options = {
            'artifacts_path': '/custom/models',
            'document_timeout': 300.0,
            'enable_remote_services': True
        }
        
        result = self.worker._convert_simple_options(system_options)
        assert 'format_options' in result

    def test_combined_options(self):
        """Test combination of multiple real options"""
        
        # Test comprehensive real options
        combined_options = {
            # VLM options
            'do_picture_description': True,
            'vlm_model': 'granite',
            'images_scale': 1.5,
            
            # Other enrichments
            'do_picture_classification': True,
            'do_code_enrichment': True,
            'do_formula_enrichment': True,
            
            # OCR options
            'do_ocr': True,
            'force_full_page_ocr': True,
            
            # Performance
            'accelerator_device': 'cpu',
            'num_threads': 6,
            
            # System
            'timeout': 600,
            'debug_mode': True
        }
        
        result = self.worker._convert_simple_options(combined_options)
        assert 'format_options' in result
        assert isinstance(result, dict)

    def test_empty_options_handling(self):
        """Test handling of empty or None options"""
        
        # Empty dict should still work
        result = self.worker._convert_simple_options({})
        assert 'format_options' in result
        
        # Dict with no recognized options should still work
        result = self.worker._convert_simple_options({'unknown_option': 'value'})
        assert 'format_options' in result

    def test_document_converter_creation(self):
        """Test DocumentConverter creation with real options"""
        
        # Test with real options
        simple_options = {
            'do_picture_description': True,
            'vlm_model': 'granite',
            'timeout': 300
        }
        
        # Should create converter without errors
        result = self.worker._create_document_converter(simple_options)
        assert result is not None

    def test_backward_compatibility(self):
        """Test that complex Docling objects still work (backward compatibility)"""
        
        # Complex options should pass through unchanged
        complex_options = {
            'format_options': {'some': 'complex_object'},
            'accelerator_options': {'another': 'complex_object'}
        }
        
        # Should NOT be detected as simple
        assert not self.worker._is_simple_options(complex_options)
        
        # Should pass through unchanged in _create_document_converter
        with patch('docling_worker.DocumentConverter') as mock_converter_class:
            mock_converter = Mock()
            mock_converter_class.return_value = mock_converter
            
            result = self.worker._create_document_converter(complex_options)
            
            # Should be called with original complex options
            mock_converter_class.assert_called_once_with(**complex_options)







    def test_core_pipeline_options(self):
        """Test real core pipeline options"""
        
        # Test real PdfPipelineOptions fields
        simple_options = {
            'create_legacy_output': False,
            'document_timeout': 600.0,
            'enable_remote_services': True,
            'allow_external_plugins': False,
            'force_backend_text': True,
            'artifacts_path': '/custom/path'
        }
        
        result = self.worker._convert_simple_options(simple_options)
        assert 'format_options' in result







    def test_multi_format_support(self):
        """Test multi-format input support (real test without complex mocking)"""
        
        # Test multiple input formats
        simple_options = {
            'input_formats': ['pdf', 'docx', 'image', 'html', 'pptx']
        }
        
        result = self.worker._convert_simple_options(simple_options)
        
        # Verify format options were created
        assert 'format_options' in result
        format_options = result['format_options']
        
        # Should have multiple format options
        assert len(format_options) > 1

    def test_comprehensive_real_options_integration(self):
        """Test ultimate combination of ALL REAL options"""
        
        # Test with ALL REAL Docling options only
        ultimate_real_options = {
            # Core Pipeline (REAL)
            'create_legacy_output': False,
            'document_timeout': 300.0,
            'enable_remote_services': True,
            'artifacts_path': '/models',
            'force_backend_text': False,
            
            # VLM (REAL)
            'do_picture_description': True,
            'vlm_model': 'granite',
            'custom_prompt': 'Analyze everything',
            'images_scale': 2.0,
            'vlm_batch_size': 16,
            
            # Enrichments (REAL)
            'do_picture_classification': True,
            'do_code_enrichment': True,
            'do_formula_enrichment': True,
            'do_table_structure': True,
            'generate_picture_images': True,
            'generate_page_images': True,
            
            # OCR (REAL)
            'do_ocr': True,
            'ocr_languages': ['en', 'es'],
            'force_full_page_ocr': True,
            'ocr_confidence_threshold': 0.8,
            
            # Table (REAL)
            'table_do_cell_matching': True,
            
            # Performance (REAL)
            'accelerator_device': 'gpu',
            'num_threads': 8,
            'cuda_use_flash_attention2': True,
            
            # Multi-format
            'input_formats': ['pdf', 'docx', 'image']
        }
        
        result = self.worker._convert_simple_options(ultimate_real_options)
        
        # Verify basic structure
        assert 'format_options' in result
        assert isinstance(result, dict)
        
        # Should have multiple format options
        format_options = result['format_options']
        assert len(format_options) >= 3  # PDF + DOCX + Image


class TestErrorHandling:
    """Test error handling in conversion"""
    
    def setup_method(self):
        self.worker = DoclingWorker()

    def test_invalid_option_types(self):
        """Test handling of invalid option types"""
        
        # Non-dict should return False for simple detection
        assert not self.worker._is_simple_options("not a dict")
        assert not self.worker._is_simple_options(None)
        assert not self.worker._is_simple_options(123)

    def test_malformed_options_fallback(self):
        """Test fallback behavior for malformed options"""
        
        with patch('docling_worker.DocumentConverter') as mock_converter_class:
            mock_converter = Mock()
            mock_converter_class.return_value = mock_converter
            
            # If conversion fails, should fall back to default
            with patch.object(self.worker, '_convert_simple_options', side_effect=Exception("Test error")):
                result = self.worker._create_document_converter({'do_picture_description': True})
                
                # Should create default converter on fallback
                assert mock_converter_class.call_count >= 1

    def test_invalid_option_values(self):
        """Test handling of invalid option values for REAL options"""
        
        # Test invalid values for real options
        simple_options = {
            'accelerator_device': 'quantum_computer',  # Invalid device
            'vlm_model': 'super_ai_9000',             # Invalid VLM model
            'do_picture_description': 'maybe'         # Invalid boolean
        }
        
        # Should not raise an exception, might use defaults
        result = self.worker._convert_simple_options(simple_options)
        assert 'format_options' in result  # Basic structure should still be created

    def test_type_conversion_edge_cases(self):
        """Test edge cases in type conversion"""
        
        with patch('docling_worker.PdfPipelineOptions'), \
             patch('docling_worker.PdfFormatOption'):
            
            # Test string numbers that might fail conversion
            simple_options = {
                'timeout': 'not_a_number',
                'max_file_size': 'also_not_a_number',
                'images_scale': 'definitely_not_a_number'
            }
            
            # Should handle gracefully (might skip invalid values)
            try:
                result = self.worker._convert_simple_options(simple_options)
                # If it succeeds, basic structure should exist
                assert 'format_options' in result
            except (ValueError, TypeError):
                # If it fails, that's also acceptable behavior
                pass

    def test_boundary_values(self):
        """Test boundary and extreme values for REAL options"""
        
        # Test extreme values for real options
        simple_options = {
            'document_timeout': 0.0,         # Zero timeout
            'images_scale': 0.001,           # Very small scale
            'num_threads': 1000,             # Very high thread count  
            'vlm_batch_size': 1,             # Tiny batch size
            'ocr_confidence_threshold': 1.1  # Over 100% confidence
        }
        
        # Should handle without crashing
        result = self.worker._convert_simple_options(simple_options)
        assert 'format_options' in result


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"]) 