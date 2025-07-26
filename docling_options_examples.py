#!/usr/bin/env python3
"""
Examples of how to use the generic Docling options system

The system now accepts any valid Docling configuration and passes it through
to the DocumentConverter, making it completely flexible and future-proof.
"""
from docling.document_converter import PdfFormatOption, WordFormatOption, ImageFormatOption
from docling.datamodel.base_models import InputFormat
from docling.datamodel.pipeline_options import (
    PdfPipelineOptions, 
    AcceleratorOptions, 
    AcceleratorDevice,
    OcrEngine,
    granite_picture_description  # VLM still supported
)


# Example 1: Basic PDF processing with OCR
def get_pdf_with_ocr_options():
    """Configure PDF processing with OCR enabled"""
    return {
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


# Example 2: High-performance configuration
def get_performance_optimized_options():
    """Configure for optimal performance"""
    return {
        "format_options": {
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=PdfPipelineOptions(
                    do_table_structure=True,
                    do_ocr=False  # Skip OCR for speed
                )
            )
        },
        "accelerator_options": AcceleratorOptions(
            num_threads=8,
            device=AcceleratorDevice.CPU
        )
    }


# Example 3: Multi-format support
def get_multi_format_options():
    """Configure support for multiple document formats"""
    return {
        "format_options": {
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=PdfPipelineOptions(
                    do_ocr=True,
                    do_table_structure=True
                )
            ),
            InputFormat.DOCX: WordFormatOption(),
            InputFormat.IMAGE: ImageFormatOption()
        }
    }


# Example 4: VLM (Visual Language Model) configuration
def get_vlm_options():
    """Configure VLM for picture descriptions (requires macOS 13.5+)"""
    return {
        "format_options": {
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=PdfPipelineOptions(
                    do_picture_description=True,
                    picture_description_options=granite_picture_description,
                    images_scale=2.0,
                    generate_picture_images=True
                )
            )
        }
    }


# Example 5: Custom OCR configuration
def get_custom_ocr_options():
    """Configure with specific OCR settings"""
    return {
        "format_options": {
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=PdfPipelineOptions(
                    do_ocr=True,
                    ocr_engine=OcrEngine.TESSERACT_CLI,
                    force_full_page_ocr=False
                )
            )
        }
    }


# Example usage in your application:
async def example_usage():
    """Example of how to use these configurations"""
    from services import DocumentService
    
    doc_service = DocumentService()
    await doc_service.setup()
    
    # Use any of the configurations above
    docling_options = get_pdf_with_ocr_options()
    
    # Process document with custom options
    result = await doc_service.process_document(
        s3_key="my-document.pdf",
        docling_options=docling_options
    )
    
    print(f"Processed document: {result['status']}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(example_usage()) 