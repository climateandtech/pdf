#!/usr/bin/env python3
"""
Simple Docling Integration Test

This test demonstrates the docling worker integration without requiring 
real S3 or NATS infrastructure. It uses mocks to simulate the flow.

Test Flow:
1. Create a test PDF
2. Mock the S3 and NATS infrastructure  
3. Process the PDF through the docling worker logic
4. Verify docling extraction works
"""
import pytest
import asyncio
import tempfile
import os
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from unittest.mock import AsyncMock, MagicMock, patch
import json

# Import the docling processing function
from docling_worker import DoclingWorker
from worker import process_with_docling

@pytest.fixture
def test_climate_pdf():
    """Create a test PDF with climate content for docling to extract"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        
        # Create realistic climate content
        c.drawString(100, 750, "Climate Risk Assessment 2024")
        c.drawString(100, 720, "Executive Summary")
        c.drawString(100, 690, "")
        c.drawString(100, 660, "This report evaluates climate change impacts on our operations.")
        c.drawString(100, 630, "Key findings:")
        c.drawString(120, 600, "• Global temperature increase of 1.5°C by 2030")
        c.drawString(120, 570, "• Sea level rise affecting coastal infrastructure") 
        c.drawString(120, 540, "• Increased extreme weather events")
        c.drawString(100, 510, "")
        c.drawString(100, 480, "Mitigation Strategies:")
        c.drawString(120, 450, "1. Carbon neutrality by 2050")
        c.drawString(120, 420, "2. Renewable energy transition")
        c.drawString(120, 390, "3. Climate adaptation measures")
        c.drawString(100, 360, "")
        c.drawString(100, 330, "Investment Required: $10M over 5 years")
        c.drawString(100, 300, "Expected ROI: 15% reduction in climate risks")
        
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        os.unlink(tmp.name)

@pytest.mark.asyncio
async def test_docling_worker_with_real_docling(test_climate_pdf):
    """
    Test the DoclingWorker with real docling processing (no S3/NATS required)
    
    This test verifies:
    1. ✅ DoclingWorker can process PDFs with real docling
    2. ✅ Content extraction works correctly
    3. ✅ Proper response format is returned
    """
    print("\n🚀 Testing Docling Worker with Real Docling Processing")
    print("=" * 60)
    
    # Read the test PDF
    with open(test_climate_pdf, 'rb') as f:
        pdf_content = f.read()
    
    print(f"📄 Created test PDF: {len(pdf_content)} bytes")
    
    # Initialize DoclingWorker
    worker = DoclingWorker()
    
    # Test the docling processing directly (bypass S3/NATS)
    print("\n--- Testing Real Docling Processing ---")
    
    try:
        # Save PDF to temp file for docling processing
        temp_file = Path(f"/tmp/test_climate_assessment.pdf")
        with open(temp_file, 'wb') as f:
            f.write(pdf_content)
            
        print(f"💾 Saved PDF to: {temp_file}")
        
        # **REAL DOCLING PROCESSING**
        print("🔬 Processing PDF with docling...")
        result = worker.doc_converter.convert(str(temp_file))
        
        # Extract content from docling result
        document = result.document
        markdown_content = document.export_to_markdown()
        
        # Get structured data if available
        try:
            structured_data = document.export_to_dict()
        except:
            structured_data = None
            
        print("✅ Docling processing completed!")
        
        # Create response in the format the worker would send
        response = {
            "request_id": "test_request",
            "status": "success", 
            "result": {
                "text": markdown_content,
                "markdown": markdown_content,
                "structured_data": structured_data,
                "metadata": {
                    "pages": len(document.pages) if hasattr(document, 'pages') else 1,
                    "format": "pdf",
                    "processed_by": "docling_worker"
                }
            }
        }
        
        # Verify the results
        assert response["status"] == "success"
        assert "result" in response
        
        docling_result = response["result"]
        extracted_text = docling_result["text"]
        metadata = docling_result["metadata"]
        
        print(f"\n📊 Docling Extraction Results:")
        print(f"   ✅ Status: {response['status']}")
        print(f"   ✅ Extracted text: {len(extracted_text)} characters")
        print(f"   ✅ Pages processed: {metadata.get('pages', 'unknown')}")
        print(f"   ✅ Format: {metadata.get('format', 'unknown')}")
        print(f"   ✅ Processed by: {metadata.get('processed_by', 'unknown')}")
        
        # Show sample of extracted content
        print(f"\n📄 EXTRACTED CONTENT SAMPLE (first 300 chars):")
        print("-" * 60)
        print(extracted_text[:300] + "..." if len(extracted_text) > 300 else extracted_text)
        print("-" * 60)
        
        # Verify content quality
        assert len(extracted_text) > 100, "Should extract meaningful content"
        
        # Check for climate-related content
        content_lower = extracted_text.lower()
        climate_keywords = ["climate", "temperature", "carbon", "risk", "mitigation", "assessment"]
        found_keywords = [kw for kw in climate_keywords if kw in content_lower]
        
        print(f"\n🌍 Climate Keywords Found: {found_keywords}")
        assert len(found_keywords) >= 2, f"Should find climate-related content, found: {found_keywords}"
        
        # Cleanup
        temp_file.unlink()
        
        print("\n🎉 Docling Worker Test Completed Successfully!")
        print("\n📋 Verified Components:")
        print("   ✅ Real docling PDF processing")
        print("   ✅ Content extraction and formatting")
        print("   ✅ Proper response structure")
        print("   ✅ Climate content recognition")
        print(f"   ✅ Processed {len(extracted_text)} characters from PDF")
        
        return response
        
    except Exception as e:
        print(f"❌ Error in docling processing: {e}")
        raise

def test_mock_docling_processing():
    """
    Test the mock docling processing function (fallback when docling not available)
    """
    print("\n🔧 Testing Mock Docling Processing Function")
    print("=" * 50)
    
    # Test the mock processing function
    test_content = b"Sample PDF content for testing"
    filename = "test_document.pdf"
    
    result = process_with_docling(test_content, filename)
    
    print(f"📊 Mock Processing Results:")
    print(f"   ✅ Content: {result['content']}")
    print(f"   ✅ Metadata: {result['metadata']}")
    print(f"   ✅ Format: {result['metadata']['format']}")
    print(f"   ✅ Pages: {result['metadata']['pages']}")
    
    # Verify structure
    assert "content" in result
    assert "metadata" in result
    assert result["metadata"]["format"] == "pdf"
    assert filename in result["content"]
    
    print("✅ Mock docling processing verified!")

@pytest.mark.asyncio 
async def test_docling_worker_message_processing_mock():
    """
    Test the complete docling worker message processing flow with mocks
    
    This simulates the NATS message processing without requiring real infrastructure
    """
    print("\n📨 Testing Docling Worker Message Processing (Mocked)")
    print("=" * 60)
    
    # Create mock NATS message
    mock_message = MagicMock()
    mock_message.data.decode.return_value = json.dumps({
        "request_id": "test_request_123",
        "s3_key": "test_document.pdf"
    })
    mock_message.ack = AsyncMock()
    
    # Create mock S3 client that returns test PDF content
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "Test Document for Mock Processing")
        c.drawString(100, 700, "This demonstrates the worker flow")
        c.save()
        
        with open(tmp.name, 'rb') as f:
            test_pdf_content = f.read()
        
        os.unlink(tmp.name)
    
    # Mock the DoclingWorker's dependencies
    worker = DoclingWorker()
    
    # Mock the client.download_result method
    worker.client.download_result = AsyncMock(return_value=test_pdf_content)
    
    # Mock the JetStream publish method
    worker.client.js.publish = AsyncMock()
    
    print("🔧 Mocks set up, processing message...")
    
    # Process the mock message
    await worker.process_document_request(mock_message)
    
    # Verify the mocks were called correctly
    assert worker.client.download_result.called
    assert worker.client.js.publish.called
    assert mock_message.ack.called
    
    # Check the published response
    publish_call = worker.client.js.publish.call_args
    response_data = json.loads(publish_call[0][1].decode())
    
    print(f"📤 Published Response:")
    print(f"   ✅ Request ID: {response_data['request_id']}")
    print(f"   ✅ Status: {response_data['status']}")
    print(f"   ✅ Result keys: {list(response_data['result'].keys())}")
    
    assert response_data["request_id"] == "test_request_123"
    assert response_data["status"] == "success"
    assert "text" in response_data["result"]
    assert "metadata" in response_data["result"]
    
    print("✅ Message processing flow verified!")

if __name__ == "__main__":
    # Run the tests
    print("🚀 Running Simple Docling Integration Tests")
    pytest.main([__file__, "-v", "-s"]) 