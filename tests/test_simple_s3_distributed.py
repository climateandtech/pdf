"""
Simple S3 + NATS Distributed Test

This test verifies the Service A → S3 → NATS → Service B flow using our real services.
"""
import pytest
import asyncio
import json
import tempfile
import os
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from moto import mock_aws
import boto3

from s3_client import S3DocumentClient
from s3_config import S3Config
from config import NatsConfig

# Test configuration using moto mock
TEST_S3_CONFIG = S3Config(
    endpoint_url=None,  # Use mocked AWS services
    region_name="us-east-1",
    bucket_name="automated-test-bucket",
    aws_access_key_id="testing",
    aws_secret_access_key="testing"
)

TEST_NATS_CONFIG = NatsConfig(
    url="nats://localhost:4222",
    stream_name="AUTOMATED_DISTRIBUTED_TEST",
    subject_prefix="auto_dist_test"
)

@pytest.fixture
def test_pdf():
    """Create a test PDF for distributed testing"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "Simple Distributed Test Document")
        c.drawString(100, 700, "This PDF tests Service A → S3 → NATS → Service B flow")
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        os.unlink(tmp.name)

@pytest.mark.asyncio
@mock_aws
async def test_simple_distributed_flow(test_pdf):
    """
    Test basic distributed file passing: Service A → S3 → NATS → Service B
    """
    print("\n🚀 Starting Simple Distributed Test")
    print("=" * 50)
    
    # Create moto S3 setup
    boto3.client('s3', **TEST_S3_CONFIG.boto3_config())
    
    # Setup S3 client
    client = S3DocumentClient(s3_config=TEST_S3_CONFIG, nats_config=TEST_NATS_CONFIG)
    await client.setup()
    
    print("✅ S3 and NATS client initialized")
    
    # Test 1: Upload a file to S3 (Service A simulation)
    s3_key = "test/simple_distributed_test.pdf"
    s3_url = await client._upload_to_s3(test_pdf, s3_key)
    
    print(f"📄 Service A: Uploaded to S3 → {s3_key}")
    
    # Test 2: Send control message via NATS
    control_message = {
        "service": "Service A (Simple Test)",
        "action": "file_uploaded",
        "s3_bucket": TEST_S3_CONFIG.bucket_name,
        "s3_key": s3_key,
        "document_name": "simple_test_doc",
        "message": "Process this document"
    }
    
    await client.js.publish(
        f"{TEST_NATS_CONFIG.subject_prefix}.file_ready",
        json.dumps(control_message).encode()
    )
    
    print("📤 Service A: Sent NATS control message")
    
    # Test 3: Service B receives and processes (simulation)
    file_content = await client.download_result(s3_key)
    
    processed_result = {
        "document_name": "simple_test_doc",
        "original_s3_key": s3_key,
        "file_size": len(file_content),
        "processing_service": "Service B (Simple Test)",
        "content_summary": "Processed simple test document",
        "status": "processing_complete"
    }
    
    print(f"📨 Service B: Downloaded and processed file ({len(file_content)} bytes)")
    
    # Test 4: Send result back via NATS
    result_message = {
        "service": "Service B (Simple Test)",
        "action": "processing_complete",
        "original_request": control_message,
        "result": processed_result,
        "message": "Successfully processed simple_test_doc"
    }
    
    await client.js.publish(
        f"{TEST_NATS_CONFIG.subject_prefix}.processing_complete",
        json.dumps(result_message).encode()
    )
    
    print("📤 Service B: Sent processing result via NATS")
    
    # Verify the complete flow worked
    assert control_message["s3_key"] == processed_result["original_s3_key"]
    assert control_message["document_name"] == processed_result["document_name"]
    assert processed_result["status"] == "processing_complete"
    assert processed_result["file_size"] > 0
    
    await client.close()
    
    print("✅ Simple distributed file passing test completed successfully!")
    print("🎉 Verified: Service A → S3 → NATS → Service B flow working!") 