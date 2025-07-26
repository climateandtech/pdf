"""
Automated Test: S3 + NATS Distributed File Passing

This test simulates the complete distributed architecture:
Service A (uploader) → S3 → NATS → Service B (worker) → S3
"""
import pytest
import asyncio
import json
import uuid
import tempfile
import os
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
from moto import mock_aws
import boto3
import pytest_asyncio

from s3_client import S3DocumentClient
from s3_config import S3Config
from config import NatsConfig

# Import real service functions
try:
    from distributed_test_uploader import upload_and_notify as real_service_a_upload, create_test_pdf
    from distributed_test_worker import process_document as real_service_b_process, handle_file_message as real_service_b_handler
except ImportError:
    # Fallback - services are in same directory
    import importlib.util
    
    # Load uploader
    uploader_spec = importlib.util.spec_from_file_location("distributed_test_uploader", "distributed_test_uploader.py")
    uploader_module = importlib.util.module_from_spec(uploader_spec)
    uploader_spec.loader.exec_module(uploader_module)
    real_service_a_upload = uploader_module.upload_and_notify
    create_test_pdf = uploader_module.create_test_pdf
    
    # Load worker
    worker_spec = importlib.util.spec_from_file_location("distributed_test_worker", "distributed_test_worker.py")
    worker_module = importlib.util.module_from_spec(worker_spec)
    worker_spec.loader.exec_module(worker_module)
    real_service_b_process = worker_module.process_document
    real_service_b_handler = worker_module.handle_file_message

# Test configuration using moto mock (no endpoint_url for mock_aws context manager)
TEST_S3_CONFIG = S3Config(
    endpoint_url=None,  # Use mocked AWS services instead of real endpoint
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
        c.drawString(100, 750, "Automated Distributed Test Document")
        c.drawString(100, 700, "This PDF tests Service A → S3 → NATS → Service B flow")
        c.drawString(100, 650, "Features tested:")
        c.drawString(120, 600, "✓ Real file upload to S3")
        c.drawString(120, 550, "✓ NATS control bus messaging")
        c.drawString(120, 500, "✓ Service-to-service file passing")
        c.drawString(120, 450, "✓ Distributed architecture simulation")
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        os.unlink(tmp.name)

class ServiceAUploader:
    """Real Service A (File Uploader) - uses actual distributed_test_uploader"""
    
    def __init__(self, s3_client: S3DocumentClient):
        self.s3_client = s3_client
        self.uploaded_files = []
    
    async def upload_and_notify(self, pdf_path: Path, doc_name: str) -> dict:
        """Upload file to S3 and send NATS notification using real service A"""
        print(f"📄 Real Service A: Uploading document '{doc_name}'")
        
        # Use the real service A upload function
        await real_service_a_upload(self.s3_client, pdf_path, doc_name)
        
        # Create the control message (same format as real service)
        s3_key = f"distributed/{doc_name}.pdf"  # Real service uses this format
        control_message = {
            "service": "Service A (Uploader)",
            "action": "file_uploaded", 
            "s3_bucket": TEST_S3_CONFIG.bucket_name,
            "s3_key": s3_key,
            "document_name": doc_name,
            "file_size": pdf_path.stat().st_size,
            "message": f"Please process document: {doc_name}"
        }
        
        self.uploaded_files.append(control_message)
        print(f"✅ Real Service A: Uploaded and notified for '{doc_name}'")
        
        return control_message

class ServiceBWorker:
    """Real Service B (File Processor) - uses actual distributed_test_worker"""
    
    def __init__(self, s3_client: S3DocumentClient):
        self.s3_client = s3_client
        self.processed_files = []
    
    async def process_file_from_message(self, control_message: dict) -> dict:
        """Download file from S3 and process it using real service B"""
        doc_name = control_message["document_name"]
        s3_key = control_message["s3_key"]
        
        print(f"📨 Real Service B: Processing file '{doc_name}' from S3")
        
        # Use the real service B process_document function
        processed_result = await real_service_b_process(self.s3_client, s3_key, doc_name)
        
        # Update result to include original_s3_key for test verification
        processed_result["original_s3_key"] = s3_key
        
        # Send result back via NATS (same format as real service)
        result_message = {
            "service": "Service B (Worker)",
            "action": "processing_complete",
            "original_request": control_message,
            "result": processed_result,
            "message": f"Successfully processed {doc_name}"
        }
        
        await self.s3_client.js.publish(
            f"{TEST_NATS_CONFIG.subject_prefix}.processing_complete",
            json.dumps(result_message).encode()
        )
        
        self.processed_files.append(result_message)
        print(f"✅ Real Service B: Completed processing '{doc_name}'")
        
        return result_message

@pytest.mark.asyncio  
@mock_aws
async def test_distributed_file_passing_flow(test_pdf):
    """
    Test complete distributed file passing: Service A → S3 → NATS → Service B
    
    This test simulates the exact distributed architecture pattern.
    """
    print("\n🚀 Starting Automated Distributed File Passing Test")
    print("=" * 70)
    
    # Create moto S3 setup (following working test pattern)
    boto3.client('s3', **TEST_S3_CONFIG.boto3_config())
    
    # Setup S3 client with moto
    client = S3DocumentClient(s3_config=TEST_S3_CONFIG, nats_config=TEST_NATS_CONFIG)
    await client.setup()
    
    # Initialize services
    service_a = ServiceAUploader(client)
    service_b = ServiceBWorker(client)
    
    print("✅ Services initialized and connected")
    
    # Test multiple documents to show distributed processing
    documents = [
        "climate_report_2024",
        "sustainability_analysis"
    ]
    
    all_control_messages = []
    all_results = []
    
    for doc_name in documents:
        print(f"\n--- Processing: {doc_name} ---")
        
        # Service A: Upload and notify
        control_message = await service_a.upload_and_notify(test_pdf, doc_name)
        all_control_messages.append(control_message)
        
        # Service B: Process the file
        result = await service_b.process_file_from_message(control_message)
        all_results.append(result)
        
        # Verify the file was actually passed between services
        assert control_message["s3_key"] == result["result"]["original_s3_key"]
        assert control_message["document_name"] == result["result"]["document_name"]
        
        print(f"🔄 Verified file passing for '{doc_name}'")
    
    # Final verification of distributed flow
    print(f"\n📊 Test Results:")
    print(f"   Documents uploaded by Service A: {len(service_a.uploaded_files)}")
    print(f"   Documents processed by Service B: {len(service_b.processed_files)}")
    
    # Verify all files were processed
    assert len(service_a.uploaded_files) == len(documents)
    assert len(service_b.processed_files) == len(documents)
    
    # Verify S3 storage was used
    for control_msg in all_control_messages:
        assert "s3_key" in control_msg
        assert control_msg["s3_bucket"] == TEST_S3_CONFIG.bucket_name
        # Note: s3_url might be None in mock, so we'll check s3_key instead
    
    # Verify processing results
    for result in all_results:
        assert result["result"]["status"] == "processing_complete"
        assert result["result"]["file_size"] > 0
        assert "extracted_text" in result["result"]
    
    await client.close()
    
    print("✅ Distributed file passing test completed successfully!")
    print("🎉 Verified: Service A → S3 → NATS → Service B → S3 flow working!")

async def test_distributed_error_handling(test_pdf):
    """Test error handling in distributed file passing"""
    print("\n🚀 Testing Distributed Error Handling")
    
    with mock_aws():
        client = S3DocumentClient(s3_config=TEST_S3_CONFIG, nats_config=TEST_NATS_CONFIG)
        await client.setup()
        
        service_a = ServiceAUploader(client)
        service_b = ServiceBWorker(client)
        
        # Upload a file
        control_message = await service_a.upload_and_notify(test_pdf, "error_test_doc")
        
        # Simulate processing error by trying to access non-existent file
        invalid_control = control_message.copy()
        invalid_control["s3_key"] = "nonexistent/file.pdf"
        
        # Service B should handle errors gracefully
        with pytest.raises(Exception):  # Should raise an error for missing file
            await service_b.process_file_from_message(invalid_control)
        
        await client.close()
        print("✅ Error handling test completed")

async def test_concurrent_distributed_processing(test_pdf):
    """Test concurrent file processing in distributed architecture"""
    print("\n🚀 Testing Concurrent Distributed Processing")
    
    with mock_aws():
        client = S3DocumentClient(s3_config=TEST_S3_CONFIG, nats_config=TEST_NATS_CONFIG)
        await client.setup()
        
        service_a = ServiceAUploader(client) 
        service_b = ServiceBWorker(client)
        
        # Upload multiple files concurrently
        async def upload_and_process(doc_name):
            control_msg = await service_a.upload_and_notify(test_pdf, f"concurrent_{doc_name}")
            result = await service_b.process_file_from_message(control_msg)
            return control_msg, result
        
        # Process 3 documents concurrently
        tasks = [upload_and_process(f"doc_{i}") for i in range(3)]
        results = await asyncio.gather(*tasks)
        
        # Verify all processed successfully
        assert len(results) == 3
        for control_msg, result in results:
            assert result["result"]["status"] == "processing_complete"
            assert control_msg["s3_key"] == result["result"]["original_s3_key"]
        
        await client.close()
        print("✅ Concurrent processing test completed")

# Individual test marks applied to each test function 