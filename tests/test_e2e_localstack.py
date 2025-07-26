"""
End-to-End Tests with LocalStack

Comprehensive E2E testing using LocalStack for realistic S3 + NATS testing.
LocalStack provides a fully functional local AWS cloud stack.
"""
import pytest
import asyncio
import json
import uuid
import tempfile
import os
import time
import subprocess
import requests
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import boto3
from botocore.exceptions import ClientError

from s3_client import S3DocumentClient, create_s3_document_client
from s3_config import S3Config, ProcessingConfig
from s3_integration import S3DocumentService, process_pdf_with_s3_sync, configure_s3_storage
from config import NatsConfig

# LocalStack configuration
LOCALSTACK_ENDPOINT = "http://localhost:4566"
LOCALSTACK_S3_ENDPOINT = f"{LOCALSTACK_ENDPOINT}"

@pytest.fixture(scope="session")
def localstack_health_check():
    """Check if LocalStack is running and healthy"""
    try:
        response = requests.get(f"{LOCALSTACK_ENDPOINT}/health")
        health = response.json()
        
        if health.get("features", {}).get("persistence") != "disabled":
            print("⚠️  LocalStack persistence enabled - tests may have side effects")
        
        s3_status = health.get("services", {}).get("s3", "unavailable")
        if s3_status != "available":
            pytest.skip(f"LocalStack S3 service not available: {s3_status}")
        
        print(f"✅ LocalStack health check passed: S3 {s3_status}")
        return True
        
    except requests.RequestException:
        pytest.skip("LocalStack not running. Start with: docker run -p 4566:4566 localstack/localstack")

@pytest.fixture
def localstack_s3_config():
    """S3 configuration for LocalStack"""
    return S3Config(
        endpoint_url=LOCALSTACK_S3_ENDPOINT,
        region_name="us-east-1",
        bucket_name=f"test-localstack-{uuid.uuid4().hex[:8]}",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        multipart_threshold=5 * 1024 * 1024,  # 5MB for testing
        presigned_url_expiry=300  # 5 minutes
    )

@pytest.fixture
def nats_config():
    """NATS configuration for LocalStack tests"""
    return NatsConfig(
        url="nats://localhost:4222",
        stream_name=f"LOCALSTACK_TEST_{uuid.uuid4().hex[:8]}",
        subject_prefix="localstack_test",
        bucket_name=f"localstack_test_{uuid.uuid4().hex[:8]}"
    )

@pytest.fixture
def test_pdf():
    """Create test PDF for LocalStack tests"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "LocalStack E2E Test Document")
        c.drawString(100, 700, "This PDF tests S3 + NATS with LocalStack")
        c.drawString(100, 650, "Features tested:")
        c.drawString(120, 600, "✓ Real S3 API compatibility")
        c.drawString(120, 550, "✓ Multipart uploads")
        c.drawString(120, 500, "✓ Presigned URLs")
        c.drawString(120, 450, "✓ Bucket lifecycle")
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        os.unlink(tmp.name)

@pytest.fixture
async def localstack_s3_client(localstack_s3_config):
    """Create S3 client configured for LocalStack"""
    client = S3DocumentClient(s3_config=localstack_s3_config)
    
    # Ensure S3 service is ready
    await client._ensure_bucket_exists()
    
    return client

class TestLocalStackS3Integration:
    """Test S3 integration using LocalStack"""
    
    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_bucket_operations(self, localstack_health_check, localstack_s3_config):
        """Test basic S3 bucket operations with LocalStack"""
        client = S3DocumentClient(s3_config=localstack_s3_config)
        
        # Test bucket creation
        await client._ensure_bucket_exists()
        
        # Verify bucket exists using direct boto3 call
        s3_client = boto3.client('s3', **localstack_s3_config.boto3_config())
        response = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in response['Buckets']]
        
        assert localstack_s3_config.bucket_name in bucket_names
        print(f"✅ Bucket created in LocalStack: {localstack_s3_config.bucket_name}")

    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_file_upload_download(self, localstack_health_check, localstack_s3_client, test_pdf):
        """Test file upload and download with LocalStack"""
        s3_key = f"test/{uuid.uuid4()}.pdf"
        
        # Upload file
        s3_url = await localstack_s3_client._upload_to_s3(test_pdf, s3_key)
        
        # Verify presigned URL was generated
        assert "localhost:4566" in s3_url or "localstack" in s3_url
        assert s3_key in s3_url
        print(f"✅ File uploaded to LocalStack: {s3_key}")
        
        # Test download
        downloaded_content = await localstack_s3_client.download_result(s3_key)
        original_content = test_pdf.read_bytes()
        
        assert downloaded_content == original_content
        print(f"✅ File downloaded successfully: {len(downloaded_content)} bytes")

    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_multipart_upload(self, localstack_health_check, localstack_s3_client):
        """Test multipart upload with LocalStack using larger file"""
        # Create a larger test file (10MB) to trigger multipart upload
        large_content = b"LocalStack multipart test data\n" * (10 * 1024 * 1024 // 31)
        
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp.write(large_content)
            tmp.flush()
            large_file = Path(tmp.name)
        
        try:
            s3_key = f"multipart/{uuid.uuid4()}.bin"
            
            # Upload large file (should trigger multipart)
            s3_url = await localstack_s3_client._upload_to_s3(large_file, s3_key)
            
            # Verify upload
            downloaded = await localstack_s3_client.download_result(s3_key)
            assert len(downloaded) == len(large_content)
            print(f"✅ Multipart upload successful: {len(downloaded)} bytes")
            
        finally:
            large_file.unlink()

    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_presigned_url_access(self, localstack_health_check, localstack_s3_client, test_pdf):
        """Test presigned URL access with LocalStack"""
        s3_key = f"presigned/{uuid.uuid4()}.pdf"
        
        # Upload and get presigned URL
        s3_url = await localstack_s3_client._upload_to_s3(test_pdf, s3_key)
        
        # Test accessing the presigned URL
        response = requests.get(s3_url)
        assert response.status_code == 200
        assert len(response.content) > 0
        print(f"✅ Presigned URL accessible: {response.status_code}")

class TestLocalStackServiceLayer:
    """Test service layer integration with LocalStack"""
    
    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_service_context_manager(self, localstack_health_check, localstack_s3_config):
        """Test S3DocumentService with LocalStack"""
        config_override = localstack_s3_config.dict()
        
        async with S3DocumentService(config_override) as service:
            assert service.client is not None
            
            # Test that S3 bucket was created
            s3_client = boto3.client('s3', **localstack_s3_config.boto3_config())
            buckets = s3_client.list_buckets()['Buckets']
            bucket_names = [b['Name'] for b in buckets]
            
            assert localstack_s3_config.bucket_name in bucket_names
            print(f"✅ Service layer working with LocalStack")

    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_error_handling(self, localstack_health_check, localstack_s3_config):
        """Test error handling with LocalStack"""
        # Test with invalid bucket name
        invalid_config = localstack_s3_config.copy()
        invalid_config.bucket_name = "invalid..bucket..name"
        
        client = S3DocumentClient(s3_config=invalid_config)
        
        with pytest.raises(ClientError):
            await client._ensure_bucket_exists()
        
        print("✅ Error handling works with LocalStack")

class TestLocalStackPerformance:
    """Performance tests with LocalStack"""
    
    @pytest.mark.localstack
    @pytest.mark.asyncio
    async def test_localstack_concurrent_uploads(self, localstack_health_check, localstack_s3_client, test_pdf):
        """Test concurrent uploads with LocalStack"""
        async def upload_file(index):
            s3_key = f"concurrent/file_{index}_{uuid.uuid4()}.pdf"
            return await localstack_s3_client._upload_to_s3(test_pdf, s3_key)
        
        # Upload 5 files concurrently
        start_time = time.time()
        tasks = [upload_file(i) for i in range(5)]
        results = await asyncio.gather(*tasks)
        end_time = time.time()
        
        assert len(results) == 5
        assert all(url is not None for url in results)
        
        print(f"✅ Concurrent uploads completed in {end_time - start_time:.2f}s")

# Helper functions for LocalStack management
def start_localstack():
    """Start LocalStack using Docker"""
    cmd = [
        "docker", "run", "-d",
        "--name", "localstack-test",
        "-p", "4566:4566",
        "-e", "SERVICES=s3",
        "-e", "DEBUG=1",
        "localstack/localstack"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        print("✅ LocalStack started successfully")
        
        # Wait for LocalStack to be ready
        for _ in range(30):  # Wait up to 30 seconds
            try:
                response = requests.get(f"{LOCALSTACK_ENDPOINT}/health")
                if response.status_code == 200:
                    print("✅ LocalStack is ready")
                    return True
            except requests.RequestException:
                pass
            time.sleep(1)
        
        print("❌ LocalStack failed to become ready")
        return False
        
    except subprocess.CalledProcessError as e:
        if "already in use" in e.stderr:
            print("⚠️  LocalStack container already running")
            return True
        print(f"❌ Failed to start LocalStack: {e}")
        return False

def stop_localstack():
    """Stop LocalStack container"""
    try:
        subprocess.run(["docker", "stop", "localstack-test"], check=True, capture_output=True)
        subprocess.run(["docker", "rm", "localstack-test"], check=True, capture_output=True)
        print("✅ LocalStack stopped and removed")
    except subprocess.CalledProcessError:
        print("⚠️  LocalStack container not found or already stopped")

# Pytest configuration for LocalStack tests
def pytest_configure(config):
    """Configure pytest with LocalStack custom markers"""
    config.addinivalue_line(
        "markers", "localstack: mark test as requiring LocalStack"
    )

def pytest_collection_modifyitems(config, items):
    """Skip LocalStack tests if not available"""
    localstack_available = False
    try:
        response = requests.get(f"{LOCALSTACK_ENDPOINT}/health", timeout=2)
        localstack_available = response.status_code == 200
    except:
        pass
    
    if not localstack_available:
        skip_localstack = pytest.mark.skip(reason="LocalStack not available")
        for item in items:
            if "localstack" in item.keywords:
                item.add_marker(skip_localstack)

# Mark all tests in this module
pytestmark = pytest.mark.localstack 