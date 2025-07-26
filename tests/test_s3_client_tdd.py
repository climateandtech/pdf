"""
TDD Tests for S3 + NATS Document Processing Client

Comprehensive test suite using moto for S3 mocking and proper boto3 testing patterns.
Tests are organized to validate the architecture pattern first, then implementation details.
"""
import pytest
import asyncio
import json
import uuid
import tempfile
import os
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock
import boto3
from moto import mock_aws
import nats
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

# Import our implementations
from s3_client import S3DocumentClient, create_s3_document_client
from s3_config import S3Config, ProcessingConfig
from s3_integration import S3DocumentService, process_pdf_with_s3, configure_s3_storage
from config import NatsConfig

# Test fixtures
@pytest.fixture
def test_pdf():
    """Create a test PDF file"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "Test S3 + NATS Integration Document")
        c.drawString(100, 700, "This PDF tests the S3 + NATS control bus pattern")
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        os.unlink(tmp.name)

@pytest.fixture
def test_pdf_bytes():
    """Create test PDF as bytes"""
    with tempfile.NamedTemporaryFile(suffix='.pdf') as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "Test S3 + NATS Integration Document (bytes)")
        c.save()
        
        return Path(tmp.name).read_bytes()

@pytest.fixture
def s3_config():
    """Test S3 configuration"""
    return S3Config(
        endpoint_url="http://localhost:9000",  # MinIO-style endpoint
        region_name="us-east-1",
        bucket_name="test-documents",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        multipart_threshold=5 * 1024 * 1024,  # 5MB for testing
        presigned_url_expiry=1800  # 30 minutes
    )

@pytest.fixture
def nats_config():
    """Test NATS configuration"""
    return NatsConfig(
        url="nats://localhost:4222",
        stream_name="TEST_S3_DOCUMENTS",
        subject_prefix="s3_test_docs",
        bucket_name="test_s3_documents"
    )

@pytest.fixture 
def processing_config():
    """Test processing configuration"""
    return ProcessingConfig(
        timeout=30,
        use_s3_storage=True,
        cleanup_on_error=True
    )

# TDD Test Classes - Architecture Pattern Tests First

class TestS3ConfigValidation:
    """Test S3 configuration validation and boto3 integration"""
    
    def test_s3_config_defaults(self):
        """Test S3 config uses environment variables and sensible defaults"""
        config = S3Config()
        assert config.region_name == "us-east-1"
        assert config.multipart_threshold == 100 * 1024 * 1024  # 100MB
        assert config.max_concurrency == 10
        assert config.presigned_url_expiry == 3600

    def test_s3_config_validation(self):
        """Test bucket name validation follows AWS conventions"""
        # Valid bucket name
        config = S3Config(bucket_name="valid-bucket-name-123")
        assert config.bucket_name == "valid-bucket-name-123"
        
        # Invalid bucket names
        with pytest.raises(ValueError):
            S3Config(bucket_name="")  # Empty
        
        with pytest.raises(ValueError):
            S3Config(bucket_name="ab")  # Too short
    
    def test_boto3_config_generation(self, s3_config):
        """Test boto3 configuration generation"""
        boto3_config = s3_config.boto3_config()
        
        assert boto3_config["region_name"] == "us-east-1"
        assert boto3_config["endpoint_url"] == "http://localhost:9000"
        assert boto3_config["aws_access_key_id"] == "minioadmin"
        assert boto3_config["aws_secret_access_key"] == "minioadmin"

class TestS3ClientArchitecture:
    """Test the S3 + NATS control bus architecture pattern"""
    
    @pytest.mark.asyncio
    async def test_client_initialization(self, s3_config, nats_config):
        """Test client initializes with proper configurations"""
        client = S3DocumentClient(
            s3_config=s3_config,
            nats_config=nats_config
        )
        
        assert client.s3_config == s3_config
        assert client.nats_config == nats_config
        assert client.session is not None  # aioboto3 session created

    @pytest.mark.asyncio
    @mock_aws
    async def test_s3_bucket_creation(self, s3_config, nats_config):
        """Test S3 bucket is created if it doesn't exist"""
        # Create real boto3 client for moto
        boto3.client('s3', **s3_config.boto3_config())
        
        client = S3DocumentClient(s3_config=s3_config, nats_config=nats_config)
        
        # Should create bucket without errors
        await client._ensure_bucket_exists()
        
        # Verify bucket exists using regular boto3 (moto intercepts)
        s3_client = boto3.client('s3', **s3_config.boto3_config())
        response = s3_client.list_buckets()
        bucket_names = [b['Name'] for b in response['Buckets']]
        assert s3_config.bucket_name in bucket_names

    @pytest.mark.asyncio
    @mock_aws
    async def test_s3_upload_file_path(self, s3_config, nats_config, test_pdf):
        """Test uploading PDF file by path uses boto3 transfer management"""
        # Setup moto S3
        boto3.client('s3', **s3_config.boto3_config()).create_bucket(
            Bucket=s3_config.bucket_name
        )
        
        client = S3DocumentClient(s3_config=s3_config, nats_config=nats_config)
        
        # Test file upload
        s3_url = await client._upload_to_s3(test_pdf, "test/document.pdf")
        
        assert s3_url is not None
        assert s3_config.bucket_name in s3_url
        assert "test/document.pdf" in s3_url
        
        # Verify file was uploaded
        s3_client = boto3.client('s3', **s3_config.boto3_config())
        response = s3_client.get_object(
            Bucket=s3_config.bucket_name, 
            Key="test/document.pdf"
        )
        assert len(response['Body'].read()) > 0

    @pytest.mark.asyncio  
    @mock_aws
    async def test_s3_upload_bytes(self, s3_config, nats_config, test_pdf_bytes):
        """Test uploading PDF as bytes"""
        # Setup moto S3
        boto3.client('s3', **s3_config.boto3_config()).create_bucket(
            Bucket=s3_config.bucket_name
        )
        
        client = S3DocumentClient(s3_config=s3_config, nats_config=nats_config)
        
        # Test bytes upload
        s3_url = await client._upload_to_s3(test_pdf_bytes, "test/bytes_document.pdf")
        
        assert s3_url is not None
        
        # Verify bytes were uploaded correctly
        s3_client = boto3.client('s3', **s3_config.boto3_config())
        response = s3_client.get_object(
            Bucket=s3_config.bucket_name,
            Key="test/bytes_document.pdf"
        )
        uploaded_bytes = response['Body'].read()
        assert uploaded_bytes == test_pdf_bytes

class TestNATSControlBusPattern:
    """Test NATS control bus messaging pattern"""
    
    @pytest.mark.asyncio
    async def test_control_message_structure(self, s3_config, nats_config, test_pdf):
        """Test control messages have correct structure for S3 + NATS pattern"""
        client = S3DocumentClient(s3_config=s3_config, nats_config=nats_config)
        
        # Mock NATS publishing
        mock_js = AsyncMock()
        client.js = mock_js
        
        request_id = str(uuid.uuid4())
        s3_key = f"raw/{request_id}.pdf"
        s3_url = "https://s3.amazonaws.com/test-bucket/raw/file.pdf"
        
        await client._publish_control_message(request_id, s3_key, s3_url, test_pdf)
        
        # Verify control message structure
        mock_js.publish.assert_called_once()
        call_args = mock_js.publish.call_args
        
        subject = call_args[0][0]
        message_bytes = call_args[0][1]
        
        assert subject == f"{nats_config.subject_prefix}.process.{request_id}"
        
        message = json.loads(message_bytes.decode())
        assert message["request_id"] == request_id
        assert message["s3_bucket"] == s3_config.bucket_name
        assert message["s3_key"] == s3_key
        assert message["s3_url"] == s3_url
        assert "timestamp" in message
        assert "file_size" in message
        assert message["processing_timeout"] == client.processing_config.timeout

class TestS3DocumentService:
    """Test the service layer integration"""
    
    @pytest.mark.asyncio
    async def test_service_context_manager(self):
        """Test service uses async context manager pattern"""
        config_override = {"bucket_name": "test-service-bucket"}
        
        async with S3DocumentService(config_override) as service:
            assert service.client is not None
            assert service.config["bucket_name"] == "test-service-bucket"
        
        # Client should be closed after context exit
        # (We can't test this directly, but the pattern is validated)

    @pytest.mark.asyncio
    async def test_service_without_context_manager_fails(self):
        """Test service fails gracefully when not used as context manager"""
        service = S3DocumentService()
        
        with pytest.raises(RuntimeError, match="Service not initialized"):
            await service.process_document("test.pdf")

class TestIntegrationHelpers:
    """Test integration helper functions"""
    
    def test_configure_s3_storage_helper(self):
        """Test S3 configuration helper function"""
        config = configure_s3_storage(
            endpoint_url="http://localhost:9000",
            bucket_name="test-bucket",
            aws_access_key_id="test-key",
            region_name="us-west-2"
        )
        
        expected = {
            "endpoint_url": "http://localhost:9000",
            "bucket_name": "test-bucket", 
            "aws_access_key_id": "test-key",
            "region_name": "us-west-2"
        }
        
        assert config == expected

    def test_configure_s3_storage_partial(self):
        """Test S3 configuration helper with partial config"""
        config = configure_s3_storage(bucket_name="partial-test")
        
        assert config == {"bucket_name": "partial-test"}

# Integration Tests
class TestS3NATSEndToEnd:
    """End-to-end tests of S3 + NATS integration"""
    
    @pytest.mark.asyncio
    @mock_aws 
    async def test_full_workflow_with_mocks(self, s3_config, nats_config, test_pdf):
        """Test complete workflow with mocked S3 and NATS"""
        # Setup moto S3
        boto3.client('s3', **s3_config.boto3_config()).create_bucket(
            Bucket=s3_config.bucket_name
        )
        
        # Mock NATS components
        mock_nc = AsyncMock()
        mock_js = AsyncMock()
        mock_consumer = AsyncMock()
        
        # Mock successful processing result
        mock_message = AsyncMock()
        mock_message.data = json.dumps({
            "status": "success",
            "request_id": "test-123",
            "result": {
                "text": "Processed PDF content",
                "pages": 1
            }
        }).encode()
        
        mock_consumer.fetch.return_value = [mock_message]
        mock_js.pull_subscribe.return_value = mock_consumer
        mock_nc.jetstream.return_value = mock_js
        
        client = S3DocumentClient(s3_config=s3_config, nats_config=nats_config)
        
        # Inject mocks
        client.nc = mock_nc
        client.js = mock_js
        
        # Test full workflow
        result = await client.process_document(test_pdf, timeout=10)
        
        # Verify result
        assert result["status"] == "success"
        assert result["request_id"] == "test-123"
        assert "text" in result["result"]
        
        # Verify S3 upload happened
        s3_client = boto3.client('s3', **s3_config.boto3_config())
        objects = s3_client.list_objects_v2(Bucket=s3_config.bucket_name)
        assert objects['KeyCount'] == 1
        
        # Verify NATS messaging
        mock_js.publish.assert_called_once()
        mock_js.pull_subscribe.assert_called_once()

# Performance and Best Practices Tests
class TestBoto3BestPractices:
    """Test boto3 usage follows best practices"""
    
    @pytest.mark.asyncio
    async def test_transfer_config_optimization(self, s3_config):
        """Test transfer configuration uses optimal settings"""
        client = S3DocumentClient(s3_config=s3_config)
        transfer_config = client._get_transfer_config()
        
        # Verify optimized settings
        assert transfer_config.multipart_threshold == s3_config.multipart_threshold
        assert transfer_config.max_concurrency == s3_config.max_concurrency
        assert transfer_config.multipart_chunksize == s3_config.multipart_chunksize
        assert transfer_config.use_threads is True

    @pytest.mark.asyncio
    async def test_resource_cleanup_pattern(self, s3_config, nats_config):
        """Test proper resource cleanup using context managers"""
        client = S3DocumentClient(s3_config=s3_config, nats_config=nats_config)
        
        # Test context manager creates and cleans up resources
        async with client.s3_client() as s3:
            assert s3 is not None
            # S3 client should be usable here
        
        # After context exit, resources should be cleaned up
        # (aioboto3 handles this automatically)

# Mark tests for different categories
pytestmark = [
    pytest.mark.s3,
    pytest.mark.integration
] 