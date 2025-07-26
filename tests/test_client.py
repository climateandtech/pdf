import pytest
import asyncio
from client import DocumentClient
import uuid
import json
from datetime import datetime

pytestmark = pytest.mark.asyncio

@pytest.fixture
async def client(js_context):
    """Create a client instance"""
    client = DocumentClient()
    await client.setup()
    yield client
    await client.nc.close()

async def test_client_setup(client):
    """Test client initialization"""
    assert client.nc is not None
    assert client.js is not None
    
    # Verify object store exists
    stores = await client.js.object_stores_info()
    assert any(store.config.name == client.config.bucket_name for store in stores)

async def test_process_document(client, js_context, test_pdf):
    """Test document processing workflow"""
    # Read test PDF
    content = test_pdf.read_bytes()
    
    # Setup mock worker
    async def mock_worker(msg):
        data = json.loads(msg.data.decode())
        
        # Get document from object store
        obj = await js_context.object_store(client.config.bucket_name)
        document = await obj.get(data["object_name"])
        assert document == content
        
        # Send success response
        await js_context.publish(
            f"docs.result.{data['request_id']}",
            json.dumps({
                "request_id": data["request_id"],
                "status": "success",
                "result": {
                    "text": "Processed content",
                    "metadata": {"pages": 1}
                }
            }).encode()
        )
    
    # Subscribe mock worker
    await js_context.pull_subscribe(
        subject="docs.process.*",
        durable="test_worker",
        stream="DOCUMENTS",
        cb=mock_worker
    )
    
    # Process document
    result = await client.process_document(content)
    
    # Verify result
    assert result["status"] == "success"
    assert "text" in result["result"]
    assert result["result"]["metadata"]["pages"] == 1

async def test_process_file_path(client, js_context, test_pdf):
    """Test processing document from file path"""
    async def mock_worker(msg):
        data = json.loads(msg.data.decode())
        await js_context.publish(
            f"docs.result.{data['request_id']}",
            json.dumps({
                "request_id": data["request_id"],
                "status": "success",
                "result": {"processed": True}
            }).encode()
        )
    
    await js_context.pull_subscribe(
        subject="docs.process.*",
        durable="test_worker",
        stream="DOCUMENTS",
        cb=mock_worker
    )
    
    # Process using file path
    result = await client.process_document(str(test_pdf))
    assert result["status"] == "success"

async def test_error_handling(client, js_context):
    """Test client error handling"""
    async def mock_worker(msg):
        data = json.loads(msg.data.decode())
        await js_context.publish(
            f"docs.result.{data['request_id']}",
            json.dumps({
                "request_id": data["request_id"],
                "status": "error",
                "error": "Processing failed"
            }).encode()
        )
    
    await js_context.pull_subscribe(
        subject="docs.process.*",
        durable="test_worker",
        stream="DOCUMENTS",
        cb=mock_worker
    )
    
    # Process invalid document
    with pytest.raises(Exception) as exc_info:
        await client.process_document(b"invalid document")
    assert "Processing failed" in str(exc_info.value)

async def test_timeout_handling(client, js_context):
    """Test handling of worker timeout"""
    async def mock_worker(msg):
        # Worker doesn't respond - should trigger timeout
        pass
    
    await js_context.pull_subscribe(
        subject="docs.process.*",
        durable="test_worker",
        stream="DOCUMENTS",
        cb=mock_worker
    )
    
    # Process with short timeout
    with pytest.raises(Exception) as exc_info:
        await client.process_document(b"test content", timeout=1)
    assert "timeout" in str(exc_info.value).lower()

async def test_cleanup_on_error(client, js_context):
    """Test cleanup of stored documents on error"""
    async def mock_worker(msg):
        data = json.loads(msg.data.decode())
        await js_context.publish(
            f"docs.result.{data['request_id']}",
            json.dumps({
                "request_id": data["request_id"],
                "status": "error",
                "error": "Processing failed"
            }).encode()
        )
    
    await js_context.pull_subscribe(
        subject="docs.process.*",
        durable="test_worker",
        stream="DOCUMENTS",
        cb=mock_worker
    )
    
    # Process document that will fail
    with pytest.raises(Exception):
        await client.process_document(b"test content")
    
    # Verify no documents left in object store
    obj = await js_context.object_store(client.config.bucket_name)
    objects = await obj.list()
    assert len(objects) == 0

async def test_large_document(client, js_context, large_test_pdf):
    """Test processing of large documents"""
    content = large_test_pdf.read_bytes()
    assert len(content) > 1024 * 1024  # Verify file is large
    
    async def mock_worker(msg):
        data = json.loads(msg.data.decode())
        
        # Verify large document was stored correctly
        obj = await js_context.object_store(client.config.bucket_name)
        document = await obj.get(data["object_name"])
        assert document == content
        
        await js_context.publish(
            f"docs.result.{data['request_id']}",
            json.dumps({
                "request_id": data["request_id"],
                "status": "success",
                "result": {"processed": True}
            }).encode()
        )
    
    await js_context.pull_subscribe(
        subject="docs.process.*",
        durable="test_worker",
        stream="DOCUMENTS",
        cb=mock_worker
    )
    
    # Process large document
    result = await client.process_document(content)
    assert result["status"] == "success" 