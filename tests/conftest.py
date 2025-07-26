import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch
import nats
import json


@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_nc():
    """Mock NATS connection."""
    mock_nc = AsyncMock()
    mock_nc.jetstream.return_value = AsyncMock()
    mock_nc.is_connected = True
    mock_nc.close = AsyncMock()
    mock_nc.drain = AsyncMock()
    return mock_nc


@pytest.fixture
async def mock_js():
    """Mock JetStream context."""
    mock_js = AsyncMock()
    
    # Mock stream operations
    mock_js.add_stream = AsyncMock()
    mock_js.delete_stream = AsyncMock()
    mock_js.stream_info = AsyncMock()
    
    # Mock consumer operations
    mock_js.add_consumer = AsyncMock()
    mock_js.delete_consumer = AsyncMock()
    
    # Mock publish/subscribe operations
    mock_js.publish = AsyncMock()
    mock_js.pull_subscribe = AsyncMock()
    
    return mock_js


@pytest.fixture
async def mock_object_store():
    """Mock JetStream Object Store."""
    mock_os = AsyncMock()
    
    # Mock object operations
    mock_os.put = AsyncMock()
    mock_os.get = AsyncMock()
    mock_os.delete = AsyncMock()
    mock_os.list = AsyncMock()
    mock_os.info = AsyncMock()
    
    return mock_os


@pytest.fixture
def mock_docling_result():
    """Mock Docling processing result."""
    return {
        "content": "This is processed document content",
        "metadata": {
            "title": "Test Document",
            "pages": 1,
            "format": "pdf"
        },
        "tables": [],
        "figures": []
    }


@pytest.fixture
def sample_document():
    """Sample document for testing."""
    return {
        "filename": "test.pdf",
        "content": b"Sample PDF content for testing",
        "metadata": {
            "size": 1024,
            "mime_type": "application/pdf"
        }
    } 