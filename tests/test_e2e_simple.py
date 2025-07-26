import pytest
import asyncio
import json
import uuid
import tempfile
import os
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import nats
from nats.js.client import JetStreamContext
from client import DocumentClient
from config import NatsConfig
import logging
import pytest_asyncio

logger = logging.getLogger(__name__)

# Simple E2E test configuration using default NATS server
from config import config
SIMPLE_E2E_CONFIG = config  # Use the default config to match client expectations

@pytest.fixture
def test_pdf():
    """Create a test PDF file"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        # Create a simple PDF with reportlab
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "Simple Test Document")
        c.drawString(100, 700, "This is a simple test PDF for end-to-end testing.")
        c.drawString(100, 650, "Using docling-parse integration.")
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        try:
            os.unlink(tmp.name)
        except:
            pass

@pytest_asyncio.fixture
async def simple_js_context():
    """Create JetStream context with test streams using running NATS server"""
    nc = await nats.connect(SIMPLE_E2E_CONFIG.url)
    js = nc.jetstream()
    
    # Create processing stream
    try:
        await js.add_stream(
            name=f"{SIMPLE_E2E_CONFIG.stream_name}_processing",
            subjects=[f"{SIMPLE_E2E_CONFIG.subject_prefix}.process.*"]
        )
    except Exception as e:
        logger.info(f"Stream may already exist: {e}")
    
    # Create results stream
    try:
        await js.add_stream(
            name=f"{SIMPLE_E2E_CONFIG.stream_name}_results",
            subjects=[f"{SIMPLE_E2E_CONFIG.subject_prefix}.result.*"]
        )
    except Exception as e:
        logger.info(f"Stream may already exist: {e}")
    
    # Create object store
    try:
        await js.create_object_store(SIMPLE_E2E_CONFIG.bucket_name)
    except Exception as e:
        logger.info(f"Object store may already exist: {e}")
    
    yield js
    
    # Cleanup streams after test
    try:
        await js.delete_stream(f"{SIMPLE_E2E_CONFIG.stream_name}_processing")
        await js.delete_stream(f"{SIMPLE_E2E_CONFIG.stream_name}_results")
        await js.delete_object_store(SIMPLE_E2E_CONFIG.bucket_name)
    except Exception as e:
        logger.info(f"Cleanup warning: {e}")
    
    await nc.close()

class SimpleMockDocumentWorker:
    """Simple mock worker for testing document processing"""
    
    def __init__(self, js_context: JetStreamContext):
        self.js = js_context
        self.running = False
        self.task = None
        
    async def start(self):
        """Start the mock worker"""
        self.running = True
        
        # Subscribe to processing requests
        sub = await self.js.pull_subscribe(
            subject=f"{SIMPLE_E2E_CONFIG.subject_prefix}.process.*",
            durable="simple_test_worker",
            stream=f"{SIMPLE_E2E_CONFIG.stream_name}_processing"
        )
        
        self.task = asyncio.create_task(self._process_messages(sub))
        
        # Give it a moment to subscribe
        await asyncio.sleep(0.1)
        
    async def stop(self):
        """Stop the mock worker"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
        # Clean up consumer
        try:
            await self.js.delete_consumer(
                stream=f"{SIMPLE_E2E_CONFIG.stream_name}_processing",
                consumer="simple_test_worker"
            )
        except Exception as e:
            logger.info(f"Consumer cleanup warning: {e}")
        
    async def _process_messages(self, subscription):
        """Process incoming document requests"""
        while self.running:
            try:
                messages = await subscription.fetch(batch=1, timeout=1.0)
                for msg in messages:
                    await self._handle_message(msg)
                    await msg.ack()
            except Exception as e:
                if self.running:  # Only log if we're supposed to be running
                    logger.debug(f"Worker fetch timeout or error: {e}")
                    
    async def _handle_message(self, msg):
        """Handle a single processing message"""
        try:
            data = json.loads(msg.data.decode())
            request_id = data['request_id']
            object_name = data['object_name']
            bucket = data['bucket']
            
            # Get document from object store
            obj_store = await self.js.object_store(bucket)
            object_result = await obj_store.get(object_name)
            document_data = object_result.data  # Extract actual bytes from ObjectResult
            
            # Simulate document processing with docling-parse
            result = {
                "request_id": request_id,
                "status": "success",
                "result": {
                    "text": "Processed: Simple test PDF content using docling-parse integration.",
                    "metadata": {
                        "title": "Simple Test Document", 
                        "pages": 1,
                        "size": len(document_data),
                        "format": "pdf"
                    },
                    "tables": [],
                    "figures": []
                }
            }
            
            # Send result back
            await self.js.publish(
                f"{SIMPLE_E2E_CONFIG.subject_prefix}.result.{request_id}",
                json.dumps(result).encode()
            )
            
            logger.info(f"Processed document {object_name} for request {request_id}")
            
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            # Send error response
            try:
                data = json.loads(msg.data.decode())
                request_id = data.get('request_id', 'unknown')
                
                error_result = {
                    "request_id": request_id,
                    "status": "error",
                    "error": str(e)
                }
                
                await self.js.publish(
                    f"{SIMPLE_E2E_CONFIG.subject_prefix}.result.{request_id}",
                    json.dumps(error_result).encode()
                )
            except:
                pass

@pytest_asyncio.fixture
async def simple_mock_worker(simple_js_context):
    """Create and start a simple mock document worker"""
    worker = SimpleMockDocumentWorker(simple_js_context)
    await worker.start()
    yield worker
    await worker.stop()

@pytest_asyncio.fixture
async def simple_e2e_client():
    """Create a DocumentClient configured for simple e2e testing"""
    
    class SimpleTestDocumentClient(DocumentClient):
        def __init__(self):
            super().__init__()
            self.config = SIMPLE_E2E_CONFIG
    
    client = SimpleTestDocumentClient()
    await client.setup()
    yield client
    await client.nc.close()

# Simple E2E Tests
pytestmark = pytest.mark.asyncio

@pytest.mark.simple_e2e
async def test_simple_nats_connection():
    """Test that we can connect to the running NATS server"""
    nc = await nats.connect(SIMPLE_E2E_CONFIG.url)
    assert nc.is_connected
    
    # Test JetStream is available
    js = nc.jetstream()
    account_info = await js.account_info()
    assert account_info is not None
    
    await nc.close()

@pytest.mark.simple_e2e
async def test_simple_document_processing_workflow(simple_e2e_client, simple_mock_worker, test_pdf):
    """Test the complete document processing workflow using running NATS server"""
    
    logger.info("Starting simple document processing test")
    
    # Process the test PDF
    result = await simple_e2e_client.process_document(test_pdf, timeout=15)
    
    # Verify result structure
    assert result["status"] == "success"
    assert "result" in result
    
    processed_result = result["result"]
    assert "text" in processed_result
    assert "metadata" in processed_result
    assert processed_result["metadata"]["pages"] == 1
    assert processed_result["metadata"]["format"] == "pdf"
    
    # Verify text content was processed
    assert len(processed_result["text"]) > 0
    assert "docling-parse" in processed_result["text"].lower()
    
    logger.info("Simple document processing test completed successfully")

@pytest.mark.simple_e2e
async def test_simple_document_processing_with_bytes(simple_e2e_client, simple_mock_worker, test_pdf):
    """Test document processing with bytes input using running NATS server"""
    
    # Read PDF as bytes
    pdf_bytes = test_pdf.read_bytes()
    
    # Process the bytes
    result = await simple_e2e_client.process_document(pdf_bytes, timeout=10)
    
    # Verify successful processing
    assert result["status"] == "success"
    assert result["result"]["metadata"]["size"] == len(pdf_bytes)

@pytest.mark.simple_e2e 
async def test_simple_document_processing_timeout(simple_e2e_client, simple_js_context, test_pdf):
    """Test document processing timeout when no worker is available"""
    
    # Don't start the mock worker - this should timeout
    with pytest.raises((TimeoutError, Exception), match="(Document processing timed out|nats: timeout)"):
        await simple_e2e_client.process_document(test_pdf, timeout=3) 