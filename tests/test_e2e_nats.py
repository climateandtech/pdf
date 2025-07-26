import pytest
import asyncio
import subprocess
import time
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
import signal
import pytest_asyncio

logger = logging.getLogger(__name__)

# E2E test configuration - use different ports to avoid conflicts
E2E_CONFIG = NatsConfig(
    url="nats://localhost:4223",  # Different port for e2e tests
    stream_name="TEST_DOCUMENTS",
    subject_prefix="test_docs",
    bucket_name="test_documents"
)

class NATSTestServer:
    """Manages a NATS server instance for testing"""
    
    def __init__(self, port=4223):
        self.port = port
        self.process = None
        self.temp_dir = None
        
    async def start(self):
        """Start NATS server for testing"""
        self.temp_dir = tempfile.mkdtemp()
        
        # Start nats-server with JetStream enabled
        cmd = [
            "nats-server", 
            "-a", "127.0.0.1",  # Bind to IPv4 only
            "-p", str(self.port),
            "-js",  # Enable JetStream
            "-sd", self.temp_dir,  # Storage directory
            "-l", os.path.join(self.temp_dir, "nats.log")  # Log file
        ]
        
        logger.info(f"Starting NATS server with command: {' '.join(cmd)}")
        
        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid  # Create new process group
        )
        
        # Wait for server to be ready with exponential backoff
        max_attempts = 30
        for attempt in range(max_attempts):
            try:
                nc = await nats.connect(f"nats://127.0.0.1:{self.port}", connect_timeout=1)
                await nc.close()
                logger.info(f"NATS test server started successfully on port {self.port}")
                return
            except Exception as e:
                if attempt < max_attempts - 1:
                    wait_time = 0.1 * (2 ** min(attempt, 5))  # Exponential backoff, max 3.2s
                    logger.debug(f"Attempt {attempt + 1}: NATS not ready, waiting {wait_time}s... ({e})")
                    await asyncio.sleep(wait_time)
                else:
                    # Get server logs for debugging
                    if self.process and self.process.poll() is not None:
                        stdout, stderr = self.process.communicate()
                        logger.error(f"NATS server failed to start. Stdout: {stdout.decode()}, Stderr: {stderr.decode()}")
                    raise Exception(f"Failed to start NATS test server after {max_attempts} attempts: {e}")
    
    async def stop(self):
        """Stop NATS server"""
        if self.process:
            try:
                # Send SIGTERM to the process group
                os.killpg(os.getpgid(self.process.pid), signal.SIGTERM)
                
                # Wait for graceful shutdown
                try:
                    self.process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Force kill if not responding
                    os.killpg(os.getpgid(self.process.pid), signal.SIGKILL)
                    self.process.wait()
                    
                logger.info("NATS test server stopped")
            except Exception as e:
                logger.warning(f"Error stopping NATS server: {e}")
        
        # Cleanup temp directory
        if self.temp_dir and os.path.exists(self.temp_dir):
            try:
                import shutil
                shutil.rmtree(self.temp_dir)
            except Exception as e:
                logger.warning(f"Error cleaning up temp directory: {e}")

@pytest_asyncio.fixture(scope="module")
async def nats_server():
    """Start and stop NATS server for the test module"""
    server = NATSTestServer()
    await server.start()
    yield server
    await server.stop()

@pytest_asyncio.fixture
async def js_context(nats_server):
    """Create JetStream context with test streams"""
    nc = await nats.connect(E2E_CONFIG.url)
    js = nc.jetstream()
    
    # Create processing stream
    try:
        await js.add_stream(
            name=f"{E2E_CONFIG.stream_name}_processing",
            subjects=[f"{E2E_CONFIG.subject_prefix}.process.*"]
        )
    except Exception:
        pass  # Stream might already exist
    
    # Create results stream
    try:
        await js.add_stream(
            name=f"{E2E_CONFIG.stream_name}_results",
            subjects=[f"{E2E_CONFIG.subject_prefix}.result.*"]
        )
    except Exception:
        pass  # Stream might already exist
    
    # Create object store
    try:
        await js.create_object_store(E2E_CONFIG.bucket_name)
    except Exception:
        pass  # Object store might already exist
    
    yield js
    await nc.close()

@pytest.fixture
def test_pdf():
    """Create a test PDF file"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        # Create a simple PDF with reportlab
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, "Test Document")
        c.drawString(100, 700, "This is a test PDF for end-to-end testing.")
        c.drawString(100, 650, "It contains some sample text to be processed.")
        c.save()
        
        yield Path(tmp.name)
        
        # Cleanup
        try:
            os.unlink(tmp.name)
        except:
            pass

class MockDocumentWorker:
    """Mock worker that processes documents like the real service would"""
    
    def __init__(self, js_context: JetStreamContext):
        self.js = js_context
        self.running = False
        self.task = None
        
    async def start(self):
        """Start the mock worker"""
        self.running = True
        
        # Subscribe to processing requests
        sub = await self.js.pull_subscribe(
            subject=f"{E2E_CONFIG.subject_prefix}.process.*",
            durable="test_worker",
            stream=f"{E2E_CONFIG.stream_name}_processing"
        )
        
        self.task = asyncio.create_task(self._process_messages(sub))
        
    async def stop(self):
        """Stop the mock worker"""
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
        
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
            document_data = await obj_store.get(object_name)
            
            # Simulate document processing with docling-parse
            # In reality, this would use the actual docling-parse library
            result = {
                "request_id": request_id,
                "status": "success",
                "result": {
                    "text": "This is processed document content from the test PDF.",
                    "metadata": {
                        "title": "Test Document", 
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
                f"{E2E_CONFIG.subject_prefix}.result.{request_id}",
                json.dumps(result).encode()
            )
            
            logger.info(f"Processed document {object_name} for request {request_id}")
            
        except Exception as e:
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
                    f"{E2E_CONFIG.subject_prefix}.result.{request_id}",
                    json.dumps(error_result).encode()
                )
            except:
                pass
            
            logger.error(f"Error processing document: {e}")

@pytest_asyncio.fixture
async def mock_worker(js_context):
    """Create and start a mock document worker"""
    worker = MockDocumentWorker(js_context)
    await worker.start()
    
    # Give worker time to subscribe
    await asyncio.sleep(0.1)
    
    yield worker
    await worker.stop()

@pytest_asyncio.fixture
async def e2e_client(nats_server):
    """Create a DocumentClient configured for e2e testing"""
    
    class TestDocumentClient(DocumentClient):
        def __init__(self):
            super().__init__()
            self.config = E2E_CONFIG
    
    client = TestDocumentClient()
    await client.setup()
    yield client
    await client.nc.close()

# E2E Tests
pytestmark = pytest.mark.asyncio

@pytest.mark.e2e
async def test_nats_server_connection(nats_server):
    """Test that we can connect to the NATS test server"""
    nc = await nats.connect(E2E_CONFIG.url)
    assert nc.is_connected
    await nc.close()

@pytest.mark.e2e 
async def test_jetstream_setup(js_context):
    """Test JetStream streams and object store setup"""
    # Verify streams exist
    streams = await js_context.streams_info()
    stream_names = [stream.config.name for stream in streams]
    
    assert f"{E2E_CONFIG.stream_name}_processing" in stream_names
    assert f"{E2E_CONFIG.stream_name}_results" in stream_names
    
    # Verify object store exists by trying to access it
    try:
        obj_store = await js_context.object_store(E2E_CONFIG.bucket_name)
        assert obj_store is not None
    except Exception as e:
        pytest.fail(f"Object store {E2E_CONFIG.bucket_name} not accessible: {e}")

@pytest.mark.e2e
async def test_document_processing_workflow(e2e_client, mock_worker, test_pdf):
    """Test the complete document processing workflow end-to-end"""
    
    # Process the test PDF
    result = await e2e_client.process_document(test_pdf, timeout=10)
    
    # Verify result structure
    assert result["status"] == "success"
    assert "result" in result
    
    processed_result = result["result"]
    assert "text" in processed_result
    assert "metadata" in processed_result
    assert processed_result["metadata"]["pages"] == 1
    assert processed_result["metadata"]["format"] == "pdf"
    
    # Verify text content was extracted
    assert len(processed_result["text"]) > 0
    assert "processed document content" in processed_result["text"].lower()

@pytest.mark.e2e
async def test_document_processing_with_bytes(e2e_client, mock_worker, test_pdf):
    """Test document processing with bytes input"""
    
    # Read PDF as bytes
    pdf_bytes = test_pdf.read_bytes()
    
    # Process the bytes
    result = await e2e_client.process_document(pdf_bytes, timeout=10)
    
    # Verify successful processing
    assert result["status"] == "success"
    assert result["result"]["metadata"]["size"] == len(pdf_bytes)

@pytest.mark.e2e 
async def test_document_processing_timeout(e2e_client, js_context, test_pdf):
    """Test document processing timeout when no worker is available"""
    
    # Don't start the mock worker - this should timeout
    with pytest.raises(TimeoutError, match="Document processing timed out"):
        await e2e_client.process_document(test_pdf, timeout=2)

@pytest.mark.e2e
async def test_object_store_cleanup_on_error(e2e_client, js_context, test_pdf):
    """Test that objects are cleaned up when processing fails"""
    
    # Start a worker that always returns errors
    class ErrorWorker(MockDocumentWorker):
        async def _handle_message(self, msg):
            data = json.loads(msg.data.decode())
            request_id = data['request_id']
            
            error_result = {
                "request_id": request_id,
                "status": "error", 
                "error": "Simulated processing error"
            }
            
            await self.js.publish(
                f"{E2E_CONFIG.subject_prefix}.result.{request_id}",
                json.dumps(error_result).encode()
            )
    
    error_worker = ErrorWorker(js_context)
    await error_worker.start()
    
    try:
        # This should fail and clean up the object
        with pytest.raises(Exception, match="Simulated processing error"):
            await e2e_client.process_document(test_pdf, timeout=5)
        
        # Verify object store is clean (no test objects remaining)
        obj_store = await js_context.object_store(E2E_CONFIG.bucket_name)
        objects = []
        async for obj_info in obj_store.list():
            if obj_info.name.startswith("doc_"):
                objects.append(obj_info.name)
        
        # Should be no leftover test objects
        assert len(objects) == 0
        
    finally:
        await error_worker.stop()

@pytest.mark.e2e
async def test_concurrent_document_processing(e2e_client, mock_worker, test_pdf):
    """Test processing multiple documents concurrently"""
    
    # Create multiple processing tasks
    tasks = []
    for i in range(3):
        task = asyncio.create_task(
            e2e_client.process_document(test_pdf, timeout=15)
        )
        tasks.append(task)
    
    # Wait for all to complete
    results = await asyncio.gather(*tasks)
    
    # Verify all succeeded
    for result in results:
        assert result["status"] == "success"
        assert "result" in result
        assert len(result["result"]["text"]) > 0 