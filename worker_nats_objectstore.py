import asyncio
import json
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext
from config import config
import logging
import uuid

# Conditional import for docling to support testing
try:
    import docling
except ImportError:
    # Use mock during testing
    docling = None

logger = logging.getLogger(__name__)

class DocumentProcessor:
    def __init__(self):
        self.nc = NATS()
        self.js: JetStreamContext = None
        
        # Initialize docling client
        if docling:
            self.docling_client = docling.Client()
        else:
            # This will be replaced by the mock in tests
            self.docling_client = None
        
    async def setup(self):
        # Connect to NATS
        await self.nc.connect(config.url)
        self.js = self.nc.jetstream()
        
        # Create or get the streams
        try:
            # Stream for incoming documents
            await self.js.add_stream(
                name=config.stream_name,
                subjects=[f"{config.subject_prefix}.process.*"],
                storage="memory",
            )
            
            # Stream for processing results
            await self.js.add_stream(
                name=f"{config.stream_name}_results",
                subjects=[f"{config.subject_prefix}.result.*"],
                storage="memory",
                max_age=3600,  # Keep results for 1 hour
            )
        except Exception as e:
            logger.error(f"Error setting up streams: {e}")
            raise
            
    async def process_document(self, msg):
        """Handle incoming document processing request"""
        try:
            # Parse incoming message
            data = json.loads(msg.data.decode())
            request_id = data.get("request_id")
            object_name = data.get("object_name")
            bucket = data.get("bucket")
            
            if not all([request_id, object_name, bucket]):
                logger.error("Invalid message format - missing required fields")
                await self.publish_error(request_id, "Missing required fields")
                return
                
            # Get document from object store
            obj = await self.js.object_store(bucket)
            document_data = await obj.get(object_name)
            
            if not document_data:
                await self.publish_error(request_id, f"Document not found: {object_name}")
                return
            
            try:
                # Process document with Docling
                # Read document data into bytes
                document_bytes = b""
                async for chunk in document_data:
                    document_bytes += chunk
                
                result = process_with_docling(document_bytes, object_name)
                
                # Publish success result
                await self.js.publish(
                    f"{config.subject_prefix}.result.{request_id}",
                    json.dumps({
                        "request_id": request_id,
                        "status": "success",
                        "result": result
                    }).encode()
                )
                
            finally:
                # Clean up object store
                try:
                    await obj.delete(object_name)
                except Exception as e:
                    logger.warning(f"Failed to clean up object {object_name}: {e}")
            
            # Acknowledge message processing
            await msg.ack()
            
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            if 'request_id' in locals():
                await self.publish_error(request_id, str(e))
            
            # Clean up object store on error
            if 'obj' in locals() and 'object_name' in locals():
                try:
                    await obj.delete(object_name)
                except:
                    pass
            
            # Negative acknowledge to retry
            await msg.nak()
            
    async def publish_error(self, request_id: str, error_message: str):
        """Publish error result"""
        try:
            await self.js.publish(
                f"{config.subject_prefix}.result.{request_id}",
                json.dumps({
                    "request_id": request_id,
                    "status": "error",
                    "error": error_message
                }).encode()
            )
        except Exception as e:
            logger.error(f"Failed to publish error: {e}")
            
    async def run_docling(self, document_data: bytes):
        """Process document with local Docling instance"""
        try:
            if not self.docling_client:
                raise Exception("Docling client not available")
                
            result = await self.docling_client.process_document(
                document_data,
                options={
                    "extract_text": True,
                    "extract_metadata": True,
                    "extract_images": True
                }
            )
            return result
            
        except Exception as e:
            logger.error(f"Error processing document with Docling: {e}")
            raise
            
    async def start(self):
        """Start the worker with pull-based consumer"""
        try:
            # Create pull consumer
            consumer = await self.js.pull_subscribe(
                subject=f"{config.subject_prefix}.process.*",
                durable="docling_worker",
                stream=config.stream_name,
            )
            
            logger.info(f"Worker started - pulling from {config.stream_name}")
            
            while True:
                try:
                    # Fetch batch of messages
                    messages = await consumer.fetch(batch=1, timeout=30)
                    for msg in messages:
                        await self.process_document(msg)
                except TimeoutError:
                    # No messages available
                    continue
                except Exception as e:
                    logger.error(f"Error in message processing loop: {e}")
                    await asyncio.sleep(1)  # Prevent tight loop on error
                    
        except Exception as e:
            logger.error(f"Fatal error in worker: {e}")
            raise

def process_with_docling(content: bytes, filename: str) -> dict:
    """Process document content with Docling (or mock for testing)."""
    if docling is None:
        # Return mock result for testing
        return {
            "content": f"Mock processed content for {filename}",
            "metadata": {
                "title": "Mock Document",
                "pages": 1,
                "format": "pdf"
            },
            "tables": [],
            "figures": []
        }
    
    # Real docling processing would go here
    # This is a placeholder for the actual implementation
    return {
        "content": f"Processed content for {filename}",
        "metadata": {
            "title": filename,
            "pages": 1,
            "format": "pdf"
        },
        "tables": [],
        "figures": []
    }
            
async def main():
    processor = DocumentProcessor()
    await processor.setup()
    await processor.start()

if __name__ == "__main__":
    asyncio.run(main()) 