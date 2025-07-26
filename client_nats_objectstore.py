import asyncio
import json
import uuid
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext
from config import config
import logging
from pathlib import Path
import os

logger = logging.getLogger(__name__)

class DocumentClient:
    def __init__(self):
        self.nc = NATS()
        self.js: JetStreamContext = None
        self.config = config
        
    async def setup(self):
        await self.nc.connect(config.url)
        self.js = self.nc.jetstream()
        
        # Ensure object store exists
        try:
            await self.js.create_object_store(config.bucket_name)
        except:
            pass
    
    async def process_document(self, source, *, timeout: int = 600) -> dict:
        """
        Process a document using the NATS+JetStream worker
        
        Args:
            source: File path or bytes content
            timeout: Timeout in seconds for processing (default: 10 minutes)
            
        Returns:
            dict: Processing results
            
        Raises:
            Exception: If processing fails or times out
        """
        try:
            # Generate unique IDs
            request_id = str(uuid.uuid4())
            object_name = f"doc_{request_id}"
            
            # Get document content
            if isinstance(source, (str, Path)) and os.path.exists(source):
                with open(source, 'rb') as f:
                    document_data = f.read()
            else:
                document_data = source if isinstance(source, bytes) else source.encode()
            
            # Store document in Object Store
            logger.info(f"Storing document in Object Store: {object_name}")
            obj = await self.js.object_store(config.bucket_name)
            await obj.put(object_name, document_data)
            
            try:
                # Create consumer for results
                results_consumer = await self.js.pull_subscribe(
                    subject=f"{config.subject_prefix}.result.{request_id}",
                    durable=f"results_{request_id}",  # Unique durable name
                    stream=f"{config.stream_name}_results",
                )
                
                try:
                    # Send processing request
                    await self.js.publish(
                        f"{config.subject_prefix}.process.{request_id}",
                        json.dumps({
                            "request_id": request_id,
                            "object_name": object_name,
                            "bucket": config.bucket_name
                        }).encode()
                    )
                    
                    # Wait for result with timeout
                    messages = await results_consumer.fetch(batch=1, timeout=timeout)
                    if not messages:
                        raise TimeoutError("Document processing timed out")
                        
                    result = json.loads(messages[0].data.decode())
                    await messages[0].ack()
                    
                    # Handle error result
                    if result.get("status") == "error":
                        raise Exception(result.get("error", "Unknown processing error"))
                    
                    return result
                    
                finally:
                    # Clean up consumer
                    try:
                        await self.js.delete_consumer(
                            stream=f"{config.stream_name}_results",
                            consumer=f"results_{request_id}"
                        )
                    except:
                        pass
                    
            except Exception as e:
                # Clean up document on error
                try:
                    await obj.delete(object_name)
                except:
                    pass
                raise
            
        except Exception as e:
            logger.error(f"Error processing document: {e}")
            raise

async def main():
    # Example usage
    client = DocumentClient()
    await client.setup()
    
    try:
        # Process a document
        with open("example.pdf", "rb") as f:
            result = await client.process_document(f.read())
            print("Document processed:", result)
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await client.nc.close()

if __name__ == "__main__":
    asyncio.run(main()) 