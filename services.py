import asyncio
import json
import uuid
from nats.aio.client import Client as NATS
from nats.js.client import JetStreamContext
from nats.js import api
import httpx
from typing import Optional, Dict, Any
import logging
from datetime import datetime
from config import config

# Import S3 + NATS distributed architecture components
from s3_client import S3DocumentClient
from s3_config import S3Config

logger = logging.getLogger(__name__)

class DocumentService:
    """
    Distributed Document Processing Service using S3 + NATS
    
    This service:
    1. Uploads files to S3
    2. Sends processing requests via NATS to docling workers
    3. Receives results back via NATS
    
    Replaces NATS Object Store with S3 + NATS messaging pattern.
    """
    
    def __init__(self):
        # S3 + NATS client for distributed architecture
        self.s3_config = S3Config()
        self.client = S3DocumentClient(self.s3_config, config)
        
    async def setup(self):
        """Initialize S3 and NATS connections"""
        await self.client.setup()
        logger.info("DocumentService connected to S3 + NATS distributed architecture")
        
    async def store_document(self, content: bytes, resource_id: str, metadata: Optional[Dict] = None) -> str:
        """Upload document to S3"""
        try:
            # Upload to S3
            s3_key = f"documents/{resource_id}.pdf"
            s3_url = await self.client._upload_to_s3_bytes(content, s3_key)
            
            logger.info(f"Document uploaded to S3: {s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error storing document to S3: {e}")
            raise
            
    async def get_document(self, s3_key: str) -> Optional[bytes]:
        """Download document from S3"""
        try:
            return await self.client.download_result(s3_key)
        except Exception as e:
            logger.error(f"Error retrieving document from S3: {e}")
            return None
            
    async def get_document_info(self, s3_key: str) -> Optional[Dict]:
        """Get document metadata (would be stored separately if needed)"""
        # In S3 + NATS architecture, metadata could be stored in a database
        # or as S3 object metadata. For now, return basic info.
        try:
            # Check if file exists in S3
            content = await self.get_document(s3_key)
            if content:
                return {
                    "s3_key": s3_key,
                    "size": len(content),
                    "exists": True
                }
            return None
        except Exception as e:
            logger.error(f"Error getting document info from S3: {e}")
            return None
            
    async def process_document(self, s3_key: str, metadata: Optional[Dict] = None, docling_options: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Send document for processing by Docling worker via NATS
        
        This replaces the HTTP API with NATS messaging to the docling worker.
        
        Args:
            s3_key: S3 location of the document
            metadata: Optional metadata to include
            docling_options: Complete Docling configuration (format_options, accelerator_options, etc.)
        """
        try:
            request_id = str(uuid.uuid4())

            # Create processing request message
            message = {
                "request_id": request_id,
                "s3_key": s3_key,
                "metadata": metadata,
                "docling_options": docling_options,  # Generic Docling configuration from publisher
                "timestamp": datetime.utcnow().isoformat()
            }
            
            logger.info(f"Sending processing request {request_id} for {s3_key} to docling worker")
            
            # Ensure results stream exists, create if needed
            results_stream_name = f"{config.stream_name}_results"
            try:
                await self.client.js.stream_info(results_stream_name)
            except Exception as e:
                if "not found" in str(e):
                    logger.info(f"Creating results stream: {results_stream_name}")
                    await self.client.js.add_stream(
                        name=results_stream_name,
                        subjects=[f"{config.subject_prefix}.result.*"],
                        storage="memory",
                        retention="limits",
                        max_msgs=1000,
                        max_bytes=100 * 1024 * 1024,  # 100MB
                        max_age=3600  # Keep results for 1 hour
                    )
                else:
                    raise
            
            # Set up response consumer before sending request
            results_consumer = await self.client.js.pull_subscribe(
                subject=f"{config.subject_prefix}.result.{request_id}",
                durable=f"result_{request_id}",
                stream=results_stream_name
            )
            
            try:
                # Send processing request to docling worker
                await self.client.js.publish(
                    f"{config.subject_prefix}.process.{request_id}",
                    json.dumps(message).encode()
                )
                
                logger.info(f"Sent processing request to docling worker: {request_id}")
                
                # Wait for response from docling worker
                logger.info(f"Waiting for response from docling worker...")
                messages = await results_consumer.fetch(batch=1, timeout=600)  # 10 minute timeout
                
                if not messages:
                    raise TimeoutError(f"Docling processing timed out after 600 seconds")
                
                # Parse response
                result = json.loads(messages[0].data.decode())
                await messages[0].ack()
                
                if result.get("status") == "error":
                    raise Exception(result.get("error", "Unknown docling processing error"))
                
                logger.info(f"Received successful response from docling worker: {request_id}")
                return result
                
            finally:
                # Cleanup consumer
                try:
                    await self.client.js.delete_consumer(
                        stream=f"{config.stream_name}_results",
                        consumer=f"result_{request_id}"
                    )
                except Exception as e:
                    logger.warning(f"Consumer cleanup failed: {e}")
                    
        except Exception as e:
            logger.error(f"Error processing document via docling worker: {e}")
            raise
            
    async def process_url(self, url: str, resource_id: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Download PDF from URL, upload to S3, and process with docling worker"""
        try:
            # Download document
            logger.info(f"Downloading document from URL: {url}")
            async with httpx.AsyncClient() as client:
                response = await client.get(url)
                response.raise_for_status()
                content = response.content
                
            # Upload to S3
            s3_key = await self.store_document(content, resource_id, metadata)
            
            # Process document via docling worker
            return await self.process_document(s3_key, metadata)
            
        except Exception as e:
            logger.error(f"Error processing URL: {e}")
            raise
            
    async def delete_document(self, s3_key: str):
        """Delete document from S3"""
        try:
            # Use S3 client to delete
            async with self.client.s3_client() as s3:
                await s3.delete_object(
                    Bucket=self.s3_config.bucket_name,
                    Key=s3_key
                )
            logger.info(f"Deleted document from S3: {s3_key}")
        except Exception as e:
            logger.error(f"Error deleting document from S3: {e}")
            raise

    async def close(self):
        """Close connections"""
        await self.client.close()

# Helper method for S3DocumentClient to upload bytes directly
async def _upload_to_s3_bytes(self, content: bytes, s3_key: str) -> str:
    """Upload bytes content to S3"""
    async with self.s3_client() as s3:
        await s3.put_object(
            Bucket=self.s3_config.bucket_name,
            Key=s3_key,
            Body=content
        )
        
        # Generate presigned URL
        return await s3.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.s3_config.bucket_name, 'Key': s3_key},
            ExpiresIn=self.s3_config.presigned_url_expiry
        )

# Add the method to S3DocumentClient
S3DocumentClient._upload_to_s3_bytes = _upload_to_s3_bytes

# Synchronous wrapper functions for compatibility with existing code
def process_document_sync(s3_key: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """Synchronous wrapper for document processing"""
    return asyncio.run(_process_document_sync(s3_key, metadata))

async def _process_document_sync(s3_key: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    service = DocumentService()
    await service.setup()
    try:
        return await service.process_document(s3_key, metadata)
    finally:
        await service.close()

def process_url_sync(url: str, resource_id: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    """Synchronous wrapper for URL processing"""
    return asyncio.run(_process_url_sync(url, resource_id, metadata))

async def _process_url_sync(url: str, resource_id: str, metadata: Optional[Dict] = None) -> Dict[str, Any]:
    service = DocumentService()
    await service.setup()
    try:
        return await service.process_url(url, resource_id, metadata)
    finally:
        await service.close() 