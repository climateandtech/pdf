"""
S3 + NATS Document Processing Client

Clean implementation using aioboto3 and boto3 best practices:
- Context managers for resource management
- Proper async support with aioboto3  
- Boto3 transfer management for large files
- Minimal boilerplate with focused functionality
"""
import asyncio
import json
import uuid
from pathlib import Path
from typing import Union, Dict, Any, AsyncContextManager
import logging
from datetime import datetime
from contextlib import asynccontextmanager

import aioboto3
from botocore.exceptions import ClientError
import nats
from nats.js.api import StreamConfig

from config import NatsConfig
from s3_config import S3Config, ProcessingConfig

logger = logging.getLogger(__name__)

class S3DocumentClient:
    """
    Document processing client using S3 + NATS control bus pattern
    
    Uses aioboto3 for proper async support and boto3 best practices
    """
    
    def __init__(
        self, 
        s3_config: S3Config = None, 
        nats_config: NatsConfig = None,
        processing_config: ProcessingConfig = None
    ):
        self.s3_config = s3_config or S3Config()
        self.nats_config = nats_config or NatsConfig()
        self.processing_config = processing_config or ProcessingConfig()
        
        # aioboto3 session for async operations
        self.session = aioboto3.Session()
        self.nc = None
        self.js = None

    @asynccontextmanager
    async def s3_client(self) -> AsyncContextManager:
        """Async context manager for S3 client with proper resource cleanup"""
        async with self.session.client('s3', **self.s3_config.boto3_config()) as client:
            yield client

    async def setup(self):
        """Initialize NATS connection and ensure S3 bucket exists"""
        # Connect to NATS
        self.nc = await nats.connect(self.nats_config.connection_url)
        self.js = self.nc.jetstream()
        
        # Ensure bucket exists
        await self._ensure_bucket_exists()
        
        logger.info("S3 + NATS client initialized")

    async def _ensure_bucket_exists(self):
        """Create S3 bucket if it doesn't exist using boto3 best practices"""
        async with self.s3_client() as s3:
            try:
                await s3.head_bucket(Bucket=self.s3_config.bucket_name)
                logger.debug(f"Bucket {self.s3_config.bucket_name} exists")
            except ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == '404':
                    # Bucket doesn't exist, create it
                    try:
                        if self.s3_config.region_name == 'us-east-1':
                            # us-east-1 doesn't need LocationConstraint
                            await s3.create_bucket(Bucket=self.s3_config.bucket_name)
                        else:
                            await s3.create_bucket(
                                Bucket=self.s3_config.bucket_name,
                                CreateBucketConfiguration={'LocationConstraint': self.s3_config.region_name}
                            )
                        logger.info(f"Created S3 bucket: {self.s3_config.bucket_name}")
                    except ClientError as create_error:
                        logger.error(f"Failed to create bucket: {create_error}")
                        raise
                else:
                    logger.error(f"Error accessing bucket: {e}")
                    raise

    async def process_document(
        self, 
        source: Union[str, Path, bytes], 
        *, 
        timeout: int = None
    ) -> Dict[str, Any]:
        """
        Process document using S3 + NATS control bus pattern
        
        Uses boto3 best practices for efficient S3 operations
        """
        request_id = str(uuid.uuid4())
        timeout = timeout or self.processing_config.timeout
        s3_key = f"raw/{request_id}.pdf"
        
        try:
            # Upload to S3 with automatic multipart handling
            s3_url = await self._upload_to_s3(source, s3_key)
            logger.info(f"Uploaded to S3: {s3_key}")
            
            # Set up NATS consumer for results
            results_consumer = await self.js.pull_subscribe(
                subject=f"{self.nats_config.subject_prefix}.result.{request_id}",
                durable=f"result_{request_id}",
                stream=f"{self.nats_config.stream_name}_results"
            )
            
            try:
                # Publish lightweight control message
                await self._publish_control_message(request_id, s3_key, s3_url, source)
                
                # Wait for processing result
                result = await self._wait_for_result(results_consumer, timeout)
                
                logger.info(f"Processing completed: {request_id}")
                return result
                
            finally:
                # Cleanup consumer
                await self._cleanup_consumer(results_consumer, request_id)
                
        except Exception as e:
            logger.error(f"Processing failed: {e}")
            if self.processing_config.cleanup_on_error:
                await self._cleanup_s3_object(s3_key)
            raise

    async def _upload_to_s3(self, source: Union[str, Path, bytes], s3_key: str) -> str:
        """Upload to S3 using boto3 transfer management for efficiency"""
        async with self.s3_client() as s3:
            # Handle different source types
            if isinstance(source, (str, Path)):
                # File upload - boto3 handles multipart automatically
                await s3.upload_file(
                    str(source), 
                    self.s3_config.bucket_name, 
                    s3_key,
                    Config=self._get_transfer_config()
                )
            else:
                # Bytes upload
                if isinstance(source, str):
                    source = source.encode()
                
                await s3.put_object(
                    Bucket=self.s3_config.bucket_name,
                    Key=s3_key,
                    Body=source
                )
            
            # Generate presigned URL
            return await s3.generate_presigned_url(
                'get_object',
                Params={'Bucket': self.s3_config.bucket_name, 'Key': s3_key},
                ExpiresIn=self.s3_config.presigned_url_expiry
            )

    def _get_transfer_config(self):
        """Get optimized transfer configuration for boto3"""
        from boto3.s3.transfer import TransferConfig
        
        return TransferConfig(
            multipart_threshold=self.s3_config.multipart_threshold,
            max_concurrency=self.s3_config.max_concurrency,
            multipart_chunksize=self.s3_config.multipart_chunksize,
            use_threads=True
        )

    async def _publish_control_message(
        self, 
        request_id: str, 
        s3_key: str, 
        s3_url: str, 
        source: Union[str, Path, bytes]
    ):
        """Publish lightweight control message to NATS"""
        control_message = {
            "request_id": request_id,
            "s3_bucket": self.s3_config.bucket_name,
            "s3_key": s3_key,
            "s3_url": s3_url,
            "timestamp": datetime.utcnow().isoformat(),
            "file_size": self._get_file_size(source),
            "processing_timeout": self.processing_config.timeout
        }
        
        await self.js.publish(
            f"{self.nats_config.subject_prefix}.process.{request_id}",
            json.dumps(control_message).encode()
        )
        
        logger.debug(f"Published control message: {request_id}")

    async def _wait_for_result(self, consumer, timeout: int) -> Dict[str, Any]:
        """Wait for processing result with proper error handling"""
        messages = await consumer.fetch(batch=1, timeout=timeout)
        
        if not messages:
            raise TimeoutError(f"Processing timed out after {timeout} seconds")
        
        result = json.loads(messages[0].data.decode())
        await messages[0].ack()
        
        if result.get("status") == "error":
            raise Exception(result.get("error", "Unknown processing error"))
        
        return result

    async def _cleanup_consumer(self, consumer, request_id: str):
        """Clean up NATS consumer"""
        try:
            await self.js.delete_consumer(
                stream=f"{self.nats_config.stream_name}_results",
                consumer=f"result_{request_id}"
            )
        except Exception as e:
            logger.warning(f"Consumer cleanup failed: {e}")

    async def _cleanup_s3_object(self, s3_key: str):
        """Clean up S3 object on error"""
        try:
            async with self.s3_client() as s3:
                await s3.delete_object(
                    Bucket=self.s3_config.bucket_name,
                    Key=s3_key
                )
            logger.debug(f"Cleaned up S3 object: {s3_key}")
        except Exception as e:
            logger.warning(f"S3 cleanup failed: {e}")

    def _get_file_size(self, source: Union[str, Path, bytes]) -> int:
        """Get file size efficiently"""
        if isinstance(source, (str, Path)):
            return Path(source).stat().st_size
        elif isinstance(source, bytes):
            return len(source)
        elif isinstance(source, str):
            return len(source.encode())
        return 0

    async def download_result(self, s3_key: str, local_path: Path = None) -> Union[bytes, Path]:
        """Download processed result from S3"""
        async with self.s3_client() as s3:
            if local_path:
                await s3.download_file(
                    self.s3_config.bucket_name,
                    s3_key,
                    str(local_path)
                )
                return local_path
            else:
                response = await s3.get_object(
                    Bucket=self.s3_config.bucket_name,
                    Key=s3_key
                )
                return await response['Body'].read()

    async def close(self):
        """Clean shutdown"""
        if self.nc:
            await self.nc.close()
        logger.info("S3DocumentClient closed")


# Factory function for easy integration
async def create_s3_document_client(**kwargs) -> S3DocumentClient:
    """Factory function with automatic setup"""
    client = S3DocumentClient(**kwargs)
    await client.setup()
    return client 