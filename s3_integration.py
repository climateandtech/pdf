"""
S3 + NATS Integration with Existing Services

Clean integration layer that works alongside existing services.py
Uses factory pattern and async context managers for clean resource management.
"""
import asyncio
from typing import Dict, Any, Union, Optional
from pathlib import Path
import logging

from s3_client import S3DocumentClient, create_s3_document_client
from s3_config import S3Config, ProcessingConfig, s3_config, processing_config
from config import NatsConfig

logger = logging.getLogger(__name__)

class S3DocumentService:
    """
    Service layer integrating S3 + NATS with existing DoclingClient from services.py
    
    Clean integration with minimal boilerplate
    """
    
    def __init__(self, config_override: Dict = None):
        # Use global configs with optional overrides
        self.config = {**s3_config.dict(), **(config_override or {})}
        self.client: Optional[S3DocumentClient] = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.client = await create_s3_document_client(
            s3_config=S3Config(**self.config)
        )
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit with cleanup"""
        if self.client:
            await self.client.close()
    
    async def process_document(
        self, 
        source: Union[str, Path, bytes],
        timeout: int = None
    ) -> Dict[str, Any]:
        """Process document using S3 + NATS architecture"""
        if not self.client:
            raise RuntimeError("Service not initialized. Use async context manager.")
        
        return await self.client.process_document(source, timeout=timeout)

class S3DoclingIntegration:
    """
    Integration layer that combines S3 storage with existing DoclingClient processing
    
    This simulates the worker component that would run separately in production
    """
    
    def __init__(self):
        # Import DoclingClient from existing services.py
        try:
            import sys
            import os
            # Add thinktank2 path to import existing services
            thinktank_path = os.path.join(os.path.dirname(__file__), '..', 'thinktank2', 'search', 'backend', 'app')
            if os.path.exists(thinktank_path):
                sys.path.insert(0, thinktank_path)
                from services import DoclingClient
                self.docling_client = DoclingClient()
                logger.info("Integrated with existing DoclingClient")
            else:
                logger.warning("Could not find existing DoclingClient, using mock")
                self.docling_client = None
        except ImportError as e:
            logger.warning(f"Could not import DoclingClient: {e}")
            self.docling_client = None
    
    async def process_from_s3_url(self, s3_url: str, custom_prompt: str = None) -> Dict[str, Any]:
        """Process PDF from S3 URL using existing DoclingClient"""
        if self.docling_client:
            # Use existing DoclingClient with streaming
            return await self.docling_client.process_pdf_streaming(s3_url, custom_prompt)
        else:
            # Mock processing for testing
            return {
                "status": "success",
                "result": {
                    "text": "Mock processed content from S3",
                    "pages": 1,
                    "processing_method": "mock_docling"
                }
            }

# Factory functions for easy usage
async def process_pdf_with_s3(
    pdf_source: Union[str, Path, bytes],
    custom_prompt: str = None,
    timeout: int = None,
    config_override: Dict = None
) -> Dict[str, Any]:
    """
    One-liner function to process PDF with S3 + NATS
    
    Example usage:
        result = await process_pdf_with_s3('document.pdf')
    """
    async with S3DocumentService(config_override) as service:
        result = await service.process_document(pdf_source, timeout=timeout)
        
        # If we have an S3 URL in the result, also run docling processing
        # (In production, this would be done by a separate worker)
        if result.get("s3_url") and custom_prompt:
            integration = S3DoclingIntegration()
            docling_result = await integration.process_from_s3_url(
                result["s3_url"], 
                custom_prompt
            )
            result["docling_processing"] = docling_result
        
        return result

def process_pdf_with_s3_sync(
    pdf_source: Union[str, Path, bytes],
    custom_prompt: str = None,
    timeout: int = None,
    config_override: Dict = None
) -> Dict[str, Any]:
    """
    Synchronous wrapper for easy integration with existing sync code
    
    Example usage:
        result = process_pdf_with_s3_sync('document.pdf')
    """
    return asyncio.run(process_pdf_with_s3(
        pdf_source, 
        custom_prompt, 
        timeout, 
        config_override
    ))

# Configuration helpers
def configure_s3_storage(
    endpoint_url: str = None,
    bucket_name: str = None,
    aws_access_key_id: str = None,
    aws_secret_access_key: str = None,
    region_name: str = None
) -> Dict[str, Any]:
    """
    Helper to create S3 configuration override
    
    Example:
        config = configure_s3_storage(
            endpoint_url="http://localhost:9000",  # MinIO
            bucket_name="my-documents"
        )
        result = await process_pdf_with_s3('doc.pdf', config_override=config)
    """
    config = {}
    if endpoint_url:
        config['endpoint_url'] = endpoint_url
    if bucket_name:
        config['bucket_name'] = bucket_name
    if aws_access_key_id:
        config['aws_access_key_id'] = aws_access_key_id
    if aws_secret_access_key:
        config['aws_secret_access_key'] = aws_secret_access_key
    if region_name:
        config['region_name'] = region_name
    
    return config

# Add to existing services.py - example integration
def add_s3_support_to_services():
    """
    Example of how to integrate S3 support into existing services.py
    
    Add this to your services.py:
    
    ```python
    from s3_integration import process_pdf_with_s3_sync, configure_s3_storage
    
    def process_pdf_with_storage_options(pdf_path: str, use_s3: bool = False, **kwargs):
        if use_s3:
            # Use S3 + NATS control bus
            s3_config = configure_s3_storage(**kwargs.get('s3_config', {}))
            return process_pdf_with_s3_sync(
                pdf_path, 
                custom_prompt=kwargs.get('custom_prompt'),
                config_override=s3_config
            )
        else:
            # Use existing NATS Object Store implementation
            return your_existing_pdf_processing_function(pdf_path, **kwargs)
    ```
    """
    pass 