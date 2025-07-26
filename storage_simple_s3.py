import boto3
import os
from botocore.client import Config
from config import config
import logging
import uuid
from typing import Optional, BinaryIO, Union

logger = logging.getLogger(__name__)

class StorageService:
    """Service for handling document storage using S3-compatible storage"""
    
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            endpoint_url=config.s3_endpoint,
            aws_access_key_id=config.s3_access_key,
            aws_secret_access_key=config.s3_secret_key,
            region_name=config.s3_region,
            config=Config(signature_version='s3v4')
        )
        self.bucket = config.s3_bucket
        
        # Ensure bucket exists
        self._ensure_bucket()
    
    def _ensure_bucket(self):
        """Ensure the configured bucket exists"""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
        except:
            try:
                self.s3_client.create_bucket(Bucket=self.bucket)
                logger.info(f"Created bucket: {self.bucket}")
            except Exception as e:
                logger.error(f"Error creating bucket: {e}")
                raise
    
    def store_document(self, 
                      content: Union[bytes, BinaryIO], 
                      resource_id: str,
                      content_type: str = "application/pdf") -> str:
        """
        Store a document in S3 storage
        
        Args:
            content: Document content as bytes or file-like object
            resource_id: Unique identifier for the resource
            content_type: MIME type of the document
            
        Returns:
            str: S3 key of stored document
        """
        try:
            # Generate S3 key
            key = f"documents/{resource_id}"
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=content,
                ContentType=content_type
            )
            
            logger.info(f"Stored document {resource_id} at {key}")
            return key
            
        except Exception as e:
            logger.error(f"Error storing document: {e}")
            raise
    
    def get_document(self, resource_id: str) -> Optional[bytes]:
        """
        Retrieve a document from S3 storage
        
        Args:
            resource_id: Unique identifier for the resource
            
        Returns:
            bytes: Document content if found, None otherwise
        """
        try:
            key = f"documents/{resource_id}"
            response = self.s3_client.get_object(
                Bucket=self.bucket,
                Key=key
            )
            return response['Body'].read()
            
        except self.s3_client.exceptions.NoSuchKey:
            logger.warning(f"Document not found: {resource_id}")
            return None
            
        except Exception as e:
            logger.error(f"Error retrieving document: {e}")
            raise
    
    def get_document_url(self, resource_id: str, expires_in: int = 3600) -> str:
        """
        Generate a pre-signed URL for document access
        
        Args:
            resource_id: Unique identifier for the resource
            expires_in: URL expiration time in seconds
            
        Returns:
            str: Pre-signed URL for document access
        """
        try:
            key = f"documents/{resource_id}"
            url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket,
                    'Key': key
                },
                ExpiresIn=expires_in
            )
            return url
            
        except Exception as e:
            logger.error(f"Error generating pre-signed URL: {e}")
            raise
    
    def delete_document(self, resource_id: str):
        """
        Delete a document from S3 storage
        
        Args:
            resource_id: Unique identifier for the resource
        """
        try:
            key = f"documents/{resource_id}"
            self.s3_client.delete_object(
                Bucket=self.bucket,
                Key=key
            )
            logger.info(f"Deleted document: {resource_id}")
            
        except Exception as e:
            logger.error(f"Error deleting document: {e}")
            raise 