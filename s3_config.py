"""
S3 + NATS Document Processing Configuration

Clean configuration management with pydantic validation and boto3 best practices.
"""
from pydantic import BaseModel, Field, field_validator
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

class S3Config(BaseModel):
    """S3 configuration with validation and boto3 best practices"""
    
    # S3 Connection
    endpoint_url: Optional[str] = Field(default_factory=lambda: os.getenv("S3_ENDPOINT_URL"))
    region_name: str = Field(default_factory=lambda: os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
    bucket_name: str = Field(default_factory=lambda: os.getenv("S3_BUCKET", "documents"))
    
    # Authentication (boto3 will use standard AWS credential chain)
    aws_access_key_id: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_ACCESS_KEY_ID"))
    aws_secret_access_key: Optional[str] = Field(default_factory=lambda: os.getenv("AWS_SECRET_ACCESS_KEY"))
    
    # Transfer settings
    multipart_threshold: int = Field(default=100 * 1024 * 1024)  # 100MB
    max_concurrency: int = Field(default=10)
    multipart_chunksize: int = Field(default=8 * 1024 * 1024)  # 8MB
    
    # URL settings
    presigned_url_expiry: int = Field(default=3600)  # 1 hour
    
    @field_validator('bucket_name')
    @classmethod
    def validate_bucket_name(cls, v):
        """Validate S3 bucket name follows AWS conventions"""
        if not v:
            raise ValueError("Bucket name is required")
        if len(v) < 3 or len(v) > 63:
            raise ValueError("Bucket name must be between 3 and 63 characters")
        return v.lower()
    
    def boto3_config(self) -> dict:
        """Return boto3 session configuration"""
        config = {
            "region_name": self.region_name,
        }
        
        if self.endpoint_url:
            config["endpoint_url"] = self.endpoint_url
            
        if self.aws_access_key_id and self.aws_secret_access_key:
            config["aws_access_key_id"] = self.aws_access_key_id
            config["aws_secret_access_key"] = self.aws_secret_access_key
            
        return config

class ProcessingConfig(BaseModel):
    """Document processing configuration"""
    
    timeout: int = Field(default=600)  # 10 minutes
    use_s3_storage: bool = Field(default_factory=lambda: os.getenv("USE_S3_STORAGE", "false").lower() == "true")
    cleanup_on_error: bool = Field(default=True)
    enable_debug_logging: bool = Field(default_factory=lambda: os.getenv("DEBUG", "false").lower() == "true")

# Global configuration instances
s3_config = S3Config()
processing_config = ProcessingConfig() 