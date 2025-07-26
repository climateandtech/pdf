from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

class NatsConfig(BaseModel):
    url: str = os.getenv("NATS_URL", "nats://localhost:4222")
    stream_name: str = "DOCUMENTS"
    subject_prefix: str = "docs"
    
    # JetStream settings
    max_payload_size: int = 8 * 1024 * 1024  # 8MB default
    max_stream_size: int = 1024 * 1024 * 1024  # 1GB default
    replicas: int = 1
    retention_policy: str = "workqueue"  # Use workqueue for processing tasks
    
    # Object Store settings
    bucket_name: str = "documents"
    max_object_size: int = 10 * 1024 * 1024 * 1024  # 10GB max object size
    
    # Subjects
    document_subject: str = f"{subject_prefix}.process"
    result_subject: str = f"{subject_prefix}.result"

config = NatsConfig() 