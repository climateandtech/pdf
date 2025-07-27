from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from dotenv import load_dotenv
import os
from enum import Enum

load_dotenv()

class ServiceType(str, Enum):
    """Supported service types for distributed processing"""
    PDF_DOCLING = "pdf_docling"
    IMAGE_PROCESSING = "image_processing"
    TEXT_ANALYSIS = "text_analysis"
    AUDIO_TRANSCRIPTION = "audio_transcription"
    VIDEO_PROCESSING = "video_processing"
    # Add more as needed

class NatsConfig(BaseModel):
    """Generic NATS configuration supporting multiple service types"""
    url: str = os.getenv("NATS_URL", "nats://localhost:4222")
    token: Optional[str] = os.getenv("NATS_TOKEN")
    
    # Connection settings
    connect_timeout: int = int(os.getenv("NATS_CONNECT_TIMEOUT", "10"))
    max_reconnect_attempts: int = int(os.getenv("NATS_MAX_RECONNECT_ATTEMPTS", "10"))
    
    # Stream settings
    max_payload_size: int = 8 * 1024 * 1024  # 8MB default
    max_stream_size: int = 1024 * 1024 * 1024  # 1GB default
    
    @property
    def connection_url(self) -> str:
        """Get connection URL with token if provided"""
        if self.token:
            parts = self.url.replace("nats://", "").split(":")
            host = parts[0]
            port = parts[1] if len(parts) > 1 else "4222"
            return f"nats://{self.token}@{host}:{port}"
        return self.url

class ServiceConfig(BaseModel):
    """Configuration for a specific service type"""
    service_type: ServiceType
    stream_name: str
    subject_prefix: str
    worker_queue_group: str  # For load balancing workers
    
    # Service-specific settings
    max_processing_time: int = 600  # 10 minutes default
    max_retries: int = 3
    retry_delay: int = 5
    
    def get_process_subject(self, request_id: str = "*") -> str:
        """Get the subject for processing requests"""
        return f"{self.subject_prefix}.process.{request_id}"
    
    def get_result_subject(self, request_id: str = "*") -> str:
        """Get the subject for processing results"""
        return f"{self.subject_prefix}.result.{request_id}"
    
    def get_status_subject(self, request_id: str = "*") -> str:
        """Get the subject for status updates"""
        return f"{self.subject_prefix}.status.{request_id}"

class GenericDistributedConfig(BaseModel):
    """Main configuration for the generic distributed processing system"""
    nats: NatsConfig = NatsConfig()
    
    # Service configurations
    services: Dict[ServiceType, ServiceConfig] = {
        ServiceType.PDF_DOCLING: ServiceConfig(
            service_type=ServiceType.PDF_DOCLING,
            stream_name="PDF_PROCESSING",
            subject_prefix="pdf.docling",
            worker_queue_group="pdf_docling_workers"
        ),
        ServiceType.IMAGE_PROCESSING: ServiceConfig(
            service_type=ServiceType.IMAGE_PROCESSING,
            stream_name="IMAGE_PROCESSING", 
            subject_prefix="image.process",
            worker_queue_group="image_workers"
        ),
        ServiceType.TEXT_ANALYSIS: ServiceConfig(
            service_type=ServiceType.TEXT_ANALYSIS,
            stream_name="TEXT_ANALYSIS",
            subject_prefix="text.analyze", 
            worker_queue_group="text_workers"
        ),
        ServiceType.AUDIO_TRANSCRIPTION: ServiceConfig(
            service_type=ServiceType.AUDIO_TRANSCRIPTION,
            stream_name="AUDIO_TRANSCRIPTION",
            subject_prefix="audio.transcribe",
            worker_queue_group="audio_workers"
        )
    }
    
    def get_service_config(self, service_type: ServiceType) -> ServiceConfig:
        """Get configuration for a specific service type"""
        return self.services[service_type]
    
    def get_all_stream_names(self) -> List[str]:
        """Get all stream names for setup"""
        return [config.stream_name for config in self.services.values()]
    
    def get_all_subjects(self) -> List[str]:
        """Get all subjects that need to be configured"""
        subjects = []
        for config in self.services.values():
            subjects.extend([
                f"{config.subject_prefix}.process.*",
                f"{config.subject_prefix}.result.*", 
                f"{config.subject_prefix}.status.*"
            ])
        return subjects

# Global config instance
config = GenericDistributedConfig()

# Convenience functions for backward compatibility
def get_pdf_docling_config() -> ServiceConfig:
    """Get PDF Docling service configuration"""
    return config.get_service_config(ServiceType.PDF_DOCLING)

# Legacy config for existing code
class NatsConfig_Legacy(BaseModel):
    """Legacy NATS config for backward compatibility"""
    url: str = config.nats.url
    token: Optional[str] = config.nats.token
    stream_name: str = "DOCUMENTS"  # Old default
    subject_prefix: str = "docs"    # Old default
    
    @property
    def connection_url(self) -> str:
        return config.nats.connection_url

# Export both for compatibility
__all__ = [
    "ServiceType", 
    "NatsConfig", 
    "ServiceConfig", 
    "GenericDistributedConfig",
    "config",
    "get_pdf_docling_config"
] 