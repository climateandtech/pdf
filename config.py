from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv
import os

load_dotenv()

class NatsConfig(BaseModel):
    url: str = os.getenv("NATS_URL", "nats://localhost:4222")
    token: Optional[str] = os.getenv("NATS_TOKEN")  # NEW: Token authentication
    stream_name: str = "DOCUMENTS"
    subject_prefix: str = "docs"
    
    # Connection settings
    connect_timeout: int = int(os.getenv("NATS_CONNECT_TIMEOUT", "10"))
    max_reconnect_attempts: int = int(os.getenv("NATS_MAX_RECONNECT_ATTEMPTS", "10"))
    
    # JetStream settings
    max_payload_size: int = 8 * 1024 * 1024  # 8MB default
    max_stream_size: int = 1024 * 1024 * 1024  # 1GB default
    
    @property
    def connection_url(self) -> str:
        """Get connection URL with token if provided"""
        if self.token:
            # Insert token into URL: nats://token@host:port
            parts = self.url.replace("nats://", "").split(":")
            host = parts[0]
            port = parts[1] if len(parts) > 1 else "4222"
            return f"nats://{self.token}@{host}:{port}"
        return self.url

# Global config instance
config = NatsConfig() 