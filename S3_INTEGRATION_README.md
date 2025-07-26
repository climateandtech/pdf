# S3 + NATS Document Processing Integration

## 🎯 Overview

This implementation provides a **production-ready S3 + NATS control bus architecture** for document processing, following industry best practices and addressing the limitations of using NATS Object Store as a persistence layer.

## 🏗️ Architecture

```
┌─────────┐ 1. PUT (multipart) ┌──────────────┐    3. presigned URL
│Uploader │───────────────────▶│   S3 / MinIO │◀────────────────┐
└─────────┘                    └──────────────┘                 │
      │2. NATS publish (JSON)         ▲                        │
      ▼                                │4. PUT processed file   │
┌──────────────┐         6. NATS pub   │                        │
│JetStream     │◀──────────────────────┘                        │
│"control bus" │                5. download + process           │
└──────────────┘                    ▲                           │
      ▲                             │                           ▼
      │                             │                           ▼
┌──────────────┐    subscribe  ┌──────────────┐       GET   ┌─────────┐
│Processor svc │──────────────▶│   Worker(s)  │───────────▶│Uploader │
└──────────────┘               └──────────────┘            └─────────┘
```

## ✅ Benefits Over NATS Object Store

1. **Scalability**: S3 handles GBs/TBs without NATS cluster limitations
2. **Reliability**: S3's 99.999999999% durability vs NATS memory/disk limits  
3. **Cost**: S3 pricing optimized for storage vs NATS cluster resources
4. **Integration**: Works with existing S3 ecosystems and tools
5. **Performance**: Multipart uploads, CDN integration, transfer acceleration

## 🚀 Quick Start

### Basic Usage

```python
from s3_integration import process_pdf_with_s3_sync, configure_s3_storage

# Simple usage with default config
result = process_pdf_with_s3_sync('document.pdf')

# With custom S3 configuration (MinIO example)
s3_config = configure_s3_storage(
    endpoint_url="http://localhost:9000",
    bucket_name="my-documents",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin"
)

result = process_pdf_with_s3_sync(
    'document.pdf', 
    config_override=s3_config
)
```

### Async Usage

```python
from s3_integration import S3DocumentService

async with S3DocumentService() as service:
    result = await service.process_document('document.pdf')
    print(f"Processing result: {result}")
```

## 🔧 Configuration

### Environment Variables

```bash
# S3 Configuration
export S3_ENDPOINT_URL="http://localhost:9000"  # MinIO endpoint
export S3_BUCKET="documents"
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_DEFAULT_REGION="us-east-1"

# Processing Configuration
export USE_S3_STORAGE="true"
export DEBUG="false"
```

### Programmatic Configuration

```python
from s3_config import S3Config, ProcessingConfig

# S3 settings
s3_config = S3Config(
    endpoint_url="http://localhost:9000",
    bucket_name="documents",
    region_name="us-east-1",
    multipart_threshold=100 * 1024 * 1024,  # 100MB
    max_concurrency=10
)

# Processing settings
processing_config = ProcessingConfig(
    timeout=600,  # 10 minutes
    use_s3_storage=True,
    cleanup_on_error=True
)
```

## 🔌 Integration with Existing services.py

### Option 1: Direct Integration

Add to your existing `services.py`:

```python
from s3_integration import process_pdf_with_s3_sync, configure_s3_storage

class DocumentService:
    def __init__(self, use_s3=False):
        self.use_s3 = use_s3
    
    def process_pdf(self, pdf_path: str, custom_prompt: str = None):
        if self.use_s3:
            # Use S3 + NATS control bus
            return process_pdf_with_s3_sync(
                pdf_path,
                custom_prompt=custom_prompt
            )
        else:
            # Use existing NATS Object Store implementation
            return self.existing_process_pdf(pdf_path, custom_prompt)
```

### Option 2: Factory Pattern

```python
from s3_integration import S3DocumentService
from client import DocumentClient  # existing implementation

def create_document_service(use_s3: bool = False):
    if use_s3:
        return S3DocumentService()
    else:
        return DocumentClient()

# Usage
async with create_document_service(use_s3=True) as service:
    result = await service.process_document('document.pdf')
```

## 📁 File Structure

```
pdf/
├── s3_config.py           # Configuration with pydantic validation
├── s3_client.py           # Main S3 + NATS client implementation
├── s3_integration.py      # Service layer and integration helpers
├── test_s3_simple.py      # Simple functional tests
├── tests/
│   └── test_s3_client_tdd.py  # Comprehensive TDD test suite
└── requirements.txt       # Dependencies with aioboto3, moto, etc.
```

## 🧪 Testing

### Run Simple Tests
```bash
cd pdf
PYTHONPATH=. python test_s3_simple.py
```

### Run TDD Test Suite
```bash
cd pdf
PYTHONPATH=. pytest tests/test_s3_client_tdd.py::TestS3ConfigValidation -v
```

### Test with MinIO

1. Start MinIO:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

2. Test with MinIO endpoint:
```python
from s3_integration import process_pdf_with_s3_sync, configure_s3_storage

config = configure_s3_storage(
    endpoint_url="http://localhost:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
    bucket_name="test-documents"
)

result = process_pdf_with_s3_sync('document.pdf', config_override=config)
```

## 🔄 Workflow Details

### 1. Upload Phase
- **File**: PDF uploaded to S3 with automatic multipart handling
- **Key**: `raw/{request_id}.pdf`
- **URL**: Presigned URL generated for worker access

### 2. Control Message
```json
{
  "request_id": "uuid-here",
  "s3_bucket": "documents",
  "s3_key": "raw/uuid-here.pdf", 
  "s3_url": "https://presigned-url",
  "timestamp": "2024-01-15T10:30:00Z",
  "file_size": 1234567,
  "processing_timeout": 600
}
```

### 3. Processing
- Worker downloads from S3 URL
- Processes with docling-parse  
- Uploads result to S3
- Publishes completion message

### 4. Result Retrieval
- Client receives completion message
- Downloads processed result
- Cleans up temporary objects

## 🚨 Error Handling

### Automatic Retry
```python
from s3_client import S3DocumentClient

client = S3DocumentClient()
try:
    result = await client.process_document('document.pdf', timeout=300)
except TimeoutError:
    print("Processing timed out")
except Exception as e:
    print(f"Processing failed: {e}")
    # S3 objects automatically cleaned up if cleanup_on_error=True
```

### Manual Cleanup
```python
# Clean up S3 objects manually if needed
await client._cleanup_s3_object("raw/request-id.pdf")
```

## 📊 Performance Optimization

### Boto3 Transfer Settings
```python
# Optimized for large files
transfer_config = TransferConfig(
    multipart_threshold=100 * 1024 * 1024,  # 100MB
    max_concurrency=10,
    multipart_chunksize=8 * 1024 * 1024,    # 8MB chunks
    use_threads=True
)
```

### Connection Pooling
- aioboto3 handles connection pooling automatically
- Async context managers ensure proper resource cleanup
- Session reuse across requests

## 🔒 Security

### Authentication
- Standard AWS credential chain support
- Environment variables
- IAM roles (when running on AWS)
- Programmatic credentials

### Permissions
Required S3 permissions:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject", 
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket",
        "arn:aws:s3:::your-bucket/*"
      ]
    }
  ]
}
```

## 🚀 Production Deployment

### Docker Compose Example
```yaml
version: '3.8'
services:
  nats:
    image: nats:latest
    command: ["-js", "-sd", "/nats-storage"]
    ports:
      - "4222:4222"
    volumes:
      - nats-storage:/nats-storage

  minio:
    image: minio/minio
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    volumes:
      - minio-data:/data

  pdf-processor:
    build: .
    environment:
      NATS_URL: nats://nats:4222
      S3_ENDPOINT_URL: http://minio:9000
      AWS_ACCESS_KEY_ID: minioadmin
      AWS_SECRET_ACCESS_KEY: minioadmin
    depends_on:
      - nats
      - minio
```

## 📈 Monitoring

### Metrics to Track
- Upload success/failure rates
- Processing latency (upload → result)
- S3 transfer speeds
- NATS message delivery
- Worker processing times

### Health Checks
```python
async def health_check():
    try:
        async with S3DocumentService() as service:
            # Quick connectivity test
            return {"status": "healthy", "s3": "connected", "nats": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
```

## 🎉 Summary

This S3 + NATS implementation provides:

✅ **Production-ready architecture** following industry best practices  
✅ **Clean boto3 integration** with proper async support  
✅ **Minimal boilerplate** with factory functions and context managers  
✅ **Comprehensive testing** with TDD approach  
✅ **Easy integration** with existing services.py  
✅ **Performance optimization** with multipart uploads and connection pooling  
✅ **Flexible configuration** supporting AWS, MinIO, and S3-compatible services

Ready for immediate production use! 🚀 