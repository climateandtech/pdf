# Docling Distributed Processing Architecture

This system provides two architectural approaches for distributed document processing with Docling. Both use NATS JetStream for messaging but differ in their storage strategy.

## Current Architecture: NATS + S3 Storage

**Files**: `docling_worker.py`, `services.py`, `s3_client.py`, `s3_config.py`

### Overview
```
Publisher (services.py) → S3 Storage → NATS Message → Worker (docling_worker.py)
                          ↓
                      S3 Object Key → Downloads from S3 → Processes with Docling
```

### Benefits
- **Scalable storage**: Leverages cloud storage (S3, Hetzner Object Storage)
- **Separation of concerns**: Messaging (NATS) separate from storage (S3)
- **Cloud-native**: Works well with existing cloud infrastructure
- **Large file support**: No size limits from messaging system
- **Persistence**: Files remain available beyond message processing

### Configuration
- Requires S3-compatible storage setup
- Uses `.env` file for credentials
- Supports various S3 providers (AWS, Hetzner, etc.)

### Usage
```python
from services import DocumentService

doc_service = DocumentService()
await doc_service.setup()

result = await doc_service.process_document(
    s3_key="documents/my-file.pdf",
    docling_options={
        "format_options": {
            InputFormat.PDF: PdfFormatOption(...)
        }
    }
)
```

## Alternative Architecture: NATS Object Store

**Files**: `worker_nats_objectstore.py`, `client_nats_objectstore.py`

### Overview
```
Publisher (client_nats_objectstore.py) → NATS Object Store → NATS Message → Worker (worker_nats_objectstore.py)
                                         ↓
                                    File in NATS → Downloads from NATS → Processes with Docling
```

### Benefits
- **Simplified deployment**: Everything runs through NATS (single system)
- **No external dependencies**: No S3 setup required
- **Built-in replication**: NATS handles data replication
- **Unified system**: Single NATS cluster handles everything

### Limitations
- **File size limits**: Limited by NATS Object Store capabilities
- **Less cloud-native**: Requires NATS Object Store setup
- **Storage scaling**: Limited by NATS cluster capacity

### Usage
```python
from client_nats_objectstore import DocumentClient

client = DocumentClient()
await client.setup()

result = await client.process_document("path/to/file.pdf")
```

## When to Use Each Approach

### Use NATS + S3 (Current) When:
- ✅ You have existing cloud storage infrastructure
- ✅ Processing large files (>100MB)
- ✅ Need long-term file persistence
- ✅ Want separation between messaging and storage
- ✅ Building cloud-native applications

### Use NATS Object Store When:
- ✅ You want simpler deployment (single system)
- ✅ Processing smaller files (<50MB)
- ✅ Don't want external storage dependencies
- ✅ Need everything self-contained in NATS

## Configuration Options

Both architectures support the same Docling configuration options:

```python
docling_options = {
    "format_options": {
        InputFormat.PDF: PdfFormatOption(
            pipeline_options=PdfPipelineOptions(
                do_ocr=True,
                ocr_engine=OcrEngine.EASYOCR,
                do_table_structure=True
            )
        )
    },
    "accelerator_options": AcceleratorOptions(
        num_threads=4,
        device=AcceleratorDevice.CPU
    )
}
```

## Migration Between Architectures

The systems are designed to be interchangeable. To switch from one to another:

1. **NATS Object Store → NATS + S3**:
   - Set up S3 storage and credentials
   - Replace imports: `client_nats_objectstore` → `services`
   - Replace imports: `worker_nats_objectstore` → `docling_worker`
   - Update deployment scripts

2. **NATS + S3 → NATS Object Store**:
   - Ensure NATS Object Store is enabled
   - Replace imports: `services` → `client_nats_objectstore`  
   - Replace imports: `docling_worker` → `worker_nats_objectstore`
   - Remove S3 dependencies

## Testing

Both architectures have comprehensive test suites:
- `tests/test_docling_options.py` - Configuration testing
- `tests/test_distributed_docling_service.py` - End-to-end testing
- Various integration tests in `tests/` directory

## Future Work

The system is designed to support:
- Dynamic architecture selection via configuration
- Hybrid approaches (NATS messaging + multiple storage backends)
- Additional storage backends (Google Cloud Storage, Azure Blob, etc.) 