# Docling Distributed Processing System

A distributed document processing system using Docling, NATS JetStream, and cloud storage for scalable PDF processing with configurable options.

## Quick Start

1. **Install dependencies**:
```bash
pip install -r requirements.txt
```

2. **Configure environment** (see `environment_config.txt` for template):
```bash
cp environment_config.txt .env
# Edit .env with your S3 and NATS credentials
```

3. **Start NATS server**:
```bash
nats-server -js
```

4. **Set up NATS streams**:
```bash
python setup_nats_streams.py
```

5. **Start the worker**:
```bash
python docling_worker.py
```

6. **Process documents**:
```python
from services import DocumentService

doc_service = DocumentService()
await doc_service.setup()

result = await doc_service.process_document(
    s3_key="documents/my-file.pdf",
    docling_options={
        "format_options": {
            InputFormat.PDF: PdfFormatOption(
                pipeline_options=PdfPipelineOptions(
                    do_ocr=True,
                    ocr_engine=OcrEngine.EASYOCR
                )
            )
        }
    }
)
```

## Architecture

This system supports **two architectural approaches**:

1. **NATS + S3 Storage** (current/recommended) - `docling_worker.py`, `services.py`
2. **NATS Object Store** (alternative) - `worker_nats_objectstore.py`, `client_nats_objectstore.py`

📖 **See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed comparison and usage guidance.**

## Key Features

- **Generic Configuration**: Pass any valid Docling options through to workers
- **Multiple Storage Options**: S3-compatible storage or NATS Object Store
- **Scalable**: Distributed worker architecture via NATS messaging
- **Flexible**: Support for all Docling input formats and processing options
- **Well-tested**: Comprehensive unit and integration test suite

## Core Files

### Current Architecture (NATS + S3)
- `docling_worker.py` - Main document processing worker
- `services.py` - Client for submitting processing requests  
- `s3_client.py` - Advanced S3 integration with async support
- `s3_config.py` - S3 configuration management

### Alternative Architecture (NATS Object Store)
- `worker_nats_objectstore.py` - NATS Object Store worker
- `client_nats_objectstore.py` - NATS Object Store client

### Configuration & Examples
- `docling_options_examples.py` - Examples of Docling configuration patterns
- `config.py` - NATS configuration
- `setup_nats_streams.py` - NATS stream initialization

## Testing

Run the comprehensive test suite:

```bash
# All tests
pytest tests/ -v

# Specific test categories
pytest tests/test_docling_options.py -v          # Configuration tests
pytest tests/test_distributed_docling_service.py -v  # End-to-end tests
```

## Documentation

- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture comparison
- **[S3_INTEGRATION_README.md](S3_INTEGRATION_README.md)** - S3 setup guide
- **`environment_config.txt`** - Environment configuration template

## Dependencies

All dependencies are consolidated in `requirements.txt` including:
- Core: `docling`, `nats-py`, `python-dotenv`
- Storage: `aioboto3`, `boto3` for S3 support
- Testing: `pytest`, `pytest-asyncio`, `reportlab`
- Mocking: `moto[s3]` for S3 testing

---

*For detailed architecture decisions and migration guidance, see [ARCHITECTURE.md](ARCHITECTURE.md)* 