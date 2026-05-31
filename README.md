# Distributed Processing System

A **generic distributed processing system** using NATS JetStream for message routing and supporting multiple service types (PDF processing, image analysis, text processing, etc.).

## 🏗️ Architecture Overview

```
Infrastructure Server    Processing Servers         Client Applications
     (NATS)              (GPU, CPU, etc.)           (Laptop, Web, etc.)
        |                       |                           |
   ┌────────────┐        ┌─────────────┐             ┌──────────────┐
   │    NATS    │◄──────►│ PDF Worker  │             │   Your App   │
   │ JetStream  │        │ Image Worker│◄───────────►│  (services.py│
   │  Message   │        │ Text Worker │             │   thinktank2) │
   │   Broker   │        │     ...     │             │      ...     │
   └────────────┘        └─────────────┘             └──────────────┘
        │                       │                           │
   Pure Messaging         Business Logic              Submit Requests
```

## 📁 Directory Structure

```
ct/
├── infrastructure/          # 🏗️ Infrastructure components
│   └── nats-server/        # Pure NATS server (dedicated server)
├── pdf/                    # 📄 PDF processing service (GPU server)
│   ├── docling_worker.py   # Worker process
│   ├── services.py         # Client library
│   └── tests/             # Service tests
└── future_services/        # 🔮 Add more services as needed
    ├── image_processing/
    ├── text_analysis/
    └── audio_transcription/
```

## 🚀 Quick Start

### 1. Infrastructure Setup (NATS Server)

**On your dedicated NATS server:**
```bash
cd infrastructure/nats-server/
./setup.sh
# Save the generated token - you'll need it for all services!
```

### 2. PDF Processing Service Setup (GPU Server)

**On your GPU server:**
```bash
cd pdf/
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Configure environment
cp environment_config.txt .env
# Edit .env with NATS server IP and token

# Start docling worker (docs.process.*)
./start_worker.sh

# Optional: GLiNER KG infer (kg.infer) — same venv + extra deps
pip install -r requirements-gliner.txt
./start_kg_gliner.sh
```

GPU deploy (both workers): push to `climateandtech/pdf` `main`, then from **`coolify-provisioning/`** run `./gpu-setup-production.sh` (once) and `./gpu-deploy-worker.sh`. See **`docs/GPU_PRODUCTION.md`**.

Platform calls `kg.infer` when `KG_EXTRACT_ON_GPU=1` (no separate platform clone on GPU).

### 3. Client Integration (Your Laptop)

**Connect your existing services.py:**
```python
# In your thinktank2 project
from pdf.services import DocumentService

# Configure to point to your NATS server
doc_service = DocumentService()
await doc_service.setup()

result = await doc_service.process_document(
    s3_key="documents/my-file.pdf",
    docling_options={...}
)
```

## 🎛️ Configuration

### Infrastructure Server (.env)
```env
# Pure NATS configuration - no service specifics
NATS_TOKEN=your-generated-secure-token
```

### Processing Services (.env)
```env
# Points to your infrastructure
NATS_URL=nats://your-nats-server-ip:4222
NATS_TOKEN=your-generated-secure-token

# Service-specific settings
AWS_ACCESS_KEY_ID=your-s3-credentials
# ... etc
```

## 🔧 Service Types & Namespacing

Each service type gets its own namespace on the shared NATS server:

| Service Type | Stream Name | Subject Prefix | Worker Group |
|-------------|-------------|----------------|--------------|
| PDF Docling | `PDF_PROCESSING` | `pdf.docling.*` | `pdf_docling_workers` |
| Image Processing | `IMAGE_PROCESSING` | `image.process.*` | `image_workers` |
| Text Analysis | `TEXT_ANALYSIS` | `text.analyze.*` | `text_workers` |
| Audio Transcription | `AUDIO_TRANSCRIPTION` | `audio.transcribe.*` | `audio_workers` |

## 📋 Deployment Scenarios

### Scenario 1: Simple Setup
- **NATS Server**: 1 dedicated server
- **PDF Processing**: 1 GPU server
- **Clients**: Your laptop

### Scenario 2: Production Setup
- **NATS Cluster**: 3 servers (HA)
- **PDF Workers**: Multiple GPU servers (auto-scaling)
- **Image Workers**: Multiple CPU servers
- **Clients**: Web applications, mobile apps, etc.

### Scenario 3: Development
- **NATS**: Local Docker container
- **Workers**: Local processes
- **Clients**: Local development

## 🛡️ Security

- **Token Authentication**: Secure token for NATS access
- **Network Isolation**: Firewall rules for known IPs only
- **TLS**: Optional TLS encryption for production
- **Separate Concerns**: Infrastructure vs. business logic

## 🔄 Adding New Services

1. **Create service directory**: `mkdir new_service/`
2. **Implement worker**: Use existing patterns from `pdf/`
3. **Configure namespace**: Add to `generic_config.py`
4. **Deploy**: On appropriate servers (GPU, CPU, etc.)
5. **Connect**: All services use the same NATS infrastructure

## 📖 Documentation

- **[Infrastructure Setup](infrastructure/README.md)** - NATS server deployment
- **[Architecture Guide](ARCHITECTURE.md)** - Detailed system design
- **[PDF Service](pdf/README.md)** - PDF processing specifics

## 🧪 Testing

```bash
# Test infrastructure
cd infrastructure/nats-server/
# Connection tests included in setup

# Test PDF service
cd pdf/
pytest tests/ -v

# Test end-to-end
python -c "
import asyncio
from services import DocumentService

async def test():
    service = DocumentService()
    await service.setup()
    print('✅ Connected to distributed system!')

asyncio.run(test())
"
```

---

## 🎯 Key Benefits

✅ **Scalable**: Add processing power by adding servers  
✅ **Flexible**: Mix different service types on same infrastructure  
✅ **Reliable**: Dedicated message infrastructure  
✅ **Maintainable**: Clear separation of concerns  
✅ **Future-proof**: Easy to add new processing capabilities  

**Perfect for**: Multi-modal AI processing, distributed computing, microservices architecture 