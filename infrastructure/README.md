# Infrastructure Components

This directory contains **pure infrastructure components** that are deployed separately from processing services.

## Directory Structure

```
infrastructure/
├── nats-server/           # NATS server setup (dedicated server)
│   ├── nats-server.conf   # Server configuration
│   ├── setup.sh          # Installation script
│   └── README.md         # Setup instructions
└── monitoring/           # Future: Monitoring stack
    └── ...
```

## Deployment Model

### 🏗️ **Infrastructure Server (NATS)**
- **What**: Pure NATS message broker with JetStream
- **Where**: Dedicated server (infrastructure only)
- **Deploy**: `infrastructure/nats-server/`
- **Purpose**: Message routing for all processing services

### ⚡ **Processing Servers** 
- **What**: Actual processing services (PDF, images, etc.)
- **Where**: GPU servers, CPU servers, etc.
- **Deploy**: Service directories (`pdf/`, `image_processing/`, etc.)
- **Purpose**: Connect to NATS, process requests

### 💻 **Client Applications**
- **What**: Your laptop, web apps, etc.
- **Where**: Anywhere with network access
- **Deploy**: Client libraries from service directories
- **Purpose**: Submit processing requests via NATS

## Setup Flow

### 1. Infrastructure Setup (Once)
```bash
# On your dedicated NATS server
cd infrastructure/nats-server/
chmod +x setup.sh
./setup.sh
```

### 2. Processing Services (Multiple servers)
```bash
# On each processing server (GPU, CPU, etc.)
cd pdf/  # or other service directory
# Configure .env with NATS server details
# Start the worker
```

### 3. Clients (Multiple locations)
```bash
# On laptop, web servers, etc.
# Configure .env with NATS server details  
# Use client libraries to submit requests
```

## Configuration

### Infrastructure Server
- No service-specific config
- Generic NATS settings
- Authentication token
- Resource limits

### Processing Services
- Service-specific configuration
- References central NATS server
- Own storage/compute settings

## Benefits of This Architecture

✅ **Clean Separation**: Infrastructure vs. business logic  
✅ **Scalability**: Add processing servers independently  
✅ **Flexibility**: Mix different service types on same NATS  
✅ **Reliability**: NATS server dedicated to messaging only  
✅ **Security**: Centralized authentication and networking  

## Adding New Services

1. Create new service directory (e.g., `image_processing/`)
2. Implement worker using NATS client patterns
3. Configure unique stream name and subject prefix
4. Deploy on appropriate servers (GPU for AI, CPU for text, etc.)
5. All services share the same NATS infrastructure

---

**Key Point**: The NATS server knows nothing about PDFs, images, or any specific processing. It's pure message infrastructure. 