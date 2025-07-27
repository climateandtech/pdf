# NATS Server Setup Guide

Guide for setting up a secure NATS server on a dedicated server for the Docling distributed processing system.

## Quick Setup (Ubuntu/Debian)

### 1. Install NATS Server

```bash
# Download and install NATS server
curl -L https://github.com/nats-io/nats-server/releases/download/v2.10.7/nats-server-v2.10.7-linux-amd64.zip -o nats-server.zip
unzip nats-server.zip
sudo mv nats-server-v2.10.7-linux-amd64/nats-server /usr/local/bin/
sudo chmod +x /usr/local/bin/nats-server
```

### 2. Generate Secure Token

```bash
# Generate a secure 64-character token
openssl rand -hex 32
# Example output: 7f8e9d2c1a3b4e5f6789abc0def12345678901234567890abcdef1234567890ab
```

### 3. Create Configuration

```bash
# Create NATS config directory
sudo mkdir -p /etc/nats
sudo mkdir -p /var/lib/nats
sudo mkdir -p /var/log/nats

# Copy the nats-server.conf from this repo
sudo cp nats-server.conf /etc/nats/

# Edit the config and replace the token
sudo nano /etc/nats/nats-server.conf
# Replace: "your-secure-token-here-replace-with-random-64-char-string"
# With your generated token from step 2
```

### 4. Create Systemd Service

```bash
sudo tee /etc/systemd/system/nats.service > /dev/null <<EOF
[Unit]
Description=NATS Server
After=network.target

[Service]
Type=simple
User=nats
Group=nats
ExecStart=/usr/local/bin/nats-server -c /etc/nats/nats-server.conf
ExecReload=/bin/kill -HUP \$MAINPID
KillMode=process
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
EOF
```

### 5. Create NATS User and Set Permissions

```bash
# Create nats user
sudo useradd --system --no-create-home --shell /bin/false nats

# Set ownership
sudo chown -R nats:nats /var/lib/nats
sudo chown -R nats:nats /var/log/nats
sudo chown nats:nats /etc/nats/nats-server.conf
```

### 6. Start NATS Service

```bash
# Enable and start NATS
sudo systemctl enable nats
sudo systemctl start nats

# Check status
sudo systemctl status nats

# View logs
sudo journalctl -u nats -f
```

## Firewall Configuration

### Allow NATS Port (4222)

```bash
# UFW (Ubuntu)
sudo ufw allow 4222/tcp
sudo ufw allow from YOUR_LAPTOP_IP to any port 4222
sudo ufw allow from YOUR_GPU_SERVER_IP to any port 4222

# Or more restrictive - only from specific IPs
sudo ufw allow from YOUR_LAPTOP_IP to any port 4222 proto tcp
sudo ufw allow from YOUR_GPU_SERVER_IP to any port 4222 proto tcp
```

### Optional: Monitoring Port (8222)

```bash
# Only if you want to access NATS monitoring from specific IPs
sudo ufw allow from YOUR_LAPTOP_IP to any port 8222 proto tcp
```

## Client Configuration

### On Laptop and GPU Server

1. **Copy environment template**:
```bash
cp environment_config.txt .env
```

2. **Edit `.env` file**:
```bash
# Replace with your NATS server details
NATS_URL=nats://YOUR_NATS_SERVER_IP:4222
NATS_TOKEN=your-secure-token-here-replace-with-random-64-char-string

# Add your S3 credentials
AWS_ACCESS_KEY_ID=your-hetzner-access-key
AWS_SECRET_ACCESS_KEY=your-hetzner-secret-key
# ... etc
```

## Testing Connection

### Test from laptop or GPU server:

```bash
# Activate virtual environment
source venv/bin/activate

# Test NATS connection
python -c "
import asyncio
from config import config

async def test():
    import nats
    try:
        nc = await nats.connect(config.connection_url)
        print('✅ NATS connection successful!')
        print(f'Connected to: {config.url}')
        print(f'Using token: {config.token[:8]}...')
        await nc.close()
    except Exception as e:
        print(f'❌ NATS connection failed: {e}')

asyncio.run(test())
"
```

## Security Best Practices

### 1. Network Security
- **Firewall**: Only allow connections from known IPs
- **VPN**: Consider using VPN for additional network isolation
- **Private network**: Use private/internal IP addresses when possible

### 2. Token Security
- **Generate strong tokens**: Use `openssl rand -hex 32`
- **Rotate tokens regularly**: Change tokens periodically
- **Environment variables**: Never commit tokens to code
- **Secure storage**: Use `.env` files with proper permissions (`chmod 600 .env`)

### 3. Additional Options (Advanced)

#### TLS Encryption
Add to `nats-server.conf`:
```
tls {
    cert_file: "/path/to/server.crt"
    key_file: "/path/to/server.key"
}
```

#### User/Password Authentication
Replace token auth with:
```
authorization {
    users = [
        {user: "docling", password: "secure-password"}
    ]
}
```

## Monitoring

### Check NATS Status
```bash
# Service status
sudo systemctl status nats

# View logs
sudo journalctl -u nats -f

# Monitor connections (if monitoring enabled)
curl http://YOUR_NATS_SERVER_IP:8222/connz
```

### NATS CLI Tool (Optional)
```bash
# Install NATS CLI
curl -sf https://binaries.nats.dev/nats-io/natscli/nats@latest | sh

# Test connection
nats --server=nats://TOKEN@YOUR_NATS_SERVER_IP:4222 server info
```

---

This setup provides **simple but effective security** for your NATS server. The token authentication prevents unauthorized access while being easy to configure and manage. 