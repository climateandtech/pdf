#!/usr/bin/env python3
"""
Service A: File Uploader (Distributed Test)

This service uploads real files to S3 and sends control messages via NATS.
Run this in one terminal while the worker runs in another.
"""
import asyncio
import tempfile
import json
from pathlib import Path
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter

from s3_client import S3DocumentClient
from s3_config import S3Config
from config import NatsConfig

# Configure for moto server
MOTO_S3_CONFIG = S3Config(
    endpoint_url="http://localhost:5000",  # Moto server
    region_name="us-east-1",
    bucket_name="distributed-test-bucket",
    aws_access_key_id="testing",
    aws_secret_access_key="testing"
)

NATS_CONFIG = NatsConfig(
    url="nats://localhost:4222",
    stream_name="DISTRIBUTED_TEST",
    subject_prefix="dist_test"
)

def create_test_pdf(name: str) -> Path:
    """Create a test PDF file"""
    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp:
        c = canvas.Canvas(tmp.name, pagesize=letter)
        c.drawString(100, 750, f"Distributed Test Document: {name}")
        c.drawString(100, 700, "This PDF demonstrates real service-to-service file passing")
        c.drawString(100, 650, "Service A → S3 → NATS → Service B → S3")
        c.drawString(100, 600, f"File: {name}")
        c.drawString(100, 550, "Processing: Real distributed architecture")
        c.save()
        return Path(tmp.name)

async def upload_and_notify(client: S3DocumentClient, pdf_path: Path, doc_name: str):
    """Upload file to S3 and send notification via NATS"""
    print(f"📄 Service A: Creating document '{doc_name}'")
    
    # Upload to S3
    s3_key = f"distributed/{doc_name}.pdf"
    s3_url = await client._upload_to_s3(pdf_path, s3_key)
    
    print(f"☁️  Service A: Uploaded to S3 → {s3_key}")
    print(f"🔗 Service A: S3 URL → {s3_url}")
    
    # Send control message via NATS
    control_message = {
        "service": "Service A (Uploader)",
        "action": "file_uploaded",
        "s3_bucket": MOTO_S3_CONFIG.bucket_name,
        "s3_key": s3_key,
        "s3_url": s3_url,
        "document_name": doc_name,
        "message": f"Please process document: {doc_name}"
    }
    
    # Publish to NATS
    await client.js.publish(
        f"{NATS_CONFIG.subject_prefix}.file_ready",
        json.dumps(control_message).encode()
    )
    
    print(f"📢 Service A: Sent NATS message → {NATS_CONFIG.subject_prefix}.file_ready")
    print(f"✅ Service A: File '{doc_name}' ready for processing by Service B")

async def main():
    """Service A main loop"""
    print("🚀 Starting Service A: File Uploader")
    print("=" * 60)
    
    # Initialize S3 client
    client = S3DocumentClient(s3_config=MOTO_S3_CONFIG, nats_config=NATS_CONFIG)
    await client.setup()
    
    print(f"✅ Service A: Connected to NATS at {NATS_CONFIG.url}")
    print(f"✅ Service A: Connected to S3 at {MOTO_S3_CONFIG.endpoint_url}")
    print(f"✅ Service A: Using bucket '{MOTO_S3_CONFIG.bucket_name}'")
    
    # Upload 3 different documents to show real file passing
    documents = [
        "climate_report_2024",
        "sustainability_analysis", 
        "carbon_footprint_study"
    ]
    
    for i, doc_name in enumerate(documents, 1):
        print(f"\n--- Document {i}/3: {doc_name} ---")
        
        # Create real PDF
        pdf_path = create_test_pdf(doc_name)
        
        try:
            # Upload and notify
            await upload_and_notify(client, pdf_path, doc_name)
            
            # Wait between uploads
            print(f"⏳ Service A: Waiting 3 seconds before next upload...")
            await asyncio.sleep(3)
            
        finally:
            # Cleanup local file
            pdf_path.unlink()
    
    print(f"\n🎉 Service A: Uploaded all {len(documents)} documents!")
    print("📋 Service A: Files are now available for Service B to process")
    print("💡 Service A: Check Service B terminal for processing results")
    
    # Keep connection alive for a bit
    print("\n⏳ Service A: Keeping connection alive for 30 seconds...")
    await asyncio.sleep(30)
    
    await client.close()
    print("👋 Service A: Shutting down")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Service A: Interrupted by user") 