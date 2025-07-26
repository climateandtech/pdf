#!/usr/bin/env python3
"""
Service B: File Processor Worker (Distributed Test)

This service listens to NATS messages and downloads files from S3 for processing.
Run this in a separate terminal from the uploader.
"""
import asyncio
import json
import tempfile
from pathlib import Path

from s3_client import S3DocumentClient
from s3_config import S3Config
from config import NatsConfig

# Configure for moto server (same as uploader)
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

async def process_document(client: S3DocumentClient, s3_key: str, doc_name: str) -> dict:
    """Download file from S3 and simulate processing"""
    print(f"⬇️  Service B: Downloading from S3 → {s3_key}")
    
    # Download file from S3
    file_content = await client.download_result(s3_key)
    
    print(f"📄 Service B: Downloaded {len(file_content)} bytes")
    
    # Simulate document processing (would use docling-parse here)
    # For demo, we'll just extract some "metadata"
    processed_result = {
        "document_name": doc_name,
        "file_size": len(file_content),
        "pages": 1,  # Simulated
        "processing_service": "Service B (Worker)",
        "content_summary": f"Processed document '{doc_name}' - Climate and sustainability analysis",
        "extracted_text": f"Key findings from {doc_name}: Climate change impacts...",
        "status": "processing_complete"
    }
    
    print(f"🔬 Service B: Processed document '{doc_name}'")
    print(f"📊 Service B: Extracted {len(processed_result['extracted_text'])} chars of text")
    
    return processed_result

async def handle_file_message(client: S3DocumentClient, message):
    """Handle incoming file notification from NATS"""
    try:
        # Parse the control message
        control_data = json.loads(message.data.decode())
        
        print(f"\n📨 Service B: Received message from {control_data['service']}")
        print(f"📁 Service B: File → {control_data['document_name']}")  
        print(f"🗂️  Service B: S3 Key → {control_data['s3_key']}")
        
        # Process the document
        result = await process_document(
            client, 
            control_data['s3_key'],
            control_data['document_name']
        )
        
        # Send processing result back via NATS
        result_message = {
            "service": "Service B (Worker)",
            "action": "processing_complete",
            "original_request": control_data,
            "result": result,
            "message": f"Successfully processed {control_data['document_name']}"
        }
        
        await client.js.publish(
            f"{NATS_CONFIG.subject_prefix}.processing_complete",
            json.dumps(result_message).encode()
        )
        
        print(f"📤 Service B: Sent processing result via NATS")
        print(f"✅ Service B: Completed processing '{control_data['document_name']}'")
        
        # Acknowledge the message
        await message.ack()
        
    except Exception as e:
        print(f"❌ Service B: Error processing message: {e}")
        await message.nak()

async def main():
    """Service B main loop"""
    print("🚀 Starting Service B: File Processor Worker")
    print("=" * 60)
    
    # Initialize S3 client
    client = S3DocumentClient(s3_config=MOTO_S3_CONFIG, nats_config=NATS_CONFIG)
    await client.setup()
    
    print(f"✅ Service B: Connected to NATS at {NATS_CONFIG.url}")
    print(f"✅ Service B: Connected to S3 at {MOTO_S3_CONFIG.endpoint_url}")
    print(f"✅ Service B: Using bucket '{MOTO_S3_CONFIG.bucket_name}'")
    
    # Set up NATS subscription to listen for file notifications
    subscription = await client.js.pull_subscribe(
        subject=f"{NATS_CONFIG.subject_prefix}.file_ready",
        durable="file_processor_worker",
        stream=NATS_CONFIG.stream_name
    )
    
    print(f"🎧 Service B: Listening for messages on '{NATS_CONFIG.subject_prefix}.file_ready'")
    print("💡 Service B: Waiting for Service A to upload files...")
    print("⏳ Service B: Ready to process documents (Ctrl+C to stop)")
    
    # Main processing loop
    processed_count = 0
    
    try:
        while True:
            try:
                # Wait for messages (10 second timeout)
                messages = await subscription.fetch(batch=1, timeout=10)
                
                if messages:
                    for message in messages:
                        await handle_file_message(client, message)
                        processed_count += 1
                        
                        print(f"📈 Service B: Processed {processed_count} documents so far")
                else:
                    print("⏱️  Service B: No messages (waiting for Service A...)")
                    
            except asyncio.TimeoutError:
                print("⏱️  Service B: Timeout waiting for messages (continuing...)")
                continue
                
    except KeyboardInterrupt:
        print(f"\n👋 Service B: Interrupted by user")
    finally:
        print(f"📊 Service B: Final stats - Processed {processed_count} documents")
        await client.close()
        print("👋 Service B: Shutting down")

if __name__ == "__main__":
    asyncio.run(main()) 