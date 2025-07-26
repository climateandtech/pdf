#!/usr/bin/env python3
"""
Simple script to set up test streams for debugging
"""
import asyncio
import nats
from config import NatsConfig

# Test configuration - use default names to match client expectations
from config import config
SIMPLE_E2E_CONFIG = config  # Use the default config

async def setup_streams():
    """Set up test streams"""
    print(f"Connecting to NATS at {SIMPLE_E2E_CONFIG.url}")
    nc = await nats.connect(SIMPLE_E2E_CONFIG.url)
    js = nc.jetstream()
    
    # Show account info
    account_info = await js.account_info()
    print(f"JetStream account: {account_info}")
    
    # Create processing stream
    processing_stream_name = f"{SIMPLE_E2E_CONFIG.stream_name}_processing"
    processing_subjects = [f"{SIMPLE_E2E_CONFIG.subject_prefix}.process.*"]
    
    try:
        stream_info = await js.add_stream(
            name=processing_stream_name,
            subjects=processing_subjects
        )
        print(f"Created processing stream: {stream_info.config.name}")
    except Exception as e:
        print(f"Processing stream might exist: {e}")
    
    # Create results stream  
    results_stream_name = f"{SIMPLE_E2E_CONFIG.stream_name}_results"
    results_subjects = [f"{SIMPLE_E2E_CONFIG.subject_prefix}.result.*"]
    
    try:
        stream_info = await js.add_stream(
            name=results_stream_name,
            subjects=results_subjects
        )
        print(f"Created results stream: {stream_info.config.name}")
    except Exception as e:
        print(f"Results stream might exist: {e}")
    
    # Create object store
    try:
        obj_store = await js.create_object_store(SIMPLE_E2E_CONFIG.bucket_name)
        print(f"Created object store: {SIMPLE_E2E_CONFIG.bucket_name}")
    except Exception as e:
        print(f"Object store might exist: {e}")
    
    # List all streams
    print("\nAll streams:")
    streams = await js.streams_info()
    for stream in streams:
        print(f"  - {stream.config.name}: subjects={stream.config.subjects}")
    
    await nc.close()
    print("Setup complete!")

if __name__ == "__main__":
    asyncio.run(setup_streams()) 