#!/usr/bin/env python3
"""
Setup NATS Streams for Docling Worker

This script creates the required NATS JetStream streams for the docling worker.
"""
import asyncio
import nats
from config import NatsConfig

async def setup_streams():
    """Create the required NATS streams"""
    config = NatsConfig()
    
    print("🚀 Setting up NATS streams for Docling Worker")
    print(f"📡 Connecting to NATS: {config.url}")
    
    # Connect to NATS
    nc = await nats.connect(config.url)
    js = nc.jetstream()
    
    try:
        # Create main processing stream
        print(f"📦 Creating stream: {config.stream_name}")
        await js.add_stream(
            name=config.stream_name,
            subjects=[f"{config.subject_prefix}.process.*"],
            storage="memory",
            retention="workqueue",
            max_msgs=1000,
            max_bytes=100 * 1024 * 1024  # 100MB
        )
        print(f"✅ Created stream: {config.stream_name}")
        
        # Create processing results stream
        results_stream = f"{config.stream_name}_processing"
        print(f"📦 Creating stream: {results_stream}")
        await js.add_stream(
            name=results_stream,
            subjects=[f"{config.subject_prefix}.process.*"],
            storage="memory", 
            retention="workqueue",
            max_msgs=1000,
            max_bytes=100 * 1024 * 1024  # 100MB
        )
        print(f"✅ Created stream: {results_stream}")
        
        # Create results stream  
        results_only_stream = f"{config.stream_name}_results"
        print(f"📦 Creating stream: {results_only_stream}")
        await js.add_stream(
            name=results_only_stream,
            subjects=[f"{config.subject_prefix}.result.*"],
            storage="memory",
            retention="limits",
            max_msgs=1000,
            max_bytes=100 * 1024 * 1024,  # 100MB
            max_age=3600  # Keep results for 1 hour
        )
        print(f"✅ Created stream: {results_only_stream}")
        
        # List all streams to verify
        print("\n📋 Current streams:")
        streams = await js.streams_info()
        for stream in streams:
            print(f"   ✅ {stream.config.name}: {stream.config.subjects}")
            
        print(f"\n🎉 NATS streams setup complete!")
        print(f"   Ready for docling worker on: {config.subject_prefix}.process.*")
        
    except Exception as e:
        if "already exists" in str(e):
            print(f"⚠️  Streams already exist - that's OK!")
        else:
            print(f"❌ Error creating streams: {e}")
            raise
    finally:
        await nc.close()

if __name__ == "__main__":
    asyncio.run(setup_streams()) 