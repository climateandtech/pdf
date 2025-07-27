#!/usr/bin/env python3
"""
Simple NATS connection test script
"""
import asyncio
import os
from dotenv import load_dotenv
import nats
from nats.errors import TimeoutError, NoServersError

# Load environment variables
load_dotenv()

async def test_nats_connection():
    """Test NATS connection with current configuration"""
    
    # Get configuration from environment
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    nats_token = os.getenv("NATS_TOKEN")
    
    print(f"🔧 NATS URL: {nats_url}")
    print(f"🔑 NATS Token: {'✅ Set' if nats_token else '❌ Not set'}")
    
    # Format connection URL with token if provided
    if nats_token:
        # Insert token into URL: nats://token@host:port
        parts = nats_url.replace("nats://", "").split(":")
        host = parts[0]
        port = parts[1] if len(parts) > 1 else "4222"
        connection_url = f"nats://{nats_token}@{host}:{port}"
        print(f"🔗 Connection URL: nats://{nats_token[:8]}...@{host}:{port}")
    else:
        connection_url = nats_url
        print(f"🔗 Connection URL: {connection_url}")
    
    # Test different connection scenarios
    scenarios = []
    
    # Scenario 1: With token (if available)
    if nats_token:
        scenarios.append(("With Token", connection_url))
    
    # Scenario 2: Without token
    scenarios.append(("Without Token", nats_url))
    
    # Scenario 3: Localhost fallback
    scenarios.append(("Localhost Fallback", "nats://localhost:4222"))
    
    for scenario_name, url in scenarios:
        print(f"\n🧪 Testing {scenario_name}: {url.replace(nats_token or '', '***') if nats_token else url}")
        
        try:
            nc = nats.NATS()
            await nc.connect(url, connect_timeout=5, max_reconnect_attempts=1)
            print(f"✅ {scenario_name}: Connection successful!")
            
            # Test JetStream
            try:
                js = nc.jetstream()
                # Try to get stream info (this will fail if JetStream is not enabled)
                stream_names = await js.streams_info()
                print(f"📊 JetStream: Available ({len(stream_names)} streams)")
            except Exception as e:
                print(f"⚠️  JetStream: Not available - {e}")
            
            await nc.close()
            print(f"🔒 {scenario_name}: Connection closed cleanly")
            return  # Success, exit early
            
        except TimeoutError:
            print(f"⏱️  {scenario_name}: Connection timeout - server not reachable")
        except NoServersError:
            print(f"🚫 {scenario_name}: No servers available - check URL")
        except Exception as e:
            error_msg = str(e)
            if "Authorization Violation" in error_msg:
                print(f"🔐 {scenario_name}: Authorization failed - check token")
            else:
                print(f"❌ {scenario_name}: Connection failed - {error_msg}")

if __name__ == "__main__":
    print("🚀 NATS Connection Test\n")
    asyncio.run(test_nats_connection()) 