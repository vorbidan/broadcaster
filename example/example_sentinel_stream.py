#!/usr/bin/env python3
"""
Example: Redis Sentinel Stream Backend

Demonstrates using Redis Streams with Sentinel for high availability.
Streams provide message persistence and consumer groups functionality.

Setup:
    docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

Run:
    python example/example_sentinel_stream.py
"""

import asyncio
from broadcaster import Broadcast


async def main():
    # Connect to Redis Streams via Sentinel
    # Supports both redis-stream+sentinel:// and rediss-stream+sentinel:// (SSL)
    url = "redis-stream+sentinel://localhost:26379,localhost:26380,localhost:26381/mymaster"
    
    print("=" * 70)
    print("Redis Sentinel Stream Backend Example")
    print("=" * 70)
    print()
    
    async with Broadcast(url) as broadcast:
        print(f"✓ Connected to Redis Streams via Sentinel")
        print(f"  Backend: {type(broadcast._backend).__name__}")
        print()
        
        channel = "stream-test"
        
        # Subscribe to stream
        async with broadcast.subscribe(channel) as subscriber:
            print(f"✓ Subscribed to stream '{channel}'")
            print()
            
            # Publish some messages
            print("Publishing messages to stream...")
            messages = [
                "Hello from Redis Streams!",
                "Message persistence enabled",
                "High availability via Sentinel",
                "Consumer groups supported",
            ]
            
            for i, msg in enumerate(messages, 1):
                await broadcast.publish(channel, msg)
                print(f"  [{i}] Published: {msg}")
                await asyncio.sleep(0.2)
            
            print()
            print("Reading messages from stream...")
            
            # Read messages back
            for i in range(len(messages)):
                event = await asyncio.wait_for(subscriber.get(), timeout=5.0)
                print(f"  [{i+1}] Received: {event.message}")
            
            print()
            print("✓ Stream communication successful!")
            print()
            print("Key features:")
            print("  • Messages persisted in Redis Streams")
            print("  • Automatic failover via Sentinel")
            print("  • Consumer group support (via Redis directly)")
            print("  • Message acknowledgment and replay")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\nError: {e}")
        import traceback
        traceback.print_exc()
