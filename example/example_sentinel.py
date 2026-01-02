#!/usr/bin/env python3
"""
Example demonstrating Redis Sentinel backend usage with broadcaster.

Before running:
1. Start Sentinel infrastructure: docker-compose up -d
2. Install dependencies: pip install -e .[redis]
3. Run: python example_sentinel.py
"""

import asyncio
from broadcaster import Broadcast


async def example_url_based():
    """Example using URL-based configuration."""
    print("=== URL-Based Configuration ===")
    
    url = "redis+sentinel://localhost:26379/mymaster"
    async with Broadcast(url) as broadcast:
        print(f"Connected to Sentinel: {url}")
        
        async with broadcast.subscribe("chatroom") as subscriber:
            print("Subscribed to 'chatroom' channel")
            
            # Publish a message
            await broadcast.publish("chatroom", "Hello from Sentinel!")
            print("Published: 'Hello from Sentinel!'")
            
            # Receive the message
            event = await subscriber.get()
            print(f"Received: channel={event.channel!r}, message={event.message!r}")


async def example_multiple_sentinels():
    """Example with multiple sentinel nodes."""
    print("\n=== Multiple Sentinels Configuration ===")
    
    # Multiple sentinels for redundancy
    url = "redis+sentinel://localhost:26379,localhost:26380,localhost:26381/mymaster"
    async with Broadcast(url) as broadcast:
        print(f"Connected through 3 sentinel nodes")
        
        async with broadcast.subscribe("notifications") as subscriber:
            print("Subscribed to 'notifications' channel")
            
            await broadcast.publish("notifications", "System status: OK")
            print("Published: 'System status: OK'")
            
            event = await subscriber.get()
            print(f"Received: {event.message!r}")


async def example_programmatic():
    """Example using programmatic backend configuration."""
    print("\n=== Programmatic Configuration ===")
    
    from broadcaster.backends.redis_sentinel import RedisSentinelBackend
    
    backend = RedisSentinelBackend(
        sentinels=[
            ("localhost", 26379),
            ("localhost", 26380),
            ("localhost", 26381),
        ],
        service_name="mymaster",
    )
    
    async with Broadcast(backend=backend) as broadcast:
        print("Connected using programmatic backend configuration")
        
        async with broadcast.subscribe("events") as subscriber:
            print("Subscribed to 'events' channel")
            
            await broadcast.publish("events", "Custom backend initialized")
            print("Published: 'Custom backend initialized'")
            
            event = await subscriber.get()
            print(f"Received: {event.message!r}")


async def example_with_params():
    """Example with connection parameters."""
    print("\n=== Configuration with Parameters ===")
    
    # URL with query parameters
    url = "redis+sentinel://localhost:26379/mymaster?db=0"
    async with Broadcast(url) as broadcast:
        print(f"Connected with parameters: db=0")
        
        async with broadcast.subscribe("config") as subscriber:
            await broadcast.publish("config", "Connection configured")
            event = await subscriber.get()
            print(f"Received: {event.message!r}")


async def example_health_check_demo():
    """Demonstrate health checking (runs continuously)."""
    print("\n=== Health Checking Demo ===")
    print("This will run for 30 seconds, monitoring connection health...")
    print("Try stopping/restarting redis-master container to see reconnection")
    print("Command: docker-compose restart redis-master")
    
    url = "redis+sentinel://localhost:26379/mymaster"
    async with Broadcast(url) as broadcast:
        async with broadcast.subscribe("health") as subscriber:
            
            # Send messages periodically
            for i in range(6):
                try:
                    message = f"Health check #{i+1}"
                    await broadcast.publish("health", message)
                    print(f"[{i*5}s] Published: {message}")
                    
                    # Use timeout to continue even if no message received
                    try:
                        event = await asyncio.wait_for(subscriber.get(), timeout=5.0)
                        print(f"[{i*5}s] Received: {event.message}")
                    except asyncio.TimeoutError:
                        print(f"[{i*5}s] No message received (timeout)")
                    
                except Exception as e:
                    print(f"[{i*5}s] Error: {e}")
                
                await asyncio.sleep(5)
            
            print("\nHealth check demo completed!")


async def main():
    """Run all examples."""
    print("Redis Sentinel Backend Examples")
    print("=" * 50)
    
    try:
        await example_url_based()
        await example_multiple_sentinels()
        await example_programmatic()
        await example_with_params()
        
        # Optional: Run health check demo (takes 30 seconds)
        # Uncomment to test:
        # await example_health_check_demo()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure Sentinel infrastructure is running:")
        print("  docker-compose up -d")
        print("\nCheck status:")
        print("  docker-compose ps")
        return 1
    
    print("\n✅ All examples completed successfully!")
    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
