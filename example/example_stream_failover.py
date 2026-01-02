"""
Test Redis Sentinel Stream backend with master failover.
This verifies that Redis Streams work correctly during Sentinel failover.
"""
import asyncio
import logging
import subprocess
from broadcaster import Broadcast

# Enable INFO logging to see any reconnection messages
logging.basicConfig(
    level=logging.INFO,
    format='%(levelname)s - %(name)s - %(message)s'
)

async def main():
    # Connect to Sentinel cluster using Stream backend
    broadcast = Broadcast(
        "redis-stream+sentinel://localhost:26379,localhost:26380,localhost:26381/mymaster"
    )
    
    await broadcast.connect()
    print("✓ Connected to Redis Sentinel cluster (Stream backend)")
    
    # Identify current master
    result = subprocess.run(
        ["docker", "exec", "redis-sentinel-2", "redis-cli", "-p", "26380", 
         "SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"],
        capture_output=True, text=True, timeout=5
    )
    master_info = result.stdout.strip().split('\n')
    master_port = master_info[1] if len(master_info) > 1 else "unknown"
    print(f"✓ Current master: 127.0.0.1:{master_port}")
    
    # Map port to container name
    port_to_container = {
        "6380": "redis-sentinel-1",
        "6381": "redis-sentinel-2", 
        "6382": "redis-sentinel-3"
    }
    master_container = port_to_container.get(master_port, "redis-sentinel-1")
    
    # Start subscriber
    async with broadcast.subscribe("test-stream") as subscriber:
        print("✓ Subscribed to test-stream")
        
        # Send initial messages
        print("\n--- Phase 1: Before failover ---")
        for i in range(3):
            await broadcast.publish("test-stream", f"message-{i}")
            event = await asyncio.wait_for(subscriber.get(), timeout=2.0)
            print(f"✓ Sent and received: {event.message}")
            await asyncio.sleep(0.5)
        
        # Kill the master
        print(f"\n--- Killing master: {master_container} (port {master_port}) ---")
        subprocess.run(["docker", "stop", master_container], 
                      capture_output=True, timeout=10)
        print(f"✗ Master {master_container} stopped")
        
        # Wait for Sentinel to detect failure and elect new master
        print("⏳ Waiting 8 seconds for Sentinel failover...")
        await asyncio.sleep(8)
        
        # Check new master
        result = subprocess.run(
            ["docker", "exec", "redis-sentinel-2", "redis-cli", "-p", "26380",
             "SENTINEL", "GET-MASTER-ADDR-BY-NAME", "mymaster"],
            capture_output=True, text=True, timeout=5
        )
        new_master_info = result.stdout.strip().split('\n')
        new_master_port = new_master_info[1] if len(new_master_info) > 1 else "unknown"
        print(f"✓ New master: 127.0.0.1:{new_master_port}")
        
        # Try to continue streaming after failover
        print("\n--- Phase 2: After failover ---")
        errors = []
        success_count = 0
        
        for i in range(5):
            try:
                print(f"\nAttempt {i+1}:")
                await broadcast.publish("test-stream", f"after-failover-{i}")
                print(f"  ✓ Published: after-failover-{i}")
                
                event = await asyncio.wait_for(subscriber.get(), timeout=3.0)
                print(f"  ✓ Received: {event.message}")
                success_count += 1
                await asyncio.sleep(0.5)
                
            except Exception as e:
                error_msg = f"Attempt {i+1} failed: {type(e).__name__}: {e}"
                print(f"  ✗ {error_msg}")
                errors.append(error_msg)
        
        # Restart the stopped master (for cleanup)
        print(f"\n--- Restarting {master_container} ---")
        subprocess.run(["docker", "start", master_container],
                      capture_output=True, timeout=10)
        print(f"✓ {master_container} restarted")
    
    await broadcast.disconnect()
    
    # Summary
    print("\n" + "="*60)
    print("SUMMARY")
    print("="*60)
    print(f"Messages before failover: 3/3 ✓")
    print(f"Messages after failover: {success_count}/5 {'✓' if success_count == 5 else '✗'}")
    
    if errors:
        print(f"\nErrors encountered: {len(errors)}")
        for error in errors:
            print(f"  - {error}")
        print("\n⚠️  FAILOVER NOT TRANSPARENT - Errors occurred")
    else:
        print("\n✓ FAILOVER WAS TRANSPARENT - No errors!")
    
    return len(errors) == 0

if __name__ == "__main__":
    try:
        success = asyncio.run(main())
        exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
        exit(1)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        exit(1)
