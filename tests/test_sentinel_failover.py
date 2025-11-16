"""
Tests for Redis Sentinel failover and master re-election.

These tests require the sentinel setup from docker-compose.yaml.
Run with: docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3
"""
import asyncio
import pytest
import subprocess

from broadcaster import Broadcast
from broadcaster.backends.redis_sentinel import RedisSentinelBackend


@pytest.mark.asyncio
async def test_sentinel_failover_reconnection():
    """Test that the backend reconnects after master failover.
    
    This test simulates a master failure by:
    1. Connecting to the current master
    2. Publishing/subscribing successfully
    3. Stopping the master container
    4. Verifying automatic reconnection
    5. Continuing pub/sub operations
    """
    url = "redis+sentinel://localhost:26379,localhost:26380,localhost:26381/mymaster"
    
    async with Broadcast(url) as broadcast:
        backend = broadcast._backend
        
        async with broadcast.subscribe("failover-test") as subscriber:
            # Pre-failover: verify normal operation
            await broadcast.publish("failover-test", "before")
            event = await asyncio.wait_for(subscriber.get(), timeout=2.0)
            assert event.message == "before"
            
            # Trigger failover by stopping master
            subprocess.run(["docker", "stop", "redis-sentinel-1"], 
                          capture_output=True, timeout=10)
            
            # Wait for reconnection (should happen within 15 seconds)
            reconnected = False
            for i in range(15):
                await asyncio.sleep(1)
                try:
                    test_msg = f"reconnect-{i}"
                    await broadcast.publish("failover-test", test_msg)
                    
                    # Consume the message
                    event = await asyncio.wait_for(subscriber.get(), timeout=1.0)
                    if event.message == test_msg:
                        reconnected = True
                        break
                except:
                    pass
            
            assert reconnected, "Failed to reconnect within 15 seconds"
            
            # Post-failover: verify continued operation
            await broadcast.publish("failover-test", "after")
            event = await asyncio.wait_for(subscriber.get(), timeout=3.0)
            assert event.message == "after"
            
            # Cleanup: restart master
            subprocess.run(["docker", "start", "redis-sentinel-1"], 
                          capture_output=True, timeout=10)


@pytest.mark.asyncio
async def test_sentinel_reconnection_mechanism():
    """Test that backend has reconnection logic for PubSub failover handling."""
    
    backend = RedisSentinelBackend(
        sentinels=[("localhost", 26379), ("localhost", 26380), ("localhost", 26381)],
        service_name="mymaster"
    )
    
    async with Broadcast(backend=backend) as broadcast:
        # Subscribe to channel
        async with broadcast.subscribe("reconnect-test") as subscriber:
            # Publish and receive initial message
            await broadcast.publish("reconnect-test", "initial")
            event = await subscriber.get()
            assert event.message == "initial"
            
            # Verify backend has reconnection logic for PubSub
            # (needed because PubSub connections don't automatically reconnect during failover)
            assert hasattr(backend, '_reconnecting')
            assert isinstance(backend._reconnecting, asyncio.Lock)


@pytest.mark.asyncio  
async def test_sentinel_tracks_subscribed_channels():
    """Test that backend tracks subscribed channels for reconnection."""
    
    backend = RedisSentinelBackend(
        sentinels=[("localhost", 26379), ("localhost", 26380), ("localhost", 26381)],
        service_name="mymaster"
    )
    
    async with Broadcast(backend=backend) as broadcast:
        # Initially no channels
        assert len(backend._subscribed_channels) == 0
        
        async with broadcast.subscribe("channel1") as sub1:
            # Should track channel1
            assert "channel1" in backend._subscribed_channels
            
            async with broadcast.subscribe("channel2") as sub2:
                # Should track both channels
                assert "channel1" in backend._subscribed_channels
                assert "channel2" in backend._subscribed_channels
            
            # channel2 should be untracked after context exit
            # (Note: actual unsubscribe happens in Broadcast cleanup)
        
        # Verify channels can be used
        async with broadcast.subscribe("test-channel") as subscriber:
            await broadcast.publish("test-channel", "test")
            event = await subscriber.get()
            assert event.message == "test"


if __name__ == "__main__":
    """
    Run failover test manually.
    
    Setup:
        docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3
    
    Run:
        python -m pytest tests/test_sentinel_failover.py::test_sentinel_failover_reconnection -v
    
    Or use the standalone test scripts:
        python test_failover_simple.py      # Automated test
        python test_failover_interactive.py # Interactive test
    """
    print("Use pytest to run these tests:")
    print("  pytest tests/test_sentinel_failover.py -v")
    print("\nOr use standalone scripts:")
    print("  python test_failover_simple.py")
    print("  python test_failover_interactive.py")
