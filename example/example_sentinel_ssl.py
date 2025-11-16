#!/usr/bin/env python3
"""
Example demonstrating Redis Sentinel backend with SSL/TLS support.

This example shows various ways to configure SSL for Redis Sentinel connections.
"""

import asyncio
import ssl
from broadcaster import Broadcast
from broadcaster.backends.redis_sentinel import RedisSentinelBackend


async def example_ssl_url_scheme():
    """Example using rediss+sentinel:// URL scheme for SSL."""
    print("=== SSL URL Scheme ===")
    
    # Use rediss+sentinel:// scheme (note the double 's')
    url = "rediss+sentinel://sentinel1:26379,sentinel2:26379/mymaster"
    
    print(f"Connecting with SSL URL: {url}")
    print("This enables SSL for connections to Redis master")
    print("(Sentinels would need to be configured separately if they also use SSL)")


async def example_ssl_with_certificates():
    """Example with SSL certificate files."""
    print("\n=== SSL with Certificate Files ===")
    
    url = (
        "rediss+sentinel://sentinel1:26379,sentinel2:26379/mymaster"
        "?ssl_certfile=/path/to/client-cert.pem"
        "&ssl_keyfile=/path/to/client-key.pem"
        "&ssl_ca_certs=/path/to/ca-cert.pem"
        "&ssl_check_hostname=true"
    )
    
    print("URL with SSL parameters:")
    print("  - ssl_certfile: Client certificate")
    print("  - ssl_keyfile: Client private key")
    print("  - ssl_ca_certs: CA certificate bundle")
    print("  - ssl_check_hostname: Verify hostname")


async def example_ssl_programmatic():
    """Example with programmatic SSL context."""
    print("\n=== Programmatic SSL Configuration ===")
    
    # Create custom SSL context
    ssl_context = ssl.create_default_context()
    
    # Optional: Load client certificate
    # ssl_context.load_cert_chain(
    #     certfile="/path/to/client-cert.pem",
    #     keyfile="/path/to/client-key.pem"
    # )
    
    # Optional: Load CA bundle
    # ssl_context.load_verify_locations(cafile="/path/to/ca-cert.pem")
    
    # Optional: Disable hostname checking (not recommended for production)
    # ssl_context.check_hostname = False
    # ssl_context.verify_mode = ssl.CERT_NONE
    
    backend = RedisSentinelBackend(
        sentinels=[("sentinel1", 26379), ("sentinel2", 26379)],
        service_name="mymaster",
        ssl=True,
        ssl_context=ssl_context
    )
    
    print("Created backend with custom SSL context")
    print(f"  SSL enabled: {backend._connection_kwargs.get('ssl')}")
    print(f"  SSL context: {backend._connection_kwargs.get('ssl_context')}")


async def example_ssl_sentinel_and_redis():
    """Example with SSL for both Sentinel nodes and Redis master."""
    print("\n=== SSL for Sentinel and Redis ===")
    
    # Create SSL context for Sentinel connections
    sentinel_ssl_context = ssl.create_default_context()
    # sentinel_ssl_context.load_verify_locations(cafile="/path/to/sentinel-ca.pem")
    
    # Create SSL context for Redis master connections
    redis_ssl_context = ssl.create_default_context()
    # redis_ssl_context.load_verify_locations(cafile="/path/to/redis-ca.pem")
    
    backend = RedisSentinelBackend(
        sentinels=[("sentinel1", 26379), ("sentinel2", 26379)],
        service_name="mymaster",
        # SSL for Sentinel nodes
        sentinel_kwargs={
            "ssl": True,
            "ssl_context": sentinel_ssl_context
        },
        # SSL for Redis master
        ssl=True,
        ssl_context=redis_ssl_context
    )
    
    print("Created backend with separate SSL contexts:")
    print("  - Sentinel SSL: Custom context for sentinel nodes")
    print("  - Redis SSL: Custom context for master connection")


async def example_ssl_simple():
    """Example with simple SSL (default certificates)."""
    print("\n=== Simple SSL (Default Certificates) ===")
    
    # Simplest SSL setup - uses system default certificates
    backend = RedisSentinelBackend(
        sentinels=[("sentinel1", 26379), ("sentinel2", 26379)],
        service_name="mymaster",
        ssl=True  # Uses default SSL context
    )
    
    print("Created backend with default SSL context")
    print("This uses system certificate store for validation")


async def example_ssl_self_signed():
    """Example for development with self-signed certificates."""
    print("\n=== Self-Signed Certificates (Development Only) ===")
    
    # Create SSL context that accepts self-signed certificates
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    backend = RedisSentinelBackend(
        sentinels=[("localhost", 26379)],
        service_name="mymaster",
        ssl=True,
        ssl_context=ssl_context
    )
    
    print("⚠️  WARNING: This disables certificate verification!")
    print("Only use this for development/testing with self-signed certs")
    print("Never use CERT_NONE in production!")


async def main():
    """Run all SSL examples."""
    print("Redis Sentinel SSL/TLS Examples")
    print("=" * 60)
    print()
    print("These examples demonstrate various SSL configuration options.")
    print("Note: These are examples only and won't actually connect without")
    print("a properly configured SSL-enabled Redis Sentinel setup.")
    print()
    
    await example_ssl_url_scheme()
    await example_ssl_with_certificates()
    await example_ssl_programmatic()
    await example_ssl_sentinel_and_redis()
    await example_ssl_simple()
    await example_ssl_self_signed()
    
    print("\n" + "=" * 60)
    print("✅ All SSL configuration examples shown")
    print()
    print("Key Points:")
    print("1. Use rediss+sentinel:// for SSL (note double 's')")
    print("2. Pass SSL parameters via URL or programmatically")
    print("3. Can configure separate SSL for Sentinel and Redis")
    print("4. Default SSL context uses system certificate store")
    print("5. Never disable certificate verification in production")


if __name__ == "__main__":
    asyncio.run(main())
