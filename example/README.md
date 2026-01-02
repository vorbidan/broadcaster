# Examples

This directory contains example applications and test scripts for broadcaster.

## Setup

Install python dependencies in your virtualenv:

```bash
pip install -r requirements.txt
```

You can also install broadcaster locally using `pip install -e .` from the project root.

## Web Application Example

Run the example web application with memory as backend:

```bash
uvicorn example.app:app
```

To run with different backends, set the `BROADCAST_URL` env and start the docker services:

| Backend         | Env                                                          | Service command              |
| --------------- | ------------------------------------------------------------ | ---------------------------- |
| kafka           | `export BROADCAST_URL=kafka://localhost:9092`                | `docker-compose up kafka`    |
| redis           | `export BROADCAST_URL=redis://localhost:6379`                | `docker-compose up redis`    |
| postgres        | `export BROADCAST_URL=postgres://localhost:5432/broadcaster` | `docker-compose up postgres` |
| redis-sentinel  | `export BROADCAST_URL=redis+sentinel://localhost:26379,localhost:26380,localhost:26381/mymaster` | `docker-compose up redis-sentinel-1 redis-sentinel-2 redis-sentinel-3` |

## Redis Sentinel Examples

### Basic Sentinel Usage

`example_sentinel.py` - Simple example showing basic Redis Sentinel connectivity:

```bash
# Start Sentinel cluster
docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# Run example
python example/example_sentinel.py
```

### Sentinel with SSL/TLS

`example_sentinel_ssl.py` - Example showing SSL/TLS configuration for Sentinel:

```bash
python example/example_sentinel_ssl.py
```

### Sentinel with Redis Streams

`example_sentinel_stream.py` - Example using Redis Streams with Sentinel for message persistence:

```bash
# Start Sentinel cluster
docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# Run example
python example/example_sentinel_stream.py
```

Redis Streams provide:
- Message persistence
- Consumer groups
- Message acknowledgment
- Automatic failover via Sentinel

## Failover Testing Examples

These examples demonstrate transparent failover handling during Redis Sentinel master re-election.

### PubSub Failover Test

`example_pubsub_failover.py` - Demonstrates PubSub resilience during Sentinel failover:

```bash
# Start Sentinel cluster
docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# Run test
python example/example_pubsub_failover.py
```

This test:
1. Connects to Sentinel and subscribes to a channel
2. Publishes 3 messages before failover
3. Kills the master Redis instance to trigger failover
4. Automatically reconnects with exponential backoff
5. Publishes 5 more messages after failover
6. Verifies all messages are received transparently

### Stream Failover Test

`example_stream_failover.py` - Demonstrates Redis Streams resilience during Sentinel failover:

```bash
# Start Sentinel cluster
docker-compose up -d redis-sentinel-1 redis-sentinel-2 redis-sentinel-3

# Run test
python example/example_stream_failover.py
```

This test follows the same pattern as PubSub but uses Redis Streams for message persistence and consumer groups.

**Key Features Demonstrated:**
- Automatic reconnection with exponential backoff (0.5s, 1s, 2s, 4s, 8s, max 10s)
- Transparent failover - no visible errors to the application
- Automatic resubscription to channels (PubSub) or consumer reconnection (Streams)
- Clear logging of reconnection attempts and success
