from __future__ import annotations

import asyncio
import logging
import typing

from redis import asyncio as redis

from .._base import Event
from .base import BroadcastBackend

logger = logging.getLogger("broadcaster.redis_sentinel")


class RedisSentinelBackend(BroadcastBackend):
    """Redis Sentinel backend for high availability deployments.
    
    Connects to Redis master through Sentinel for both publishing and subscribing.
    
    Note: The redis-py Sentinel client automatically handles most failover scenarios
    by discovering the new master through Sentinel. The reconnection logic in this
    backend serves as a fallback for edge cases where the automatic discovery fails
    (e.g., network partitions, pubsub connection drops, etc.).
    """
    
    _conn: redis.Redis
    _sentinel: redis.Sentinel | None

    def __init__(
        self,
        url: str | None = None,
        *,
        sentinels: list[tuple[str, int]] | None = None,
        service_name: str | None = None,
        sentinel_kwargs: dict[str, typing.Any] | None = None,
        **kwargs: typing.Any,
    ):
        """Initialize Redis Sentinel backend.
        
        Args:
            url: URL in format redis+sentinel://host1:port1,host2:port2/service_name?param=value
                 or rediss+sentinel:// for SSL connections
            sentinels: List of (host, port) tuples for sentinel nodes
            service_name: Name of the Redis service/master to connect to
            sentinel_kwargs: Additional kwargs for Sentinel client (e.g., ssl=True for sentinel SSL)
            **kwargs: Additional kwargs passed to master_for() (e.g., password, db, ssl, ssl_certfile)
            
        SSL Support:
            - Use rediss+sentinel:// scheme for SSL
            - URL params: ssl_certfile, ssl_keyfile, ssl_ca_certs, ssl_cert_reqs, ssl_check_hostname
            - Or pass ssl=True and SSL context via kwargs when using programmatic init
        """
        if url is not None:
            # Parse redis+sentinel://host1:port1,host2:port2/service_name?db=0&password=xxx
            self._sentinels, self._service_name, self._connection_kwargs = self._parse_sentinel_url(url)
            self._sentinel_kwargs = sentinel_kwargs or {}
        else:
            assert sentinels is not None, "sentinels must be provided if url is not"
            assert service_name is not None, "service_name must be provided if url is not"
            self._sentinels = sentinels
            self._service_name = service_name
            self._sentinel_kwargs = sentinel_kwargs or {}
            self._connection_kwargs = kwargs

        self._sentinel: redis.Sentinel | None = None
        self._conn: redis.Redis | None = None
        self._pubsub: redis.client.PubSub | None = None
        self._ready = asyncio.Event()
        self._queue: asyncio.Queue[Event] = asyncio.Queue()
        self._listener: asyncio.Task[None] | None = None
        self._subscribed_channels: set[str] = set()  # Track subscribed channels
        self._reconnecting = asyncio.Lock()  # Prevent concurrent reconnection attempts

    @staticmethod
    def _parse_sentinel_url(url: str) -> tuple[list[tuple[str, int]], str, dict[str, typing.Any]]:
        """Parse redis+sentinel URL into components.
        
        Supports both redis+sentinel:// and rediss+sentinel:// (SSL) schemes.
        SSL parameters: ssl_certfile, ssl_keyfile, ssl_ca_certs, ssl_cert_reqs, ssl_check_hostname
        """
        from urllib.parse import parse_qs, urlparse
        
        parsed = urlparse(url)
        
        # Check for SSL scheme
        use_ssl = parsed.scheme == "rediss+sentinel"
        
        # Parse sentinel hosts from netloc: host1:port1,host2:port2
        if not parsed.netloc:
            raise ValueError("Sentinel URL must contain host:port pairs")
        
        sentinels = []
        for host_port in parsed.netloc.split(","):
            host_port = host_port.strip()
            if ":" in host_port:
                host, port_str = host_port.rsplit(":", 1)
                sentinels.append((host, int(port_str)))
            else:
                # Default sentinel port
                sentinels.append((host_port, 26379))
        
        # Service name from path: /mymaster
        service_name = parsed.path.lstrip("/")
        if not service_name:
            raise ValueError("Sentinel URL must contain service name in path (e.g., /mymaster)")
        
        # Parse query params for connection kwargs
        connection_kwargs: dict[str, typing.Any] = {}
        if parsed.query:
            params = parse_qs(parsed.query)
            for key, value in params.items():
                # Take first value if list
                val = value[0] if isinstance(value, list) else value
                # Convert db to int
                if key == "db":
                    connection_kwargs[key] = int(val)
                # Convert boolean strings
                elif key in ("ssl_check_hostname",):
                    connection_kwargs[key] = val.lower() in ("true", "1", "yes")
                # SSL-related parameters
                elif key in ("ssl_certfile", "ssl_keyfile", "ssl_ca_certs", "ssl_cert_reqs"):
                    connection_kwargs[key] = val
                else:
                    connection_kwargs[key] = val
        
        # If using SSL scheme, ensure ssl=True is set
        if use_ssl:
            connection_kwargs["ssl"] = True
        
        return sentinels, service_name, connection_kwargs

    async def connect(self) -> None:
        """Connect to Redis master through Sentinel."""
        await self._connect_sentinel()
        self._listener = asyncio.create_task(self._pubsub_listener())

    async def _connect_sentinel(self) -> None:
        """Establish connection to Sentinel and get master connection."""
        self._sentinel = redis.Sentinel(
            self._sentinels,
            sentinel_kwargs=self._sentinel_kwargs,
        )
        
        # Get master connection for both pub and sub
        self._conn = self._sentinel.master_for(
            self._service_name,
            **self._connection_kwargs,
        )
        
        # Initialize pubsub
        self._pubsub = self._conn.pubsub()
        await self._pubsub.connect()  # type: ignore[no-untyped-call]

    async def disconnect(self) -> None:
        """Disconnect from Redis and Sentinel."""
        # Cancel listener task
        if self._listener is not None:
            self._listener.cancel()
            try:
                await self._listener
            except asyncio.CancelledError:
                pass
            except Exception as e:
                # Listener might have crashed, log but continue cleanup
                logger.warning(f"Listener task ended with error: {type(e).__name__}: {e}")
        
        # Close connections
        if self._pubsub is not None:
            try:
                if hasattr(self._pubsub, 'aclose'):
                    await self._pubsub.aclose()  # type: ignore[no-untyped-call]
                else:
                    await self._pubsub.close()
            except Exception as e:
                logger.warning(f"Error closing pubsub connection: {type(e).__name__}: {e}")
        
        if self._conn is not None:
            try:
                if hasattr(self._conn, 'aclose'):
                    await self._conn.aclose()
                else:
                    await self._conn.close()
            except Exception as e:
                logger.warning(f"Error closing connection: {type(e).__name__}: {e}")
        
        # Sentinel doesn't need explicit close in redis-py async

    async def subscribe(self, channel: str) -> None:
        """Subscribe to a channel."""
        self._subscribed_channels.add(channel)  # Track for reconnection
        self._ready.set()
        if self._pubsub is not None:
            await self._pubsub.subscribe(channel)

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a channel."""
        self._subscribed_channels.discard(channel)  # Remove from tracked channels
        if self._pubsub is not None:
            try:
                await self._pubsub.unsubscribe(channel)
            except Exception as e:
                # If unsubscribe fails (e.g., during reconnection), log but don't crash
                # The channel is already removed from tracked channels
                logger.warning(f"Failed to unsubscribe from {channel}: {type(e).__name__}: {e}")

    async def publish(self, channel: str, message: typing.Any) -> None:
        """Publish a message to a channel.
        
        Note: Sentinel connection pool automatically handles master failover,
        so publish will transparently reconnect to the new master.
        """
        if self._conn is not None:
            await self._conn.publish(channel, message)

    async def next_published(self) -> Event:
        """Get the next published event from the queue."""
        return await self._queue.get()

    async def _pubsub_listener(self) -> None:
        """Listen for pubsub messages.
        
        Handles connection errors by triggering reconnection. While redis-py's
        Sentinel connection pool handles master failover for regular commands,
        the persistent PubSub connection may break during failover and needs
        explicit reconnection logic.
        """
        while True:
            try:
                await self._ready.wait()
                
                if self._pubsub is None:
                    await asyncio.sleep(0.1)
                    continue
                
                async for message in self._pubsub.listen():
                    if message["type"] == "message":
                        event = Event(
                            channel=message["channel"].decode(),
                            message=message["data"].decode(),
                        )
                        await self._queue.put(event)

                # When no channel subscribed, clear the event
                self._ready.clear()
                
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # PubSub connection broken - attempt reconnection with retries
                logger.error(f"PubSub listener error: {type(e).__name__}: {e}")
                logger.info("Attempting to reconnect PubSub connection...")
                
                # Keep retrying with exponential backoff until successful
                # Faster initial attempts (0.5s, 1s, 2s, 4s, 8s...) to minimize downtime
                attempt = 0
                while True:
                    try:
                        # More aggressive initial backoff: 0.5 * 2^attempt, max 10s
                        delay = min(0.5 * (2 ** attempt), 10)
                        if attempt > 0:  # Don't delay on first attempt
                            logger.info(f"Retrying in {delay:.1f}s (attempt {attempt + 1})...")
                            await asyncio.sleep(delay)
                        else:
                            logger.info(f"Attempting reconnection (attempt {attempt + 1})...")
                        
                        await self._reconnect()
                        logger.info(f"PubSub reconnection successful after {attempt + 1} attempt(s)")
                        break  # Exit retry loop and resume listening
                    except asyncio.CancelledError:
                        raise
                    except Exception as reconnect_error:
                        attempt += 1
                        logger.warning(f"Reconnection attempt {attempt} failed: {type(reconnect_error).__name__}: {reconnect_error}")
                        # Continue to next attempt (infinite retry)

    async def _reconnect(self) -> None:
        """Reconnect to Redis Sentinel after connection failure.
        
        This recreates both the connection and PubSub, allowing Sentinel
        to discover the new master, and resubscribes to all channels.
        """
        async with self._reconnecting:
            logger.info("Reconnecting to Redis Sentinel...")
            
            # Close old pubsub connection
            if self._pubsub is not None:
                try:
                    if hasattr(self._pubsub, 'aclose'):
                        await self._pubsub.aclose()  # type: ignore[no-untyped-call]
                    else:
                        await self._pubsub.close()
                except Exception:
                    pass  # Ignore errors closing broken connection
            
            # Close old connection
            if self._conn is not None:
                try:
                    if hasattr(self._conn, 'aclose'):
                        await self._conn.aclose()
                    else:
                        await self._conn.close()
                except Exception:
                    pass  # Ignore errors closing broken connection
            
            # Get new master connection (Sentinel will discover current master)
            if self._sentinel is not None:
                self._conn = self._sentinel.master_for(
                    self._service_name,
                    **self._connection_kwargs,
                )
                
                # Create new pubsub connection
                self._pubsub = self._conn.pubsub()
                await self._pubsub.connect()  # type: ignore[no-untyped-call]
                
                # Resubscribe to all channels
                if self._subscribed_channels:
                    logger.info(f"Resubscribing to {len(self._subscribed_channels)} channels: {self._subscribed_channels}")
                    for channel in self._subscribed_channels:
                        await self._pubsub.subscribe(channel)
                
                logger.info("Reconnection complete")


StreamMessageType = typing.Tuple[bytes, typing.Tuple[typing.Tuple[bytes, typing.Dict[bytes, bytes]]]]


class RedisSentinelStreamBackend(BroadcastBackend):
    """Redis Sentinel Stream backend for high availability deployments.
    
    Uses Redis Streams for message delivery with Sentinel for automatic failover.
    """

    def __init__(
        self,
        url: str | None = None,
        *,
        sentinels: list[tuple[str, int]] | None = None,
        service_name: str | None = None,
        sentinel_kwargs: dict[str, typing.Any] | None = None,
        **kwargs: typing.Any,
    ):
        """Initialize Redis Sentinel Stream backend.
        
        Args:
            url: URL in format redis-stream+sentinel://host1:port1,host2:port2/service_name
                 or rediss-stream+sentinel:// for SSL connections
            sentinels: List of (host, port) tuples for sentinel nodes
            service_name: Name of the Redis service/master to connect to
            sentinel_kwargs: Additional kwargs for Sentinel client
            **kwargs: Additional kwargs passed to master_for()
        """
        if url is not None:
            # Replace redis-stream+sentinel with redis+sentinel for parsing
            parse_url = url.replace("redis-stream+sentinel", "redis+sentinel", 1)
            parse_url = parse_url.replace("rediss-stream+sentinel", "rediss+sentinel", 1)
            self._sentinels, self._service_name, self._connection_kwargs = RedisSentinelBackend._parse_sentinel_url(parse_url)
            self._sentinel_kwargs = sentinel_kwargs or {}
        else:
            assert sentinels is not None, "sentinels must be provided if url is not"
            assert service_name is not None, "service_name must be provided if url is not"
            self._sentinels = sentinels
            self._service_name = service_name
            self._sentinel_kwargs = sentinel_kwargs or {}
            self._connection_kwargs = kwargs

        self.streams: dict[bytes | str | memoryview, int | bytes | str | memoryview] = {}
        self._ready = asyncio.Event()
        self._sentinel: redis.Sentinel | None = None
        self._producer: redis.Redis | None = None
        self._consumer: redis.Redis | None = None
        self._reconnecting = asyncio.Lock()  # Prevent concurrent reconnection attempts

    async def connect(self) -> None:
        """Connect to Redis master through Sentinel for producer and consumer."""
        await self._connect_sentinel()

    async def _connect_sentinel(self) -> None:
        """Establish connection to Sentinel and get master connections."""
        self._sentinel = redis.Sentinel(
            self._sentinels,
            sentinel_kwargs=self._sentinel_kwargs,
        )
        
        # Get separate master connections for producer and consumer
        self._producer = self._sentinel.master_for(
            self._service_name,
            **self._connection_kwargs,
        )
        self._consumer = self._sentinel.master_for(
            self._service_name,
            **self._connection_kwargs,
        )

    async def disconnect(self) -> None:
        """Disconnect from Redis and Sentinel."""
        if self._producer is not None:
            if hasattr(self._producer, 'aclose'):
                await self._producer.aclose()
            else:
                await self._producer.close()
        
        if self._consumer is not None:
            if hasattr(self._consumer, 'aclose'):
                await self._consumer.aclose()
            else:
                await self._consumer.close()

    async def subscribe(self, channel: str) -> None:
        """Subscribe to a stream."""
        if self._consumer is None:
            return
            
        try:
            info = await self._consumer.xinfo_stream(channel)
            last_id = info["last-generated-id"]
        except redis.ResponseError:
            last_id = "0"
        self.streams[channel] = last_id
        self._ready.set()

    async def unsubscribe(self, channel: str) -> None:
        """Unsubscribe from a stream."""
        self.streams.pop(channel, None)

    async def publish(self, channel: str, message: typing.Any) -> None:
        """Publish a message to a stream.
        
        Note: Sentinel connection pool automatically handles master failover.
        """
        if self._producer is not None:
            await self._producer.xadd(channel, {"message": message})

    async def wait_for_messages(self) -> list[StreamMessageType]:
        """Wait for messages from subscribed streams.
        
        Handles connection errors with automatic reconnection using Sentinel
        to discover the new master.
        """
        await self._ready.wait()
        
        if self._consumer is None:
            await asyncio.sleep(0.1)
            return []
        
        # Keep trying with reconnection on errors
        attempt = 0
        reconnecting = False
        while True:
            try:
                messages = None
                while not messages:
                    messages = await self._consumer.xread(self.streams, count=1, block=100)
                # Successfully read messages - reset attempt counter and log if we were reconnecting
                if reconnecting:
                    logger.info("Stream consumer successfully reconnected and reading messages")
                return messages
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Connection error - attempt reconnection with backoff
                reconnecting = True
                if attempt == 0:
                    logger.error(f"Stream consumer error: {type(e).__name__}: {e}")
                    logger.info("Attempting to reconnect Stream consumer...")
                
                # More aggressive initial backoff: 0.5 * 2^attempt, max 10s
                delay = min(0.5 * (2 ** attempt), 10)
                if attempt > 0:
                    logger.info(f"Retrying in {delay:.1f}s (attempt {attempt + 1})...")
                    await asyncio.sleep(delay)
                
                try:
                    await self._reconnect_consumer()
                    # Connection recreated, will retry xread on next loop iteration
                    attempt += 1
                except Exception as reconnect_error:
                    attempt += 1
                    logger.warning(f"Reconnection failed: {type(reconnect_error).__name__}: {reconnect_error}")
                    # Continue to next attempt (infinite retry)

    async def _reconnect_consumer(self) -> None:
        """Reconnect the Stream consumer after connection failure."""
        async with self._reconnecting:
            logger.debug("Reconnecting Stream consumer to Redis Sentinel...")
            
            # Close old consumer connection
            if self._consumer is not None:
                try:
                    if hasattr(self._consumer, 'aclose'):
                        await self._consumer.aclose()
                    else:
                        await self._consumer.close()
                except Exception:
                    pass  # Ignore errors closing broken connection
            
            # Recreate Sentinel client to ensure fresh master discovery
            self._sentinel = redis.Sentinel(
                self._sentinels,
                sentinel_kwargs=self._sentinel_kwargs,
            )
            
            # Get new master connection (Sentinel will discover current master)
            self._consumer = self._sentinel.master_for(
                self._service_name,
                **self._connection_kwargs,
            )
            logger.debug("Stream consumer connection recreated")

    async def next_published(self) -> Event:
        """Get the next published event from streams."""
        messages = await self.wait_for_messages()
        stream, events = messages[0]
        _msg_id, message = events[0]
        self.streams[stream.decode("utf-8")] = _msg_id.decode("utf-8")
        return Event(
            channel=stream.decode("utf-8"),
            message=message.get(b"message", b"").decode("utf-8"),
        )
