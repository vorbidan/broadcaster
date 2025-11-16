from __future__ import annotations

import asyncio
import typing

import pytest
from redis import asyncio as redis

from broadcaster import Broadcast, BroadcastBackend, Event
from broadcaster.backends.kafka import KafkaBackend
from broadcaster.backends.redis import RedisBackend
from broadcaster.backends.redis_sentinel import RedisSentinelBackend, RedisSentinelStreamBackend


class CustomBackend(BroadcastBackend):
    def __init__(self, url: str):
        self._subscribed: set[str] = set()

    async def connect(self) -> None:
        self._published: asyncio.Queue[Event] = asyncio.Queue()

    async def disconnect(self) -> None:
        pass

    async def subscribe(self, channel: str) -> None:
        self._subscribed.add(channel)

    async def unsubscribe(self, channel: str) -> None:
        self._subscribed.remove(channel)

    async def publish(self, channel: str, message: typing.Any) -> None:
        event = Event(channel=channel, message=message)
        await self._published.put(event)

    async def next_published(self) -> Event:
        while True:
            event = await self._published.get()
            if event.channel in self._subscribed:
                return event


@pytest.mark.asyncio
async def test_memory():
    async with Broadcast("memory://") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis():
    async with Broadcast("redis://localhost:6379") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_configured_client():
    backend = RedisBackend(conn=redis.Redis.from_url("redis://localhost:6379"))
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_requires_url_or_connection():
    with pytest.raises(AssertionError, match="conn must be provided if url is not"):
        RedisBackend()


@pytest.mark.asyncio
async def test_redis_stream():
    async with Broadcast("redis-stream://localhost:6379") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"
        async with broadcast.subscribe("chatroom1") as subscriber:
            await broadcast.publish("chatroom1", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom1"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_postgres():
    async with Broadcast("postgres://postgres:postgres@localhost:5432/broadcaster") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_kafka():
    async with Broadcast("kafka://localhost:9092") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_kafka_multiple_urls():
    async with Broadcast(backend=KafkaBackend(urls=["kafka://localhost:9092", "kafka://localhost:9092"])) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_custom():
    backend = CustomBackend("")
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_unknown_backend():
    with pytest.raises(ValueError, match="Unsupported backend"):
        async with Broadcast(url="unknown://"):
            pass


@pytest.mark.asyncio
async def test_needs_url_or_backend():
    with pytest.raises(AssertionError, match="Either `url` or `backend` must be provided."):
        Broadcast()


@pytest.mark.asyncio
async def test_redis_sentinel_url():
    async with Broadcast("redis+sentinel://localhost:26379/mymaster") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_sentinel_configured_backend():
    backend = RedisSentinelBackend(
        sentinels=[("localhost", 26379)],
        service_name="mymaster"
    )
    async with Broadcast(backend=backend) as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_sentinel_url_with_params():
    async with Broadcast("redis+sentinel://localhost:26379,localhost:26380/mymaster?db=0") as broadcast:
        async with broadcast.subscribe("chatroom") as subscriber:
            await broadcast.publish("chatroom", "hello")
            event = await subscriber.get()
            assert event.channel == "chatroom"
            assert event.message == "hello"


@pytest.mark.asyncio
async def test_redis_sentinel_requires_sentinels():
    with pytest.raises(AssertionError, match="sentinels must be provided if url is not"):
        RedisSentinelBackend(service_name="mymaster")


@pytest.mark.asyncio
async def test_redis_sentinel_requires_service_name():
    with pytest.raises(AssertionError, match="service_name must be provided if url is not"):
        RedisSentinelBackend(sentinels=[("localhost", 26379)])


@pytest.mark.asyncio
async def test_redis_sentinel_url_parsing():
    backend = RedisSentinelBackend("redis+sentinel://host1:26379,host2:26380/mymaster?db=1&password=secret")
    assert backend._sentinels == [("host1", 26379), ("host2", 26380)]
    assert backend._service_name == "mymaster"
    assert backend._connection_kwargs["db"] == 1
    assert backend._connection_kwargs["password"] == "secret"


@pytest.mark.asyncio
async def test_redis_sentinel_ssl_url_parsing():
    backend = RedisSentinelBackend("rediss+sentinel://host1:26379,host2:26380/mymaster?ssl_certfile=/path/to/cert.pem&ssl_check_hostname=true")
    assert backend._sentinels == [("host1", 26379), ("host2", 26380)]
    assert backend._service_name == "mymaster"
    assert backend._connection_kwargs["ssl"] is True
    assert backend._connection_kwargs["ssl_certfile"] == "/path/to/cert.pem"
    assert backend._connection_kwargs["ssl_check_hostname"] is True


@pytest.mark.asyncio
async def test_redis_sentinel_ssl_programmatic():
    import ssl
    ssl_context = ssl.create_default_context()
    backend = RedisSentinelBackend(
        sentinels=[("localhost", 26379)],
        service_name="mymaster",
        ssl=True,
        ssl_context=ssl_context
    )
    assert backend._sentinels == [("localhost", 26379)]
    assert backend._service_name == "mymaster"
    assert backend._connection_kwargs["ssl"] is True
    assert backend._connection_kwargs["ssl_context"] == ssl_context


@pytest.mark.asyncio
async def test_redis_sentinel_stream_url():
    async with Broadcast("redis-stream+sentinel://localhost:26379/mymaster") as broadcast:
        assert isinstance(broadcast._backend, RedisSentinelStreamBackend)


@pytest.mark.asyncio
async def test_redis_sentinel_stream_configured_backend():
    backend = RedisSentinelStreamBackend(
        sentinels=[("localhost", 26379)],
        service_name="mymaster"
    )
    async with Broadcast(backend=backend) as broadcast:
        assert isinstance(broadcast._backend, RedisSentinelStreamBackend)


@pytest.mark.asyncio
async def test_redis_sentinel_stream_url_parsing():
    backend = RedisSentinelStreamBackend("redis-stream+sentinel://host1:26379,host2:26380/mymaster?db=1&password=secret")
    assert backend._sentinels == [("host1", 26379), ("host2", 26380)]
    assert backend._service_name == "mymaster"
    assert backend._connection_kwargs["db"] == 1
    assert backend._connection_kwargs["password"] == "secret"


@pytest.mark.asyncio
async def test_redis_sentinel_stream_ssl_url_parsing():
    backend = RedisSentinelStreamBackend("rediss-stream+sentinel://host1:26379,host2:26380/mymaster?ssl_certfile=/path/to/cert.pem")
    assert backend._sentinels == [("host1", 26379), ("host2", 26380)]
    assert backend._service_name == "mymaster"
    assert backend._connection_kwargs["ssl"] is True
    assert backend._connection_kwargs["ssl_certfile"] == "/path/to/cert.pem"


