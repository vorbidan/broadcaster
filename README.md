# Broadcaster

Broadcaster helps you develop realtime streaming functionality by providing
a simple broadcast API onto a number of different backend services.

It currently supports [Redis PUB/SUB](https://redis.io/topics/pubsub), [Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/), [Apache Kafka](https://kafka.apache.org/), and [Postgres LISTEN/NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html), plus a simple in-memory backend, that you can use for local development or during testing.

<img src="https://raw.githubusercontent.com/encode/broadcaster/master/docs/demo.gif" alt='WebSockets Demo'>

Here's a complete example of the backend code for a simple websocket chat app:

**app.py**

```python
# Requires: `starlette`, `uvicorn`, `jinja2`
# Run with `uvicorn example:app`
import anyio
from broadcaster import Broadcast
from starlette.applications import Starlette
from starlette.routing import Route, WebSocketRoute
from starlette.templating import Jinja2Templates


broadcast = Broadcast("redis://localhost:6379")
templates = Jinja2Templates("templates")


async def homepage(request):
    template = "index.html"
    context = {"request": request}
    return templates.TemplateResponse(template, context)


async def chatroom_ws(websocket):
    await websocket.accept()

    async with anyio.create_task_group() as task_group:
        # run until first is complete
        async def run_chatroom_ws_receiver() -> None:
            await chatroom_ws_receiver(websocket=websocket)
            task_group.cancel_scope.cancel()

        task_group.start_soon(run_chatroom_ws_receiver)
        await chatroom_ws_sender(websocket)


async def chatroom_ws_receiver(websocket):
    async for message in websocket.iter_text():
        await broadcast.publish(channel="chatroom", message=message)


async def chatroom_ws_sender(websocket):
    async with broadcast.subscribe(channel="chatroom") as subscriber:
        async for event in subscriber:
            await websocket.send_text(event.message)


routes = [
    Route("/", homepage),
    WebSocketRoute("/", chatroom_ws, name='chatroom_ws'),
]


app = Starlette(
    routes=routes, on_startup=[broadcast.connect], on_shutdown=[broadcast.disconnect],
)
```

The HTML template for the front end [is available here](https://github.com/encode/broadcaster/blob/master/example/templates/index.html), and is adapted from [Pieter Noordhuis's PUB/SUB demo](https://gist.github.com/pietern/348262).

## Requirements

Python 3.8+

## Installation

* `pip install broadcaster`
* `pip install broadcaster[redis]`
* `pip install broadcaster[postgres]`
* `pip install broadcaster[kafka]`

## Available backends

* `Broadcast('memory://')`
* `Broadcast("redis://localhost:6379")`
* `Broadcast("redis-stream://localhost:6379")`
* `Broadcast("redis+sentinel://localhost:26379/mymaster")`
* `Broadcast("redis-stream+sentinel://localhost:26379/mymaster")`
* `Broadcast("postgres://localhost:5432/broadcaster")`
* `Broadcast("kafka://localhost:9092")`

## Redis Sentinel Backend

The Redis Sentinel backend provides high availability for Redis deployments. It connects to the Redis master through Sentinel nodes and includes automatic health checking and reconnection.

```python
from broadcaster import Broadcast

# URL-based configuration
broadcast = Broadcast("redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster")

# With authentication (credentials are used for both Sentinel nodes and Redis master)
broadcast = Broadcast("redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster?username=user&password=secret")

# With separate credentials for Sentinel vs Redis (if needed)
broadcast = Broadcast("redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster?sentinel_username=sentinel_user&sentinel_password=sentinel_pass&username=redis_user&password=redis_pass")

# Additional parameters
broadcast = Broadcast("redis+sentinel://sentinel1:26379,sentinel2:26379/mymaster?db=0&username=user&password=secret")

# SSL/TLS support
broadcast = Broadcast("rediss+sentinel://sentinel1:26379,sentinel2:26379/mymaster?username=user&password=secret&ssl_check_hostname=false")

# SSL with certificate files
broadcast = Broadcast("rediss+sentinel://sentinel1:26379,sentinel2:26379/mymaster?ssl_certfile=/path/to/cert.pem&username=user&password=secret")

# Direct backend instantiation (for advanced configuration)
from broadcaster.backends.redis_sentinel import RedisSentinelBackend

backend = RedisSentinelBackend(
    sentinels=[("sentinel1", 26379), ("sentinel2", 26379)],
    service_name="mymaster",
    # Credentials for Sentinel nodes
    sentinel_kwargs={
        "username": "sentinel_user",
        "password": "sentinel_pass",
    },
    # Credentials for Redis master
    username="redis_user",
    password="redis_pass",
    db=0
)
broadcast = Broadcast(backend=backend)

# SSL with programmatic configuration
import ssl
ssl_context = ssl.create_default_context()
backend = RedisSentinelBackend(
    sentinels=[("sentinel1", 26379), ("sentinel2", 26379)],
    service_name="mymaster",
    sentinel_kwargs={
        "username": "user",
        "password": "secret",
        "ssl": True,
        "ssl_context": ssl_context,
    },
    username="user",
    password="secret",
    ssl=True,
    ssl_context=ssl_context
)
broadcast = Broadcast(backend=backend)
```

**Features:**
- Connects to Redis master for both publishing and subscribing
- Event-driven connection error detection (no polling overhead)
- Automatic reconnection on connection failures
- Support for multiple sentinel nodes for redundancy
- Full SSL/TLS support for secure connections

## Redis Sentinel Stream Backend

The Redis Sentinel Stream backend combines Redis Streams with Sentinel for message persistence and high availability.

```python
from broadcaster import Broadcast

# URL-based configuration
broadcast = Broadcast("redis-stream+sentinel://sentinel1:26379,sentinel2:26379/mymaster")

# With authentication
broadcast = Broadcast("redis-stream+sentinel://sentinel1:26379,sentinel2:26379/mymaster?username=user&password=secret")

# With SSL/TLS
broadcast = Broadcast("rediss-stream+sentinel://sentinel1:26379,sentinel2:26379/mymaster?username=user&password=secret")

# Direct backend instantiation
from broadcaster.backends.redis_sentinel import RedisSentinelStreamBackend

backend = RedisSentinelStreamBackend(
    sentinels=[("sentinel1", 26379), ("sentinel2", 26379)],
    service_name="mymaster",
    sentinel_kwargs={
        "username": "user",
        "password": "secret",
    },
    username="user",
    password="secret",
    db=0
)
broadcast = Broadcast(backend=backend)
```

**Features:**
- Message persistence via Redis Streams
- Automatic failover via Sentinel
- Event-driven error detection and reconnection
- Consumer group support (via Redis directly)
- Message acknowledgment and replay capabilities


### Using custom backends

You can create your own backend and use it with `broadcaster`.
To do that you need to create a class which extends from `BroadcastBackend`
and pass it to the `broadcaster` via `backend` argument.

```python
from broadcaster import Broadcaster, BroadcastBackend

class MyBackend(BroadcastBackend):

broadcaster = Broadcaster(backend=MyBackend())
```

## Where next?

At the moment `broadcaster` is in Alpha, and should be considered a working design document.

The API should be considered subject to change. If you *do* want to use Broadcaster in its current
state, make sure to strictly pin your requirements to `broadcaster==0.3.0`.

To be more capable we'd really want to add some additional backends, provide API support for reading recent event history from persistent stores, and provide a serialization/deserialization API...

* Serialization / deserialization to support broadcasting structured data.
* A backend for RabbitMQ.
* Add support for `subscribe('chatroom', history=100)` for backends which provide persistence. (Redis Streams, Apache Kafka) This will allow applications to subscribe to channel updates, while also being given an initial window onto the most recent events. We *might* also want to support some basic paging operations, to allow applications to scan back in the event history.
* Support for pattern subscribes in backends that support it.

## Third Party Packages

### MQTT backend
[Gist](https://gist.github.com/alex-oleshkevich/68411a0e7ad24d53afd28c3fa5da468c)

Integrates MQTT with Broadcaster
