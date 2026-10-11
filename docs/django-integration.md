# Django integration guide

This is an evaluation guide for an experimental Django 5.2 handler, not a
production-readiness claim. The application contract is in [SPEC.md](../SPEC.md);
execution policy is in [README.md](../README.md#gunicorn-worker).

## Installation

Install a local checkout with its Django and Gunicorn extras:

```console
python -m pip install '/path/to/asgi-function-profile[django,gunicorn]'
```

For reproducible evaluation, use a checkout at a recorded Git commit or a wheel
built from that commit. Record the resolved dependency versions and wheel
SHA-256. The development version `0.1.0.dev0` alone does not identify a build.
An editable installation follows working-directory changes and is suitable for
development rather than reproducible benchmark results.

The repository's `test` dependency group is not a published extra. HTTP-only
evaluation does not require a WebSocket backend; socket evaluation must
explicitly select and install a backend supported by Uvicorn.

## HTTP-only entry point

Create an experimental deployment module, preserving the project's existing
settings initialization and import path. Replace `myproject.settings` below
with the project's actual settings module.

```python
import os

from asgi_function_profile.django import get_function_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
http_application = get_function_application()


def application(scope, receive, send):
    if scope["type"] == "http":
        return http_application(scope, receive, send)
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                send({"type": "lifespan.shutdown.complete"})
                return
            else:
                raise ValueError(f"unexpected lifespan event: {event['type']}")
    raise ValueError(f"unsupported scope: {scope['type']}")
```

This lifespan acknowledges startup and shutdown but allocates no resources.
Project-specific lifecycle work needs explicit ownership: lifespan runs per
worker, not once per deployment. Shared lifespan values must be suitable for
access from different application threads.

For a module named `myproject.function_http`, start it with:

```console
python -m gunicorn myproject.function_http:application \
  --worker-class asgi_function_profile.gunicorn.ThreadedUvicornWorker \
  --workers 2 --bind 127.0.0.1:8000
```

Run initial evaluation in an isolated environment. Preserve the existing
production entry point for comparison and rollback. Choose process counts,
proxy configuration, and shutdown timeouts appropriate to the deployment.

## Execution policy

- Threads are created on demand and reused; the pool starts empty.
- Each process admits up to **65,536 outstanding invocations**, including
  lifespan. This is a high guardrail, not measured production capacity.
- At the ceiling, `ThreadCapacityError` reports the limit and says the
  invocation was rejected instead of queued. Uvicorn logs the application
  error; there is no custom overload response or retry policy. OS
  thread-creation errors propagate and may occur earlier.
- Gunicorn `--threads` and `--worker-connections` do not control this worker.
- Idle threads remain until worker exit and are joined during shutdown.
  Arbitrary synchronous work cannot be interrupted safely; Gunicorn may kill
  a worker that exceeds its graceful timeout.
- The development runner uses asyncio's finite default executor, rather than
  the Gunicorn worker's growing pool.
- This worker accepts function-profile applications. An existing coroutine
  ASGI application cannot be passed to it unchanged. Mixed deployments need
  separate servers or an outer coroutine router with an adapter around only
  the function branch; such worker composition is not supplied here.

The handler buffers request bodies with disk spill, like Django's ASGI handler.
It preserves synchronous response iteration, rejects async response iterators,
and does not concurrently monitor disconnects while a view or iterator runs.
Middleware contexts can end before response iteration, as in Django's ordinary
synchronous pipeline; evaluate iterators that depend on request-scoped state.
File sending may differ in efficiency from WSGI server optimizations.

## Validation evidence and remaining work

For native Graphene Django and Strawberry Django socket experiments, see the
[GraphQL WebSocket guide](graphql-websockets.md), including application-side
translation boundaries and Sentry checks.

### Sentry on native threads

Keep the application's existing `sentry_sdk.init(...)` configuration with
`DjangoIntegration` enabled. Initialize it before the application is loaded by
the native Gunicorn worker or development runner. Those deployment paths place
the SDK's `SentryAsgiMiddleware` outside the function adapter automatically when
the integration is enabled. No Sentry dependency or initialization is added to
the application handler or adapter; applications without the integration keep
their existing behavior.

This wrapper is necessary because the function handler overrides Django's
coroutine entry point, which Sentry normally patches to start transactions and
isolate request context. The inherited Django hooks still instrument views,
database operations, request data, and exceptions. The adapter carries the
outer middleware's context into the native application thread.

Isolated SDK 2.38.0 tests, with and without its default integrations, cover HTTP
transactions and route names, incoming trace
continuation, view and SQLite query spans, HTTP 500 errors captured once with
request/user data, streaming exceptions, and user/tag/breadcrumb isolation in
overlapping requests and on a reused thread. A real native Gunicorn deployment
also emits a transaction with query spans into a local test transport. Nothing
is sent to Sentry during these tests.

After adding this integration, the full suite passed **84 tests with no skips**
on the Python 3.14 environment recorded below. The isolated Sentry checks also
passed separately with both SDK integration configurations.

Custom deployments that instantiate `FunctionProfileAdapter` directly must put
Sentry's ASGI middleware outside that adapter themselves. The gevent worker,
WebSocket instrumentation, profiling, custom processors/integrations, and other
SDK versions have not been validated by this check. Evaluate these separately
when used; the tests establish specific HTTP instrumentation behavior rather
than complete Sentry feature parity.

### Earlier compatibility checks

The complete suite passed **80 tests, with no skips**, on macOS ARM64 using
Python 3.14.2, Django 5.2.17, Gunicorn 26.2.0, Uvicorn 0.54.0,
uvicorn-worker 0.4.0, gevent 26.9.0, websockets 17.2, and psutil 7.2.2.

A wheel installation was also evaluated on macOS ARM64 with Python 3.11.14,
Django 5.2.16, Gunicorn 23.0.0, Uvicorn 0.35.0, uvicorn-worker 0.3.0, and
asgiref 3.8.1. All 18 Gunicorn integration tests, 8 adapter tests, 7 Django
handler tests, and 6 executor tests passed without skips. The first integration
run exceeded the fixture's five-second startup deadline on its first test;
the complete rerun passed. Gevent was not evaluated in that environment.

Tests cover reuse, capacity rejection and recovery, thread-creation failure,
HTTP alongside open sockets, authentication, streaming and backpressure,
SQLite transaction cleanup, shutdown, and two-process preload startup.
Declared support is Python >=3.11 and Django >=5.2,<5.3, but this declaration
is broader than the tested combinations. Linux deployment, PostgreSQL,
production library compatibility, load capacity, and additional ASGI extensions
need further validation. No throughput or latency improvement is claimed.
The [deployment experiments](deployment-experiments.md) now cover native Redis
sources, real nginx forwarding and worker replacement, a WebSocket denial
extension, and a local synthetic Django performance probe. They are narrower
evidence than production load or exhaustive extension conformance.

A practical evaluation should compare unchanged representative views against
an existing deployment with equal resource budgets. Check middleware and
request-context isolation, database cleanup, streaming, proxy behavior, and
shutdown before measuring throughput, tail latency, CPU, memory, native
threads, and database connections. Define acceptable performance from the
application's service requirements rather than assuming a universal threshold.

## Native Channels integration

Install the optional `channels` extra for ordinary consumers and event iterators.
This is a translated subset, not full Channels compatibility. It keeps existing
backends, including `channels_redis.core.RedisChannelLayer`, behind one owned
asyncio loop per `FunctionChannelLayer`. Create the layer after fork, during
lifespan startup, and close it after connections and sources finish:

```python
from channels_redis.core import RedisChannelLayer
from asgi_function_profile.channels import FunctionChannelLayer, JsonWebsocketConsumer

# In lifespan startup; obtain redis_url from your deployment configuration.
layer = FunctionChannelLayer(lambda: RedisChannelLayer(hosts=[redis_url]))

class Room(JsonWebsocketConsumer):
    channel_layer = layer
    groups = ("updates",)

    def receive_json(self, content):
        self.channel_layer.group_send("updates", {"type": "room.update", "value": content})

    def room_update(self, event):
        self.send_json(event["value"])

websocket_application = Room.as_asgi()
# In lifespan shutdown, after consumers finish: layer.close()
```

Backend construction and operations stay on that layer's loop. Do not pass a
backend previously used by another loop or inherited after fork. The ordinary
facade is separate from Channels' async `get_channel_layer()` interface.
Scopes and wire messages are unchanged; external Channels publishers can use
the same Redis namespace and groups.

Publishers outside an ASGI server still need an owner for the layer. A
short-lived command can close it explicitly through its context manager:

```python
with FunctionChannelLayer(lambda: RedisChannelLayer(hosts=[redis_url])) as layer:
    layer.group_send("updates", {"type": "room.update", "value": "ready"})
```

For Channels-style Graphene publishers, bind the subscription class's
`channel_layer` to this layer before calling its ordinary `broadcast()` method.
Long-lived task workers can reuse a layer created after fork, provided their
worker lifecycle closes it before process exit. The backend thread is
non-daemon: an unclosed layer keeps a publisher process alive, and ordinary
`atexit` callbacks run after Python waits for non-daemon threads. ASGI lifespan
does not manage publishers in other processes.

To reuse Django's `CHANNEL_LAYERS` configuration, the optional
`asgi_function_profile.django_channels.DjangoChannelLayerProvider` supplies
explicit shared and temporary ownership:

```python
from asgi_function_profile.django_channels import DjangoChannelLayerProvider

layers = DjangoChannelLayerProvider()  # "default" alias; starts no threads.
# During server/task-worker startup, after fork:
layer = layers.get()
# Assign this layer to consumers, then close it after they finish:
# layers.close()
```

In a standalone command, without creating a shared layer:

```python
from asgi_function_profile.django_channels import DjangoChannelLayerProvider

with DjangoChannelLayerProvider().publishing() as publisher:
    publisher.group_send("updates", {"type": "room.update", "value": "ready"})
```

`get()` lazily creates and reuses a shared layer for the configured alias.
`publishing()` borrows an existing shared layer, or creates and closes a
temporary layer on context exit. Nested publishing contexts reuse the temporary
layer, so a batch can avoid starting a backend for every send. Concurrent
standalone callers own separate temporary layers; no subscription class binding
changes during a send. Another alias can use another provider instance.

An owner must call `close()` after shared consumers and publishers finish;
closing while they run is unsupported. This closes all cached layers and permits
a later `get()` to create a fresh one. Replacing a `CHANNEL_LAYERS` entry retains
separate layers until cleanup; do not mutate configuration entries in place.
Fork resets the provider's cache and locks without touching the parent's
backends. Construction is safe before fork, but create shared layers after fork.
Copied contexts reuse temporary layers only while open and in their owning
process; a later call creates a fresh layer if needed.
Forking inside a publishing context and unwinding that inherited context in the
child is unsupported: inherited layer references cannot be used or closed there.
There is no automatic exit hook. The provider constructs its own backend on its
own loop; it does not reuse Channels' cached coroutine layer.

Socket and channel handlers run on the invocation thread, including Django ORM
work, context variables and Sentry attribution. The handshake is delivered
before channel events. Each input source has at most one unhandled event, with
a shared inbox that retains completion order. A busy channel source cannot
continually jump ahead of queued socket input. Groups in `groups`, or joined
through `join_group()`, are renewed at half the backend's `group_expiry` and
discarded on cleanup. `leave_group()` is serialized with renewal so departed
memberships are not re-added. Direct `layer.group_add()` calls remain the
application's responsibility.

An ordinary transport can reuse the multiplexer by subclassing `Consumer` and
overriding `run(self, receive)` to call `transport(self.scope, receive, self.send)`.
Use `join_group()` there to receive deployment drain events. Transport socket
handling stays ordinary; channel events invoke the consumer's named handlers.
Transport operation threads may send concurrently: the consumer serializes
actual sends and propagates their errors.

`ChannelEvents(groups, channel_layer=layer, limit=1024)` offers an ordinary
iterator without a consumer. It renews groups and buffers at most `limit`
notifications. Its thread-safe, nonblocking `cancel()` wakes a waiting `next()`;
the owning thread then calls `close()` to discard memberships. It uses backend
futures without adding a native reader thread. Overflow terminates the iterator
with `ChannelEventOverflow`, discarding pending events explicitly rather than
silently evicting a control notification. Backend and renewal errors wake the
iterator with their original exceptions. This is live, at-most-once delivery;
backend capacity drops, expiry, disconnect and cancellation still preclude a
lossless or replay guarantee.

The tests exercise race ordering, bounded read-ahead, group renewal and removal,
overflow, cancellation, original failure propagation, delegated async-hook
rejection, authenticated ORM work, Sentry attribution, unchanged Redis
publishers and cooperative Gunicorn shutdown. Cleanup errors propagate when
there is no active application failure; otherwise the original failure wins.

Each layered consumer retains one additional native socket-reader thread. It
is joined, rather than hidden behind a daemon or a timed join. An ordinary
receive has no portable cancellation capability: closing a real Uvicorn
socket releases it via disconnect, and cancellation of the outer adapter also
releases it. A close send that fails without a disconnect can leave cleanup
waiting until that outer cancellation; a synthetic regression test records this
boundary. A real TCP test verifies that a peer disconnect after both close sends
fail still releases the reader, removes consumer and subscription groups, and
joins application threads before Gunicorn's worker-exit hook.
Long-running handlers must also cooperate with shutdown. Gunicorn's graceful
timeout remains the fallback for uncooperative work. No change to the profile
or adapter contract is introduced by this integration.
