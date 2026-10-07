# Function-profile GraphQL WebSockets

These optional native-thread experiments serve Graphene Django and Strawberry
Django schemas over `graphql-transport-ws` and legacy `graphql-ws`. They accept
queries and mutations as well as subscriptions. They do not change the profile
specification, and they are not replacements for Django Channels consumers.

Install the selected extra alongside the native Gunicorn worker:

```console
python -m pip install '/path/to/asgi-function-profile[graphene,gunicorn]'
# or:
python -m pip install '/path/to/asgi-function-profile[strawberry,gunicorn]'
```

Set up Django as usual before constructing the application. Route WebSocket
scopes to one of these ordinary applications, and HTTP scopes to the existing
function Django handler:

```python
from asgi_function_profile.django import get_function_application
from asgi_function_profile.graphene import GrapheneDjangoWebSocket

http_application = get_function_application()

def authenticated(request, connection_params):
    return request.user.is_authenticated and not request.session.modified

socket_application = GrapheneDjangoWebSocket(
    schema,
    allowed_origins={"https://app.example.com"},
    on_connect=authenticated,
)

def application(scope, receive, send):
    if scope["type"] == "websocket" and scope["path"] == "/graphql":
        return socket_application(scope, receive, send)
    return http_application(scope, receive, send)
```

This minimal example leaves lifespan unsupported, as the HTTP handler does.
An application's existing ordinary lifespan handler can be routed separately.
For Strawberry, import `StrawberryDjangoWebSocket` from
`asgi_function_profile.strawberry` and use it in place of
`GrapheneDjangoWebSocket` with the Strawberry schema.
Run the entry point with `ThreadedUvicornWorker`; application code constructs
no coroutine adapter. Other custom socket paths remain ordinary applications
selected by this router. The GraphQL implementation does not translate their
application logic automatically.

## Ordinary subscription sources

Graphene uses its existing `subscribe_<field>` convention. The source is an
ordinary iterator, and each yielded event is passed through the schema's
normal synchronous execution pipeline:

```python
class Subscription(graphene.ObjectType):
    ticks = graphene.Int(steps=graphene.Int(default_value=3))

    def subscribe_ticks(root, info, steps):
        for value in range(steps):
            if info.context.cancelled.is_set():
                return
            yield value
```

Strawberry uses its existing subscription decorator with an explicit event
type. Its type inference currently understands async stream annotations, so
use `graphql_type` for an ordinary source:

```python
@strawberry.type
class Subscription:
    @strawberry.subscription(graphql_type=int)
    def ticks(self, info: strawberry.Info, steps: int = 3):
        for value in range(steps):
            if info.context.request.cancelled.is_set():
                return
            yield value
```

Graphene's default context is a Django request. Strawberry's default is its
existing `StrawberryDjangoContext`, containing `request` and `response`. Both
requests carry `scope`, `connection_params`, and a `cancelled` threading event.
The response object is only context; a WebSocket cannot deliver HTTP response
headers or updated session cookies. Keep login and cookie rotation on HTTP.

An optional `get_context(request, connection_params, cancelled)` function can
return a project's existing context shape. It runs on the operation thread.
The request side of Django's session/auth middleware supplies cookie-based
authentication. `on_connect` is optional: without it, initialization is allowed
for anonymous clients, and the schema must enforce its own permissions. Exact
allowed origins are mandatory, including rejection of missing or duplicate
origins. Origin checking does not grant application authorization.

Graphene uses configured `GRAPHENE["MIDDLEWARE"]`, or an explicit `middleware=`
list, for ordinary field execution. Strawberry retains its synchronous schema
extension pipeline. Async sources/resolvers/extensions require translation;
this experiment does not introduce an asyncio loop to run them.
Graphene field middleware runs when resolving each event, but does not wrap
the subscription source resolver. Put subscription authorization in the source
resolver itself, and use `get_context` for operation-start context setup. Reset
permission caches and recheck authorization during event delivery when those
decisions must remain fresh throughout a long-lived subscription.

## Execution and cleanup

### Broadcast-backed sources

[examples/broadcast.py](../examples/broadcast.py) is a process-local experiment
using an ordinary iterator, a condition variable, and bounded subscriber queues.
It is example code, not a library API or distributed broker. A Graphene source
can return it directly:

```python
from examples.broadcast import Broadcast

updates = Broadcast(capacity=64)

def subscribe_updates(root, info, topic):
    return updates.subscribe(topic, authorized=lambda: can_read(info.context, topic))

# Ordinary HTTP/application code can publish; publish never waits for a socket.
updates.publish("example-topic", 42)
```

For Strawberry, return the same iterator from the subscription method. Publish
immutable values: subscribers receive references to the same event object.
The authorization callback checks at subscription creation and before every
delivered event. It must make a fresh decision rather than reuse cached user or
permission state. It does not proactively revoke an idle socket or make checks
atomic with subsequent network delivery.

Overflow removes that subscriber and raises `SlowSubscriberError` on its next
read; pending events are discarded. Other subscribers and publishers continue.
This is an explicit example policy. Lossless delivery, dropping old events,
coalescing updates, and replay require different application choices.
Cancellation wakes `next()` and unregisters the subscriber. `drain()` wakes all
sources, completes their GraphQL operations, and rejects new subscriptions and
publishes; it does not itself close sockets or announce a reconnect code.

Each Gunicorn process has its own broker. Cross-process or cross-container
broadcasts need an external broker with equivalent cancellation, bounded
buffering, and cleanup. Calling an async channel layer through a new event loop
per receive is not demonstrated here. No Redis or Channels compatibility is
claimed by this experiment.

### Threads and resources

A connection retains a native thread for receiving messages. Each active
GraphQL operation runs in another native thread from a per-connection growing
executor; idle operation threads are reused until the socket ends. Multiple
operations can progress on one connection. The executor has the same high
fail-fast guard as the native worker, but its operation pool is separate from
the worker's scope pool. No deployment-wide connection or operation limit is
introduced. Native-thread and memory costs need load evaluation.

Each operation keeps source creation, iterator advancement, result execution,
and iterator cleanup on one thread, with no running asyncio loop. Each result
waits for the actual network send before asking for the next source item.
Sends from different operations are serialized. Parsing, validation, variable
coercion, aliases, fragments, and event result execution remain in the schema
libraries. Only subscription source selection uses a small translated
graphql-core execution context, accepting ordinary iterators.

Database connections are closed after source creation and after each event's
execution, before network sends. Iterator cleanup runs on its operation thread;
disconnect, client cancellation, protocol failure, and server shutdown signal
cancellation and join operation threads. A blocking source must cooperate:
`request.cancelled.wait(timeout)` can replace a sleep, or a source iterator can
provide a **thread-safe, nonblocking** `cancel()` that wakes its blocking
`next()`. `close()` is called on the operation thread after `next()` returns.
Arbitrary synchronous code cannot be interrupted safely; a source that never
returns can still require Gunicorn's graceful-timeout process termination.

Do not hold a transaction or open connection while waiting indefinitely inside
a source. Cleanup cannot run until that source returns or yields. Transactions
and per-event authorization remain application choices. A request/user is
created once per GraphQL operation, so long-lived operations need explicit
session freshness, permission-cache invalidation, and revocation policies.
HTTP middleware and `ATOMIC_REQUESTS` do not automatically apply to sockets.

## Sentry

Initialize the SDK before constructing schemas, with its Django integration
enabled, so the native worker supplies the outer ASGI transaction. Preserve
Graphene integration or enable Strawberry's synchronous integration
(`StrawberryIntegration(async_execution=False)` for tested SDK 2.38.0).
That SDK's Graphene integration requires Graphene 3.3 or newer. Older Graphene
versions can satisfy the transport's dependency range while leaving GraphQL
instrumentation unavailable; check the SDK integration requirements separately
when preserving an existing dependency lock.

Operation threads inherit the connection trace but establish separate Sentry
isolation/current scopes for user, tags, and breadcrumbs. Existing Graphene
execution instrumentation remains active. Strawberry's view error hook is
retained because the SDK captures resolver errors there rather than in schema
execution alone. Unexpected iterator exceptions are captured separately.
`get_context` can set Sentry user data according to the application's privacy
policy. The socket's outer transaction lasts for the connection; profiling and
per-operation transaction design are not validated.

## Evidence and boundaries

The executable checks use real login/session cookies, ordinary ORM work,
queries, mutations, variables, both WebSocket protocols, subscription events,
simultaneous operations, cancellation/ID reuse, initialization timeout,
duplicate operations, invalid messages/origins, source errors, resolver errors,
HTTP alongside sockets, and cooperative shutdown. Isolated bridge checks prove
that a blocked send prevents another source item and that send errors propagate
while closing the iterator. Local Sentry envelopes verify resolver/source error
capture with the correct operation user and query spans; tests send nothing to
an external Sentry service.
The broadcast example additionally tests topic isolation, bounded overflow
without stopping a fast subscriber, cancellation of waiting sources, permission
revocation, broker draining, and worker shutdown with idle broadcast sources.
Real-worker checks deliver events through both schema libraries and verify that
revocation ends the operation while the socket remains usable. Permission and
other source errors are GraphQL errors even when they inherit from `OSError`;
errors raised by the actual send retain transport failure behavior.

Dependency versions exercised are Graphene 3.4.3, Graphene Django 3.2.3,
graphql-core 3.2.13, Strawberry 0.327.7, Django 5.2, and Sentry SDK 2.38.0 on
the native Gunicorn/Uvicorn worker. The Strawberry extra is deliberately narrow:
this experiment uses its execution-context and view-error integration points.
The 11 GraphQL checks also pass with Graphene 3.3, Graphene Django 3.2.2, and
graphql-core 3.2.3, including Sentry's Graphene instrumentation.
Gevent, Linux, throughput, third-party schema extensions, and exhaustive wire
protocol conformance are not validated.

This implementation supplies **schema execution and WebSocket transport**, not
a message broker. Channels-style subscription classes, group membership,
Redis/channel-layer broadcast routing, application permission middleware,
deployment drain groups, and custom chat/AI streaming consumers need separate
translation in the application repository. An existing Channels `Subscription`
class cannot be passed in unchanged merely because it uses Graphene.
