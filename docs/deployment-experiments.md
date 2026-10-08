# Native deployment experiment findings

These local experiments changed neither the function-profile contract nor the
adapter. They evaluate feasibility, resource costs, and shutdown behavior;
the examples are research code rather than supported application services.

## What we learned

- **Redis delivery works with ordinary subscription iterators.** Independent
  subscriber processes received publications. Graphene and Strawberry sources
  cancelled cooperatively, removed subscriptions, and cleaned up after errors.
  Each operation owns its Pub/Sub connection on its thread; cancellation signals
  an event rather than closing that connection from another thread.
- **The pieces compose through a proxy.** Authenticated GraphQL subscriptions
  received Redis events through nginx. Protocol pings preserved idle sockets
  beyond its short read timeout. Worker shutdown delivered close code 1012,
  joined application threads, and removed Redis subscriptions. A reconnect
  reached a replacement worker after nginx reloaded its upstream.
- **An existing ASGI extension needs no special bridge.** The WebSocket denial
  example delivered HTTP 429, headers, and a multipart body. Ordinary sends
  preserved backpressure and the original send exception. A capability-absent
  fallback used core close-before-accept behavior.
- **Idle sockets have a measurable native-thread cost.** With one worker and
  eight HTTP clients, 128 idle echo sockets left HTTP working and added roughly
  14 MiB of worker RSS. About 128 additional threads remained after socket
  closure because the pool reuses them until worker shutdown. GraphQL operation
  threads and Redis connections were not included in these echo measurements.

The synthetic throughput comparison showed a large local WSGI/ASGI difference
that we could not fully explain. It is not evidence of a general speedup from
the calling convention. The useful result is HTTP progress and measured resource
cost alongside idle sockets. Raw measurements are retained locally outside the
versioned experiment; they are not a performance promise.

## Retained experiments

- [Redis source](../examples/redis_broadcast.py) and
  [tests](../tests/test_redis_broadcast.py): live, at-most-once JSON Pub/Sub with
  authorization at subscription and event delivery, finite read polling, and
  cooperative drain. It has no replay or Channels naming/serialization
  compatibility. The payload limit applies after redis-py parses a message;
  Redis output buffers and connection limits remain separate resources.
- [Proxy tests](../tests/test_proxy.py): optional Docker nginx smoke checks for
  idle connections, replacement/reconnect, and combined GraphQL/Redis shutdown.
  The harness uses Docker Desktop's host networking on macOS; Linux, TLS, and
  cloud load balancers were not evaluated.
- [Denial example](../examples/denial.py) and
  [extension tests](../tests/test_extensions.py): a compact conformance example
  for one extension, not a new overload policy or all-extension certification.
- [Resource/performance probe](../benchmarks/probe.py): identical synchronous
  Django views through WSGI gthread, coroutine ASGI, and the function handler.
  It records CPU, memory, latency, and threads with 0/32/128 idle echo sockets.
  Views use SQLite transaction setup but no model queries; Sentry is disabled.

## Reproducing the infrastructure checks

Install all optional dependencies and use a disposable Redis endpoint. Never
point these tests at a production service; they terminate their own test Redis
connections. The proxy tests create and remove their own nginx containers.

```console
uv sync --locked --all-extras --group test
docker run --detach --name function-profile-test-redis \
  --publish 127.0.0.1::6379 redis:8-alpine
docker port function-profile-test-redis 6379
docker pull nginx:1.30.5-alpine
```

Replace PORT with the displayed port:

```console
ASGI_FUNCTION_REDIS_URL=redis://127.0.0.1:PORT/0 \
ASGI_FUNCTION_NGINX_IMAGE=nginx:1.30.5-alpine \
uv run --locked --all-extras --group test python -m unittest discover -s tests -v
docker rm --force function-profile-test-redis
```

Infrastructure suites explicitly skip without these environment variables.
The enabled macOS ARM64 run passed 126 tests without skips on Python 3.14.
An older-dependency Python 3.11 run passed 125 tests without skips, followed by
all three final proxy checks. PostgreSQL and production workloads remain open.

## Local performance probe

Run without competing tests. Each client reuses a connection and waits for its
response before issuing another request, so this is closed-loop synthetic load.
The probe records source hashes, versions, parameters, and worker resources.

```console
uv run --locked --all-extras --group test python -m benchmarks.probe \
  --path / --requests 512 --repeat 3 > /tmp/fast-probe.json
uv run --locked --all-extras --group test python -m benchmarks.probe \
  --path /wait/ --requests 256 --repeat 3 > /tmp/wait-probe.json
```

A useful next performance comparison needs representative views, database work,
production instrumentation, equal resource budgets, and an application-specific
acceptance threshold. These experiments do not establish production readiness.
