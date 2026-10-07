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
production library compatibility, load capacity, and ASGI extension conformance
need further validation. No throughput or latency improvement is claimed.

A practical evaluation should compare unchanged representative views against
an existing deployment with equal resource budgets. Check middleware and
request-context isolation, database cleanup, streaming, proxy behavior, and
shutdown before measuring throughput, tail latency, CPU, memory, native
threads, and database connections. Define acceptable performance from the
application's service requirements rather than assuming a universal threshold.
