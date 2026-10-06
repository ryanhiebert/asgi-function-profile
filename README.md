# ASGI Function Profile

An experiment in using ASGI's scopes and event protocols with ordinary Python
functions.

```python
def application(scope, receive, send):
    event = receive()
    send(event)
```

The function profile represents waiting as an ordinary function call. A call
may suspend its current execution context until it can return; the interface
does not prescribe how that suspension is implemented. The current experiments
use both operating-system threads and gevent greenlets with the same application
interface and adapter.

This project is exploratory and is not an official part of ASGI.

The current draft specification is [SPEC.md](SPEC.md). Design rationale and the
translation audit remain in [docs/design-notes.md](docs/design-notes.md).

## Direction

The experiment aims to:

- reuse ASGI scopes, events, and protocol specifications;
- derive a regular-function calling convention mechanically from ASGI's
  coroutine convention;
- identify only the existing runtime assumptions that cannot be translated
  mechanically;
- preserve HTTP, WebSocket, streaming, extensions, and their existing
  semantics unchanged;
- resolve lifespan's event-loop affinity for the function form;
- demonstrate interoperability with an existing ASGI server; and
- determine whether the function form can be specified as a small profile over
  the existing coroutine specification.

The project does not assume that regular functions inherently block native
threads. "Coroutine profile" and "function profile" describe calling
conventions, not scheduling implementations.

## Contributing and continuing the experiment

This README is also the contributor entry point: `AGENTS.md` is a symlink to
it, so people and agents receive the same guidance. Keep that guidance here.
Read [SPEC.md](SPEC.md) for the proposed contract and
[docs/design-notes.md](docs/design-notes.md) for its rationale and translation
audit before changing the interface. The implementation sections below record
the evidence, deployment choices, and known limitations.

The guiding practical target is **existing synchronous Django views without
rewriting them**, alongside synchronous WebSockets. Application code should not
need `async`/`await`, explicit bridges, or scheduler management to use the
function profile. Select the profile at the deployment boundary.

Keep these boundaries when contributing:

- Treat coroutine ASGI as the complete semantic specification. Only a
  non-mechanical difference justifies additional profile semantics; lifespan
  affinity is the substantive difference identified so far. Do not broaden
  this work into resolving unrelated ASGI ambiguities.
- Keep scheduling, connection limits, overload policy, and framework-specific
  conveniences out of the profile unless evidence shows they are required.
  The two workers deliberately have different execution policies; neither
  defines the portable interface.
- Prefer a small executable experiment over a new framework or abstraction.
  Get it working, analyze whether it can be simpler, then simplify and record
  the learnings. The experimental code may ultimately be discarded.
- Explain outcomes and remaining limits with as little conceptual overhead as
  possible. Separate demonstrated behavior from inference and untested claims.
- Use the [full test command](#tests) for integration changes. Optional
  dependencies can otherwise make whole suites skip; report skipped or blocked
  checks rather than treating them as validation.

### Current position and remaining questions

The adapter, Django HTTP integration, authenticated synchronous WebSockets,
native-thread worker, and gevent worker are implemented and tested. The gevent
experiment demonstrates that the calling convention can survive a scheduler
change without changing application code, the Django handler, or the adapter.

No next implementation milestone has been selected after gevent. Explicit
remaining questions include a first ASGI extension conformance example and the
compatibility of other database drivers and libraries with the execution
backends. The adoption sequence in the design notes is a longer-term direction,
not an instruction to implement every candidate, publish, or contact upstream.
Agree on the next experiment before expanding scope, and update this section
as that direction changes.

## Reference implementation

A dependency-free adapter runs each function-profile scope invocation in an
executor worker. Ordinary `receive()` and `send()` calls bridge to the actual
ASGI awaitables and wait for their completion, preserving backpressure and
exception propagation.

Application code does not import or construct that adapter. The development
runner establishes the profile at the deployment boundary:

```console
uv sync --extra server
uv run asgi-function examples.echo:application
```

Then, in another terminal:

```console
curl --data-binary 'hello' http://127.0.0.1:8000/
```

The example echoes `hello` without using `async` or `await` in application
code.

## Gunicorn worker

Gunicorn can load the same function application directly using the optional
thread-based worker (on a POSIX host):

```console
uv sync --extra gunicorn --extra django --group test
uv run gunicorn examples.django_demo.application:application \
  --worker-class asgi_function_profile.gunicorn.ThreadedUvicornWorker \
  --workers 2 --threads 8 --bind 127.0.0.1:8000
```

For the Django demo, initialize its database and user as described below first.
The `test` dependency group supplies the WebSocket backend for this example;
the `gunicorn` extra alone does not install a WebSocket backend. For another
project, replace the application target with its function-profile entry point.

Selecting `ThreadedUvicornWorker` selects the function profile at deployment:
application code does not import the adapter. The worker subclasses the maintained
[Uvicorn worker](https://github.com/Kludex/uvicorn-worker) and wraps the loaded
application with the existing `FunctionProfileAdapter`. Gunicorn manages
processes; Uvicorn handles HTTP/WebSockets; the adapter runs application calls
in native threads. No new launcher or scheduling mechanism is introduced.

`--workers` controls **processes**; `--threads N` gives each process N application
threads shared by HTTP requests and WebSocket connections. Lifespan has a
separate single-thread executor, so it does not consume a request slot. Pools
are created after forking and joined before worker exit, including with
`--preload`. Gunicorn's default is one application thread per process; choose
more to serve HTTP alongside open WebSockets.

Idle WebSockets still occupy application threads. When all N slots are busy,
new invocations queue until one finishes; `--threads` is not a connection or
queue limit. The `asgi-function` development runner remains available and keeps
using the event loop's default executor.

The Django HTTP and authentication/socket tests also run through this worker.
Tests cover graceful `SIGTERM` shutdown with an authenticated socket open,
lifespan shutdown, and joined application threads before Gunicorn's worker-exit
hook. Capacity tests exercise the default single application thread and an
explicit three-thread pool: lifespan leaves all slots available, open sockets
fill the pool, closing a socket releases queued HTTP, and shutdown joins both
pools. A separate smoke test exercises two worker processes with `--preload`.
The integration retains Gunicorn's SIGTERM handler so Uvicorn's signal replay
can return through normal executor and worker cleanup. Gunicorn may still
force-terminate workers that outlive its graceful timeout; arbitrary synchronous
work cannot be safely interrupted in a thread.

## Gevent experiment

The alternative worker runs the **same function application** in greenlets:

```console
uv sync --extra gevent --extra django --group test
uv run gunicorn examples.django_demo.application:application \
  --worker-class asgi_function_profile.gevent.GeventUvicornWorker \
  --workers 2 --bind 127.0.0.1:8000
```

Initialize the demo database and user as described below first. As with the
thread worker, the test dependency group supplies the example's WebSocket
backend. This is an experimental POSIX worker, not a production-readiness claim.

### What changed, and what we learned

The application, Django handler, and `FunctionProfileAdapter` are unchanged.
A small executor schedules each invocation as a greenlet. Each worker process
uses its main native thread for gevent and one additional native thread for
Uvicorn's asyncio loop. This separation keeps Django off the asyncio thread:
its async-safety checks stay enabled. There is no `DJANGO_ALLOW_ASYNC_UNSAFE`
workaround and no new protocol implementation.

The worker applies [gevent monkey-patching](https://www.gevent.org/intro.html)
in the child process, before loading the application. Ordinary supported I/O
then yields cooperatively, and thread-local storage becomes greenlet-local.
**`--preload` is rejected**; do not import the application from Gunicorn's
configuration or hooks before worker initialization either. Patching cannot
retroactively make already-created Django connection locals safe.

The decisive test opens **64 authenticated synchronous WebSockets in one
worker**. Ordinary Django HTTP and socket messages continue to work, with
**two native threads both before and after opening the sockets**. Shutdown
closes all 64 sockets, finishes lifespan, and joins the server thread before
Gunicorn's worker-exit hook. This is a concurrency check, not a throughput or
latency benchmark.
The test deliberately sets Gunicorn's `--worker-connections` to 1 to verify
that this setting does not cap the worker's application concurrency.

Overlapping authenticated Django requests also share one native thread while
retaining distinct database wrappers, transaction state, thread-local values,
and context variables. The existing auth, logout/revocation, rollback,
streaming, disconnect, and network-backpressure tests run on this worker too.
The adapter's eight semantic tests run separately in a monkey-patched process,
including cancellation and propagation of send errors. Two-worker startup and
shutdown are tested without preloading.

The simplification was to keep the original bridge and change only its
executor, rather than build a second bridge or HTTP/WebSocket stack. One
shutdown detail needed explicit handling: receiving `lifespan.shutdown.complete`
is not the same as the application returning. The worker waits for that return
before closing the asyncio loop.

### Limits

Like Uvicorn's default coroutine execution, this worker adds **no application
concurrency cap** and configures no total connection-count limit. Each
invocation can start in a greenlet without waiting for an application slot.
Gunicorn's `--worker-connections` and `--threads` are unused by this worker;
unlike Gunicorn's WSGI gevent worker, it leaves connection handling to Uvicorn.
Existing timeout and protocol controls still apply. Resource-limit and overload
policies are separate deployment concerns, not requirements of the function
profile. The native `ThreadedUvicornWorker` still uses `--threads` to size its
finite pool.

Gevent removes the native-thread cost of idle sockets, not every source of
blocking. CPU-bound work and non-cooperative I/O can still stall all application
greenlets in a worker. The demo uses SQLite; its short operations passed these
tests, but SQLite is not made cooperative by this worker. Other database drivers
and third-party libraries need their own compatibility checks and may need
additional deployment integration. DNS or such integrations may also create
native helper threads; the measured two-thread result is for this test workload.
Arbitrary uncooperative work can still require Gunicorn to kill the process at
its graceful timeout. Reloading, TLS, and async Django views have not been
validated on this backend.

Validated here with Python 3.14, gevent 26.9.0, Gunicorn 26.2.0, and Uvicorn
0.54.0; other supported dependency versions have not been exercised.

```console
uv run --extra gevent --extra django --group test python -m unittest discover -s tests -p test_gevent.py -v
```

## Django experiment

The optional Django 5.2 integration runs existing synchronous views and
middleware through the function profile. In an existing project's deployment
entry point, keep its `DJANGO_SETTINGS_MODULE` setup and use:

```python
from asgi_function_profile.django import get_function_application

application = get_function_application()
```

Run that entry point with `asgi-function myproject.asgi:application`. Views,
URL configuration, models, and middleware keep their existing interfaces.
This is an experimental handler built on Django internals, currently scoped to
Django 5.2; it is not a production integration or an exhaustive compatibility
claim. Code that specifically depends on a WSGI environment still needs review.

The runnable [demo](examples/django_demo/application.py) combines Django HTTP,
an ordinary WebSocket echo function, and authenticated private notes. From the
repository root:

```console
uv sync --extra server --extra django --group test
uv run python -m django migrate --run-syncdb --settings=examples.django_demo.settings
uv run python -m django createsuperuser --settings=examples.django_demo.settings
uv run asgi-function examples.django_demo.application:application
```

Try `/`, `/echo/`, `/notes/`, `/stream/`, or `/download/` over HTTP, or `/ws/`
with a WebSocket client. The `test` group provides the optional WebSocket
implementation used for this experiment. The demo database is local and ignored
by Git. The demo has local-only settings, not a production deployment
configuration. Re-run `migrate --run-syncdb` if you used the earlier demo;
the original notes table is retained and the new tables are added.

For the authenticated example, open `http://127.0.0.1:8000/account/`, sign in
with the account you created, and save a note. The page sends text over
`/ws/notes/`; `/me/` reads that user's notes through an ordinary Django view.
The socket uses the browser's existing session cookie. If you change the host
or port, update `WEBSOCKET_ALLOWED_ORIGINS` in the demo settings to match the
browser's exact HTTP origin.

### What the experiment established

- Django already supplies the needed pieces. The new handler joins its ASGI
  request parsing to its synchronous middleware/view pipeline, then sends the
  response through ordinary `send()` calls. The existing adapter is unchanged.
- Identical views produce matching status, headers, and bodies through Django's
  WSGI handler and this handler. Tests cover forms, raw bodies, uploads, cookies,
  CSRF middleware, files, errors, and URL prefixes. SQLite ORM transactions and
  connection cleanup work through the real server; middleware, queries, and
  request signals stay on the request's worker thread.
- Synchronous streaming remains incremental. A comparison test shows that
  Django 5.2's coroutine response path buffers a synchronous iterator, whereas
  this handler sends its first chunk before asking for the next one. One
  iterator loop handles ordinary responses, streams, and files.
- Synchronous WebSockets work beside Django on the same server, including text,
  binary messages, and disconnect. They are a separate function-profile
  application; an HTTP Django view does not become a WebSocket handler.
- No new profile semantics were needed. The experiment demonstrates
  compatibility and incremental streaming, not a performance improvement.

The initial native-thread experiment uses one executor thread per active
scope, including lifespan and long-lived sockets. The gevent worker described
above instead uses a greenlet per invocation. A disconnect during
body reception is handled, and a failed send closes response resources, but
this handler does not monitor disconnects concurrently while a view or iterator
runs. Arbitrary synchronous work cannot be interrupted; older ASGI send
semantics may silently discard writes after disconnect. Long-running streams
therefore still need an application-specific stopping policy. Async views retain
Django's own fallback adaptation but are outside this experiment's tested path;
async response iterators are explicitly rejected to avoid silent buffering.

### What authenticated sockets taught us

The second experiment adds only example code. The profile, adapter, and Django
HTTP handler needed no changes. The socket-specific code is in
[sockets.py](examples/django_demo/sockets.py), with three ordinary functions:
look up a session user, bound database work, and handle socket messages.

- **Django authentication is reusable, but HTTP middleware is not automatically
  WebSocket middleware.** HTTP login/logout use Django's built-in views and full
  session/auth/CSRF middleware. The socket builds an `ASGIRequest` from the
  handshake and calls the request hooks of `SessionMiddleware` and
  `AuthenticationMiddleware`. It does not run the HTTP response pipeline or
  invent another credential format. This validates those hooks, not arbitrary
  middleware or Channels compatibility.
- **A connection and a database operation have different lifetimes.** The socket
  can remain open while each message authenticates, commits its note, and closes
  its database connections before sending or waiting again. Transactions are
  explicit per message; Django's `ATOMIC_REQUESTS` does not apply to this loop.
  Tests assert thread affinity and no open database connection/transaction at
  every socket receive/send boundary, including failures.
- **Authentication freshness is an application policy.** A fresh lookup on each
  message avoids retaining Django's cached user for the socket's lifetime. With
  the demo's database sessions, HTTP logout prevents the next message from
  writing and prevents reconnection with the old cookie. An idle socket is not
  forcibly closed at logout; concurrent work already underway can still finish.
- **Cookie authentication needs an origin policy.** The demo accepts only exact
  configured origins, rejecting missing, opaque, duplicate, and untrusted ones.
  This is independent of HTTP CSRF checks. See the
  [Channels explanation of WebSocket origins](https://channels.readthedocs.io/en/latest/topics/security.html#websockets).

The tests log in over real HTTP, verify per-user note isolation, reject
anonymous/invalid sessions, exercise logout and CSRF failures, drop a TCP
connection, and inject an error after a database write to verify rollback and
cleanup. The simplification pass kept all protocol-specific choices in the
example: no consumer framework, generic middleware stack, or new scheduling
abstraction was necessary.

Login and session-cookie updates remain HTTP operations. If authentication
requires rotating a session cookie, the socket refuses it and requires a fresh
HTTP login. Other session backends, custom authentication middleware, push
revocation, and broadcast are untested. Capacity behavior is covered by the
thread-pool and gevent experiments, not a production load benchmark. A committed note may survive
a disconnect before its reply arrives; this example makes no exactly-once or
replay guarantee. A socket retains a thread or greenlet for its lifetime,
depending on the backend. These are boundaries of the
experiment, not additional requirements on the function profile.

### What a full worker pool taught us

The capacity experiment runs the same adapter and Django application with an
explicit **three-thread executor in one Uvicorn process**. These are application
threads, not Uvicorn's `--workers` processes. A test-only launcher injects the
executor using the adapter's existing argument; the production runner and
adapter are unchanged. The usual runner uses the event loop's shared default
executor, whose capacity is not fixed by this experiment.

| Active application invocations | Available threads | New Django HTTP request |
| --- | ---: | --- |
| Lifespan + one WebSocket | 1 | Runs normally |
| Lifespan + two WebSockets | 0 | Reaches the adapter but waits to start |
| One of those sockets closes | 1 | Starts and returns its ordinary response |

An existing WebSocket can still echo messages while HTTP is queued. The event
loop remains responsive: the limitation is that each synchronous invocation
retains its thread even while waiting in `receive()`. Lifespan retains a thread
between startup and shutdown as well. In this setup, HTTP progress depends on a
socket ending, not merely becoming idle.

Shutdown with both sockets open and HTTP queued completed without hitting the
server's two-second graceful-shutdown deadline. Clients received WebSocket
close code 1012, lifespan shutdown ran, every started invocation finished, and
the executor joined all its workers. A separate test disconnects a queued HTTP
client: the invocation still waits for a free thread, then exits without leaking
a worker; a later HTTP request succeeds. This is cooperative cleanup for the
tested applications, not an ability to interrupt arbitrary synchronous code.

This exposes an adapter scheduling limitation, not a new calling-convention
requirement. A larger fixed pool moves the threshold but retains the same
failure mode. The adapter currently has no admission limit or rejection policy
for queued scopes. The `ThreadedUvicornWorker` gives lifespan a separate thread
and sizes the remaining pool with `--threads`, but HTTP and WebSockets still
compete for that pool. The gevent experiment avoids a native thread per idle
invocation without adding an application-concurrency cap. Reserving HTTP
capacity or rejecting overload with bounded admission remain possible separate
experiments, not new rules in the profile. These tests characterize
resource exhaustion, not throughput or a production connection limit.

The reproducible tests are in [tests/test_capacity.py](tests/test_capacity.py):

```console
uv run --extra server --extra django --group test python -m unittest discover -s tests -p test_capacity.py -v
```

## Tests

For the complete suite, including both Gunicorn workers and Django, use a POSIX
host and install all optional dependencies:

```console
uv sync --locked --all-extras --group test
uv run --locked --all-extras --group test python -m unittest discover -s tests -v
```

Integration tests start local HTTP/WebSocket servers and worker subprocesses;
they need permission to bind loopback sockets. Inspect the test summary for
skips when running in a different environment. The commands below are useful
for narrower setups, but do not necessarily exercise every backend.

The semantic suite covers blocking receive, send backpressure, exception
propagation, HTTP streaming, WebSockets, lifespan, overlapping scopes, and
adapter cancellation. With the `server` extra installed, it also launches the
development runner and verifies streaming, network backpressure, disconnects,
lifespan state, and graceful shutdown through a real Uvicorn TCP connection:

```console
uv run python -m unittest discover -s tests -v
```

To include the Django and real WebSocket tests explicitly:

```console
uv run --extra server --extra django --group test python -m unittest discover -s tests -v
```

Add `--extra gunicorn` to include the Gunicorn integration tests as well.
Add `--extra gevent` to include the greenlet worker tests (and its Gunicorn
dependencies).

The integration tests validate the complete compatibility path through
Uvicorn, not a native function-profile server. Arbitrary Python code running in
a worker thread still cannot be forcibly cancelled safely.

## Naming

`asgi-function-profile` is a descriptive working repository name. It does not
settle the eventual name of the interface or claim official ASGI status.
