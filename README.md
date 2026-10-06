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
does not prescribe how that suspension is implemented. Initial implementations
will use operating-system threads, while future implementations could use green
threads or another stack-preserving scheduler.

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

The runnable [demo](examples/django_demo/application.py) combines Django HTTP
with an ordinary WebSocket echo function. From the repository root:

```console
uv sync --extra server --extra django --group test
uv run python -m django migrate --run-syncdb --settings=examples.django_demo.settings
uv run asgi-function examples.django_demo.application:application
```

Try `/`, `/echo/`, `/notes/`, `/stream/`, or `/download/` over HTTP, or `/ws/`
with a WebSocket client. The `test` group provides the optional WebSocket
implementation used for this experiment. The demo database is local and ignored
by Git. The demo has local-only settings and does not include an authentication
or deployment configuration.

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
  application; an HTTP Django view does not become a WebSocket handler. Django
  authentication, sessions, and Channels integration for sockets remain future
  work.
- No new profile semantics were needed. The experiment demonstrates
  compatibility and incremental streaming, not a performance improvement.

The limitations are concrete: the compatibility adapter uses one worker per
active scope, including lifespan and long-lived sockets. A disconnect during
body reception is handled, and a failed send closes response resources, but
this handler does not monitor disconnects concurrently while a view or iterator
runs. Arbitrary synchronous work cannot be interrupted; older ASGI send
semantics may silently discard writes after disconnect. Long-running streams
therefore still need an application-specific stopping policy. Async views retain
Django's own fallback adaptation but are outside this experiment's tested path;
async response iterators are explicitly rejected to avoid silent buffering.

## Tests

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

The integration tests validate the complete compatibility path through
Uvicorn, not a native function-profile server. Arbitrary Python code running in
a worker thread still cannot be forcibly cancelled safely.

## Naming

`asgi-function-profile` is a descriptive working repository name. It does not
settle the eventual name of the interface or claim official ASGI status.
