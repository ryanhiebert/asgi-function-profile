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

## Tests

The semantic suite covers blocking receive, send backpressure, exception
propagation, HTTP streaming, WebSockets, lifespan, overlapping scopes, and
adapter cancellation:

```console
uv run python -m unittest discover -s tests -v
```

These tests validate the compatibility adapter, not a native function-profile
server. Arbitrary Python code running in a worker thread still cannot be
forcibly cancelled safely.

## Naming

`asgi-function-profile` is a descriptive working repository name. It does not
settle the eventual name of the interface or claim official ASGI status.
