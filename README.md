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

## Direction

The experiment aims to:

- reuse ASGI scopes, events, and protocol specifications;
- define a regular-function application calling convention;
- support HTTP, WebSocket, lifespan, streaming, and extensions;
- specify concurrency, backpressure, disconnect, and shutdown semantics;
- demonstrate interoperability with an existing ASGI server; and
- determine whether coroutine and function execution profiles can eventually
  share one ASGI specification.

The project does not assume that regular functions inherently block native
threads. "Coroutine profile" and "function profile" describe calling
conventions, not scheduling implementations.

## Current work

The [design notes](docs/design-notes.md) contain the initial audit of ASGI,
including:

- language that could be clarified for ASGI generally;
- shared concepts needed to support multiple execution profiles; and
- behavior that must be specified uniquely for the function profile.

The next milestone is a minimal function-profile-to-ASGI adapter with executable
tests for HTTP streaming, WebSockets, lifespan, backpressure, and disconnects.

## Naming

`asgi-function-profile` is a descriptive working repository name. It does not
settle the eventual name of the interface or claim official ASGI status.
