# ASGI Function Profile

Status: experimental draft

Version: 0.1

## 1. Scope

The ASGI function profile defines an ordinary-function calling convention for
ASGI 3 applications. It incorporates the ASGI 3 base specification and its
protocol specifications by reference, except where this document explicitly
replaces coroutine-specific requirements.

The function profile changes the application calling convention. It does not
change scopes, events, protocol versions, ordering, backpressure, error
behavior, or protocol extensions.

The legacy ASGI 2 two-callable application form is outside the scope of this
profile.

## 2. Profile selection

The component invoking an application must know its profile before invocation.
Selection therefore occurs through deployment or server configuration, not
through the connection scope and not through negotiation with the application.

A function-profile application requires no decorator, adapter declaration, or
coroutine wrapper in application code. A compatibility adapter may translate
the interface internally when deploying it behind a coroutine-profile ASGI
server.

Existing `scope["asgi"]` values retain their ASGI meanings. This profile does
not add a scope key or protocol-version value.

## 3. Calling convention

A function-profile application is one ordinary callable:

```python
def application(scope, receive, send):
    ...
```

Its abstract types are:

```python
Receive = Callable[[], Event]
Send = Callable[[Event], None]
Application = Callable[[Scope, Receive, Send], None]
```

`receive()` waits as necessary and returns the event directly. `send(event)`
waits as necessary and returns when the corresponding coroutine-profile ASGI
call would complete. Exceptions that would be raised while awaiting an
operation are raised by the ordinary call.

Calling and returning from the ordinary application correspond to calling and
awaiting a coroutine-profile ASGI 3 application.

## 4. Mechanical interpretation of ASGI

Coroutine-specific language in the incorporated ASGI specifications is read as
follows:

| Coroutine-profile language | Function-profile interpretation |
| --- | --- |
| async application callable | ordinary application callable |
| call and await the application | call the application |
| awaitable `receive` | ordinary `receive` returning its event |
| awaitable `send` | ordinary `send` returning after the same operation completes |
| exception raised while awaiting | exception raised by the ordinary call |

This interpretation also applies to middleware. Function-profile middleware
receives ordinary `receive` and `send` callables and calls its inner
function-profile application as an ordinary callable. Crossing between
profiles requires an explicit server or adapter boundary.

## 5. Execution environment

A function-profile application is not required to run on the main thread or in
an event loop. The profile does not otherwise prescribe its scheduling model.

An implementation may use native threads, green threads, or another
stack-preserving mechanism. Waiting in `receive()` or `send()` does not imply
that the implementation must keep a native thread blocked.

## 6. Lifespan domains

A lifespan domain consists of one lifespan invocation and the other scope
invocations governed by it. When lifespan is supported:

- startup completes before the other scope invocations in the domain begin;
- shutdown begins after those scope invocations end; and
- each such scope receives the shallow copy of that lifespan's `state`
  namespace required by the ASGI lifespan specification.

For the coroutine profile, a lifespan domain corresponds to the event loop
that processes its scopes.

For the function profile, membership in one lifespan domain does not imply
native-thread affinity. Associated scope invocations may execute on different
threads and may overlap. Values placed in lifespan `state` must be suitable for
access from those invocations. An implementation may offer a stronger affinity
guarantee, but portable applications cannot depend on one.

A compatibility adapter inherits the lifespan domains established by its
underlying ASGI server. A native function-profile server establishes stable
domain boundaries appropriate to its worker and application topology; it does
not create a separate lifespan merely for each request thread.

## 7. Conformance

A conforming function-profile implementation must preserve every observable
requirement of the incorporated ASGI specifications after applying the
mechanical interpretation above and the lifespan rule in this document.

In particular, an intermediary queue is not by itself completion of a
`send(event)` operation. The ordinary call may return only when the underlying
ASGI operation would complete, and it must propagate the corresponding error.

This profile does not add general cancellation, thread-interruption,
concurrent-call, or capacity semantics where ASGI does not already define
them.
