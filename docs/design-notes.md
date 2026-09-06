# ASGI function-profile specification notes

Status: exploratory design notes; not an official ASGI proposal

## Problem and motivation

ASGI usefully separates application code from servers and defines protocol
behavior for HTTP, WebSocket, lifespan, and extensions. Its application
interface, however, requires the coroutine calling convention even when an
application and its dependencies are otherwise organized around ordinary
Python calls.

The goal of this project is to find out whether ASGI's protocol model can
support an equally complete ordinary-function calling convention. In practical
implementations today, concurrency would normally come from operating-system
threads. Free-threaded Python makes that model capable of CPU parallelism as
well as I/O concurrency, but the interface must also work on conventional
Python builds. In the future, a widely adopted green-thread runtime could
provide the same calling convention without dedicating a native thread to each
active execution context.

This is not an argument that ordinary functions never wait, or that coroutine
code necessarily permits useful concurrency. Either convention can prevent
other work from progressing if its runtime does not schedule around a waiting
operation. Conversely, an ordinary call can transparently suspend a green
thread. For that reason, this document avoids using "blocking" and
"non-blocking" as names for the two profiles.

## Design hypothesis

The proposed direction is to extend ASGI with multiple execution profiles over
the same scopes and event protocols:

```python
# Coroutine profile
async def application(scope, receive, send):
    event = await receive()
    await send(event)


# Function profile
def application(scope, receive, send):
    event = receive()
    send(event)
```

The profile names describe their calling conventions, not their scheduling
behavior. The coroutine profile represents suspension explicitly with
awaitables. The function profile uses regular function calls that return their
results directly; those calls may transparently suspend the current execution
context while they wait.

The function profile initially uses operating-system threads. Its interface
should not depend on that implementation: a future server could use green
threads without changing application code.

The long-term preference is one shared ASGI protocol family with multiple
execution profiles, rather than a competing protocol that copies ASGI's event
definitions. The experiment must not assume that ASGI will adopt that model,
however. An independent specification and adapter can establish whether it is
coherent and useful first.

## Terminology

The working terms are:

- **Coroutine profile:** an application is called as a coroutine, and
  `receive()` and `send()` produce awaitables.
- **Function profile:** an application is called as an ordinary function, and
  `receive()` and `send()` return their results directly.
- **Application context:** the configured application, middleware, lifespan,
  and shared lifespan state that form one deployment unit within a worker.
- **Execution context:** the task, native thread, green thread, or equivalent
  context running one scope invocation.

"Stackful profile" was considered but rejected as the primary name. It
describes an observable programming model, but it can be implemented by a
runtime whose own design is described as stackless. "Synchronous profile" is
familiar shorthand, but too easily conflates an ordinary calling convention
with blocking a native thread.

The name `asgi-function-profile` is similarly only a descriptive repository
name. It does not claim official ASGI status or settle the eventual name of an
independent interface.

## Relationship to WSGI and ASGI

### This is not WSGI 2

WSGI is centered on an HTTP request/response exchange and an iterable response
body. The function profile instead retains ASGI's scopes and bidirectional
events. This is necessary for WebSockets, lifespan, disconnect notification,
request-body streaming, response backpressure, and protocol extensions.

"All the features of ASGI" means that an ASGI protocol specification should be
expressible in either execution profile without changing its scope and event
shapes. It does not mean that every current implementation-specific extension
will work automatically; extensions that expose awaitables, event-loop objects,
file descriptors, or other runtime resources require an explicit audit.

### Why retain `receive` and `send` callables

A response generator is attractive for a simple, one-way HTTP response, but it
does not by itself model ASGI's full-duplex conversation. The server must be
able to deliver inbound events even when the application has no outbound event
to yield, and the application may need to receive and send concurrently. A
single generator resumption/yield handshake also becomes awkward when child
execution contexts participate in a scope.

Separate `receive` and `send` capabilities provide:

- independent inbound and outbound flow;
- a natural point at which the server can apply backpressure;
- uniform treatment of HTTP, WebSocket, and lifespan events;
- middleware that can wrap either direction independently; and
- a possible concurrency model for application-created child execution
  contexts.

The choice of callables is therefore not inherently coroutine-specific. The
function profile should initially preserve ASGI's event API and change only how
those calls suspend and return. A generator-based convenience layer could be
built on top for protocols where it fits.

### Compatibility target

The first implementation should be an adapter that lets a function-profile
application run behind an existing ASGI server. That establishes compatibility
with real protocol handling while keeping the experimental surface small. A
native function-profile server can follow if the adapter exposes limitations
that are inherent to crossing between execution models.

Profile selection must be explicit. Automatically treating a callable as one
profile based only on `inspect.iscoroutinefunction()` is not reliable for
decorators, callable instances, middleware, or wrappers.

## Audit structure

The issues below fall into three categories: clarifications that would improve
ASGI generally, changes needed in the shared specification to support execution
profiles, and behavior unique to the function profile.

## Clarify or improve ASGI generally

These issues already exist in coroutine ASGI and can be addressed independently
of the function profile.

- Define what returning from `send()` guarantees, including its relationship to
  buffering and backpressure.
- Specify whether concurrent `send()` or `receive()` calls are allowed and how
  message ordering works.
- Define ownership and mutation rules for scopes and event dictionaries.
- State whether `receive` and `send` remain valid after the application returns.
- Make behavior after disconnect consistent, particularly whether a subsequent
  `send()` is a no-op or raises an exception.
- State explicitly that shallow copies of lifespan `state` still contain shared
  values.
- Distinguish an unsupported scope, especially lifespan, from a genuine
  application startup failure.
- Clarify the use of "connection" when referring to a physical socket, HTTP
  request, HTTP/2 stream, or application scope.
- Resolve the conflict between lists, tuples, and arbitrary synchronous
  `Iterable` values in scopes and messages.
- Resolve the distinction between a missing optional key and a key whose value
  is `None`, including in the canonical typing definitions.
- Clarify which events must be serializable and how resource-bearing extensions,
  such as zero-copy sends containing file descriptors, fit that requirement.
- State that the same application and middleware objects may handle overlapping
  scopes.

## Generalize ASGI to support execution profiles

These concepts belong in the shared or base specification rather than either
individual execution profile.

### Application context

Define an **application context** (or **lifespan domain**) independently of event
loops and operating-system threads. An application context contains:

- One configured application and middleware stack.
- One lifespan session.
- One lifespan-state namespace.
- All scope invocations that share that lifespan and state.

A multiprocess server normally has one application context in each worker
process. A coroutine server may have one per event loop. A threaded
implementation of the function profile normally has one per worker process, not
one per request thread.

### Execution context

Define an **execution context** as the context in which one scope invocation
runs. An execution context may be an asyncio task, an operating-system thread,
or a green thread, depending on the selected profile and server.

### Lifespan

Replace the generic "once per event loop/thread" wording with "once per
application context." Each profile can then explain how its runtime maps
application contexts onto event loops, processes, threads, or other schedulers.

### Profile selection

The server must know the execution profile before invoking the application, so
scope metadata cannot perform the initial negotiation. Selection must happen
out of band through deployment configuration, standardized application
metadata, or an explicit adapter. Servers should not rely solely on coroutine
function introspection, which is unreliable around decorators and callable
objects.

The selected profile can still be exposed to applications and middleware:

```python
scope["asgi"] = {
    "version": "4.0",
    "spec_version": "2.5",
    "execution": "coroutine",  # or "function"
}
```

The exact names and version numbers are placeholders.

### Shared lifecycle rules

The base specification should define:

- Which scopes belong to an application context and share lifespan state.
- When a scope invocation begins and ends.
- When server-provided `receive` and `send` capabilities become invalid.
- That work using those capabilities must not outlive the invocation.
- General disconnect and cleanup obligations, while allowing profiles to define
  different interruption mechanisms.

## Rules unique to the function profile

These requirements describe regular functions and their threaded or
green-threaded execution. They should live in the function profile rather than
the generic ASGI protocol specifications.

### Calling convention

- The application is an ordinary callable.
- `receive()` returns an event directly, after waiting if necessary.
- `send(event)` returns `None` directly, after waiting if necessary, or raises
  an exception.
- Waiting may transparently suspend the current execution context; it does not
  imply that an operating-system thread must remain blocked.

### Parallel invocation

- Separate scope invocations may run simultaneously on different operating-system
  threads.
- Applications, middleware, and shared lifespan values must tolerate true
  parallel access.
- A server may serialize invocations for debugging or compatibility, but a
  portable application cannot rely on serialization.

### Callable affinity and concurrency

- `receive` and `send` may be transferred to child execution contexts belonging
  to the same scope.
- At most one `receive()` may be outstanding for a scope.
- The application must serialize calls to `send()` unless a future version
  defines concurrent-send ordering.
- The server must permit these calls from an execution context other than the
  one that initially entered the application.
- Calls for different scopes may occur simultaneously.

### Thread affinity

- Initial implementations will normally keep an invocation on one native
  thread, supporting existing thread-affine synchronous libraries.
- Portable applications should not depend on native-thread identity unless the
  server advertises a thread-affinity capability.
- This preserves a path to green-thread implementations that may not map one
  logical execution context permanently to one native thread.

### Context-local state

- Each scope invocation should begin with an independent
  `contextvars.Context`.
- Context changes in one scope must not leak into another.
- Lifespan-to-request data should pass through `scope["state"]`, not accidental
  thread-context inheritance.
- Raw `threading.local()` behavior is not portable to all prospective green-thread
  implementations.

### Lifespan state and parallelism

- Each scope receives its own top-level shallow copy of lifespan state.
- Values inside that mapping may be accessed simultaneously by multiple scopes.
- Shared values must therefore be concurrency-safe.
- Pools, registries, immutable configuration, and synchronized services are
  appropriate shared values; unsynchronized thread-affine connections are not.

### Backpressure and message ownership

- `send()` blocks when the server's bounded output capacity is exhausted.
- Returning means the server has accepted the event; it does not mean the peer
  has received the bytes.
- An event becomes server-owned when `send()` begins, and the application must
  not mutate it afterward.
- An event returned by `receive()` becomes application-owned.
- Applications treat the original scope as read-only. Middleware copies it
  before making changes.

### Disconnect and cancellation

- Disconnect wakes an outstanding `receive()`.
- A blocked or subsequent `send()` fails with a defined disconnection exception.
- The server may discard an eventual application result after disconnect.
- An operating-system thread executing arbitrary Python code cannot be safely
  terminated.
- Cancellation is therefore cooperative except at server-provided blocking
  operations.
- Future green-thread implementations may provide stronger interruption at
  defined suspension points, but portable applications cannot assume arbitrary
  interruption.

### Child execution lifetime

- Application-created child threads or green threads must finish or be cancelled
  before the main application callable returns.
- `receive` and `send` become invalid when the application returns.
- Calls after return fail.
- The server is not responsible for discovering or joining arbitrary
  application-created threads.

### Shutdown

- Server shutdown unblocks outstanding server-provided `receive()` and `send()`
  calls.
- It cannot necessarily interrupt database, filesystem, lock, extension-module,
  or CPU-bound application operations.
- Implementations therefore need a graceful-shutdown interval followed by a
  worker-level termination policy.

### Capacity and admission control

This is primarily implementation guidance rather than a wire-protocol rule:

- A fixed-size thread pool can be occupied indefinitely by WebSockets and other
  long-lived scopes.
- An unbounded native thread per scope can exhaust memory or scheduler capacity.
- Servers should expose admission limits and may use separate capacity limits
  for short HTTP requests and long-lived scopes.
- The ASGI specification should permit servers to reject or defer scopes when
  their configured execution capacity is exhausted.

## Experimental validation

The proposal should be judged against executable behavior rather than only an
interface sketch. A minimal adapter and conformance suite should demonstrate:

- an HTTP request whose body arrives in multiple events;
- a response streamed in multiple events with bounded buffering;
- a disconnect that wakes or fails the relevant operation;
- a long-lived, bidirectional WebSocket scope;
- lifespan startup, shared state, and shutdown;
- multiple scopes running with genuine overlap on native threads;
- isolation of `contextvars` between scopes;
- well-defined behavior when the application returns with outstanding work;
  and
- at least one ASGI extension, to test whether the profile boundary composes.

The adapter should expose ordinary calls to application code without requiring
that code to invoke an event loop, submit work to a thread pool, or use an
async-to-sync bridge itself. Internally, the adapter may use those mechanisms to
connect to an ASGI server.

Performance is relevant, particularly capacity consumption by long-lived
scopes, but raw throughput is not the first success criterion. The first test is
whether the semantics are complete, predictable, and implementable without
unbounded queues or hidden loss of backpressure.

## Open design questions

- Is the function profile best expressed as an ASGI version, an ASGI extension,
  standardized application metadata, or an adjacent experimental
  specification?
- What exact exception types represent peer disconnect, server shutdown,
  expired capabilities, and capacity rejection?
- Should the base contract allow one sender and one receiver in different child
  execution contexts, or should the first version restrict both calls to the
  application invocation's original context?
- Does a function-profile server guarantee native-thread affinity, advertise it
  as an optional capability, or leave it entirely outside the contract?
- How should deadlines and cooperative cancellation be exposed without tying
  the profile to a particular scheduler?
- Which existing ASGI extensions rely on coroutine- or event-loop-specific
  behavior despite using generic event dictionaries?
- What is the smallest application-context definition that makes lifespan
  portable across event loops, native threads, green threads, and worker
  processes?
- Can a bidirectional adapter preserve backpressure and cancellation faithfully
  enough to serve as a reference implementation?

## Adoption strategy

This repository should first operate as an independent experiment. The intended
sequence is:

1. Publish a draft with explicit unresolved questions.
2. Build the adapter and conformance tests.
3. Exercise it with a small framework or realistic application.
4. Seek implementation feedback from the broader Python threading and web
   communities.
5. Approach ASGI maintainers with working evidence and a narrowly scoped
   integration proposal.

The initial public claim should be "an experimental function execution profile
using ASGI scopes and events," not that the work is already an ASGI standard.
If upstream integration is not appropriate, the same evidence can support an
independent interface without changing the experiment's technical value.

## Suggested proposal boundary

The initial function-profile proposal should focus on:

1. Application contexts and lifespan.
2. Explicit execution-profile selection.
3. The regular-function application calling convention.
4. Parallel invocation and shared-state requirements.
5. Callable affinity and single-reader/single-writer rules.
6. Context isolation.
7. Message ownership and backpressure.
8. Disconnect, completion, and shutdown behavior.

Tuple/list inconsistencies, optional typing, serializability conflicts, and
general extension cleanup are worth addressing in ASGI, but they should remain
separate proposals so they do not obscure the execution-profile change.

## Reference material

- [ASGI main specification](https://asgi.readthedocs.io/en/latest/specs/main.html)
- [ASGI HTTP and WebSocket specification](https://asgi.readthedocs.io/en/latest/specs/www.html)
- [ASGI lifespan specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html)
- [ASGI extensions](https://asgi.readthedocs.io/en/latest/extensions.html)
- [asgiref repository](https://github.com/django/asgiref)
- [Python free-threading HOWTO](https://docs.python.org/3/howto/free-threading-python.html)
