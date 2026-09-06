# ASGI function-profile specification notes

Status: exploratory design notes

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
