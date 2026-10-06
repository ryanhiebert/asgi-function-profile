# ASGI function-profile specification notes

Status: exploratory design notes; not an official ASGI proposal

## Lifespan domains

A **lifespan domain** consists of one lifespan invocation and the other scope
invocations governed by it. When lifespan is supported:

- startup completes before the other scope invocations in the domain begin;
- shutdown begins after those scope invocations end; and
- each such scope receives the existing shallow copy of that lifespan's
  `state` namespace.

For the coroutine form, a lifespan domain corresponds to the event loop that
processes its scopes, preserving ASGI's existing affinity rule.

For the function profile, membership in the same lifespan domain does not
imply execution on the same native thread. Associated scope invocations may
execute on different threads and may overlap. Values placed in lifespan
`state` must therefore be suitable for access from those invocations. An
implementation may offer a stronger affinity guarantee, but portable
applications cannot depend on one.

A function-profile adapter inherits the lifespan domains established by its
underlying ASGI server. A native function-profile server chooses stable domain
boundaries appropriate to its worker and application topology; it does not
create a separate lifespan merely for each request thread.

## Execution environment

A function-profile application is invoked as an ordinary callable. It is not
required to run on the main thread or in an event loop. The profile otherwise
imposes no additional scheduling model.

## Problem and motivation

ASGI usefully separates application code from servers and defines protocol
behavior for HTTP, WebSocket, lifespan, and extensions. Its application
interface, however, requires the coroutine calling convention even when an
application and its dependencies are otherwise organized around ordinary
Python calls.

The goal of this project is to find out whether ASGI's protocol model can
support an equally complete ordinary-function calling convention. The current
experiments use both operating-system threads and gevent greenlets. The latter
demonstrates the calling convention without dedicating a native thread to each
active execution context. Free-threaded Python also motivates the native-thread
model's potential for CPU parallelism, but that has not been validated here;
the interface must also work on conventional Python builds.

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

The initial implementation used operating-system threads. The gevent experiment
now uses the same interface and adapter without changing application code,
providing evidence that the profile is independent of that initial scheduler.

The coroutine ASGI specification remains the complete semantic specification.
The function profile is a small transformation of its application calling
convention, plus explicit replacements for the few runtime assumptions that
cannot be translated mechanically. The profiles therefore do not need to be
independent specifications over a newly invented common base.

An independent profile and adapter can establish whether that transformation
is coherent and useful without first restructuring ASGI itself.

## Terminology

The working terms are:

- **Coroutine profile:** an application is called as a coroutine, and
  `receive()` and `send()` produce awaitables.
- **Function profile:** an application is called as an ordinary function, and
  `receive()` and `send()` return their results directly.
- **Lifespan domain:** one lifespan invocation and the other scope invocations
  governed by it.

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
single generator resumption/yield handshake makes that full-duplex exchange
awkward.

Separate `receive` and `send` capabilities provide:

- independent inbound and outbound flow;
- a natural point at which the server can apply backpressure;
- uniform treatment of HTTP, WebSocket, and lifespan events;
- middleware that can wrap either direction independently; and
- direct correspondence with the existing ASGI event API.

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

The initial adapter supplies the profile boundary explicitly by wrapping a
function-profile application in a coroutine-profile ASGI application. The
proposal therefore does not need profile negotiation, a new scope key, or
callable introspection.

## Audit rule

This experiment does not try to clarify or generalize ASGI as a prerequisite.
Instead, it takes coroutine ASGI as canonical and asks of each statement:

> Can the function form preserve this requirement merely by replacing
> coroutine invocation and awaitable completion with ordinary calls and direct
> returns?

If so, the profile incorporates the requirement unchanged through the following
mechanical translation:

| Coroutine specification | Function profile |
| --- | --- |
| async application callable | ordinary application callable |
| call and await the application | call the application |
| awaitable `receive` | ordinary `receive` returning the event |
| awaitable `send` | ordinary `send` returning after the same operation completes |
| exception raised by awaiting a call | exception raised by the ordinary call |

Removing `async` and `await` does not change event shapes, ordering,
backpressure, error behavior, scope lifetime, or any protocol-specific
requirement. Existing ambiguities are inherited too; they are not enlarged into
this proposal merely because they may also be worth resolving in ASGI.

## Translation audit

### Base ASGI specification

The base specification's profile-specific wording is mechanical:

- The overview describes applications as asynchronous callables running as
  `async`/`await`-compatible coroutines on the main thread and in an event loop.
  The function profile replaces that execution declaration with an ordinary
  callable and makes no event-loop or main-thread guarantee.
- The Applications section's coroutine signature and awaitable `receive` and
  `send` definitions map directly to the function signature and calls.
- Middleware's awaitable callables and Error Handling's references to errors
  raised from awaitables map directly to ordinary calls.

These substitutions need to be stated, but they do not require a new shared
specification or new semantics.

The legacy ASGI 2 two-callable application form is not part of the initial
profile. The function profile corresponds to the ASGI 3 single-callable form.

### Protocol specifications

The HTTP, WebSocket, and TLS scope and event specifications contain no
event-loop or awaitable requirements and do not require semantic changes. Their
calls are governed by the base application's calling convention. The same is
true for protocol extensions unless an individual extension explicitly exposes
an event loop, awaitable, or another runtime-affine object; such extensions can
be evaluated individually without changing the core profile.

### Lifespan is not mechanical

The lifespan specification makes a stronger promise than coroutine syntax:

- lifespan runs once per event loop that processes requests;
- lifespan and its requests run in the same event loop; and
- lifespan state may consequently contain event-loop-affine resources.

Replacing "event loop" with "thread" is not equivalent. A function-profile
server may use many request threads for one application deployment, and running
a separate lifespan on every transient or pooled request thread would change
both resource ownership and observable startup and shutdown behavior.

This is the one substantive translation problem found by the audit. The
lifespan-domain rule at the beginning of this document resolves it without
requiring the coroutine specification to be refactored around general
application or execution contexts.

## Deferred ASGI issues

The earlier audit identified several worthwhile questions: send completion,
concurrent calls and ordering, object mutation, capability lifetime,
post-disconnect behavior, optional values, serializability, and overlapping
scope invocations. None is caused by translating the coroutine interface into
ordinary calls. They should remain independent ASGI work rather than expand
the initial function-profile proposal.

## Candidate function-profile wording

These are the additions to the incorporated coroutine specification that the
initial function profile is expected to need.

### Calling convention

- The application is an ordinary callable.
- `receive()` returns an event directly, after waiting if necessary.
- `send(event)` returns directly when the corresponding coroutine-profile call
  would complete, or raises the corresponding exception.
- Waiting may transparently suspend the current execution context; it does not
  imply that an operating-system thread must remain blocked.

### Lifespan

The lifespan-domain rule replaces the lifespan specification's event-loop
affinity rule. In particular, the function profile does not mechanically
substitute "thread" for "event loop."

## Experimental validation

The proposal should be judged against executable behavior rather than only an
interface sketch. A minimal adapter and conformance suite should demonstrate:

- an HTTP request whose body arrives in multiple events;
- a response streamed in multiple events with bounded buffering;
- a disconnect that wakes or fails the relevant operation;
- a long-lived, bidirectional WebSocket scope;
- lifespan startup, shared state, and shutdown;
- multiple scopes making progress through the adapter; and
- at least one ASGI extension, to test whether the profile boundary composes.

The adapter should expose ordinary calls to application code without requiring
that code to invoke an event loop, submit work to a thread pool, or use an
async-to-sync bridge itself. Internally, the adapter may use those mechanisms to
connect to an ASGI server.

Performance is relevant, but raw throughput is not the first success criterion.
The first test is whether the adapter preserves the coroutine specification's
observable behavior, particularly send completion and backpressure. Admission
queues and resource limits are separate implementation policies; the profile
does not require a fixed concurrency limit.

## Reference adapter result

The initial adapter runs each scope invocation in an executor worker. Its
ordinary `receive()` and `send()` callables submit the corresponding awaitable
operation to the ASGI server's event loop and wait for that operation itself to
complete. No intermediary queue is treated as completion.

Executable tests demonstrate delayed receive, send backpressure, exception
propagation, streaming HTTP messages, bidirectional WebSockets, lifespan,
overlapping scopes, and release of a blocked bridge operation when the outer
ASGI invocation is cancelled. The same adapter and an ordinary-function echo
application have also run successfully behind Uvicorn. Subsequent experiments
exercise unchanged synchronous Django views and authenticated WebSockets through
native-thread and gevent-backed Uvicorn workers. See the [README](../README.md)
for the current results, test commands, and backend-specific limitations. The
ASGI extension conformance example listed above remains outstanding.

The native-thread Gunicorn worker now uses a reusable pool that grows on demand
with a high fail-fast ceiling instead of a small fixed pool. This removes waiting
for application slots when idle sockets occupy threads, without changing the
adapter or profile. Thread creation
errors propagate; reaching the ceiling raises a clear capacity error instead
of admitting more work into a waiting queue. Shutdown still joins started threads. Resource usage and performance need
production-shaped measurements.

Closing the bridge can release a thread waiting in `receive()` or `send()`. It
cannot interrupt arbitrary Python code executing between those calls. Long-lived
scopes also occupy executor capacity in this native-thread implementation.
Those are implementation constraints to measure and document, not new profile
semantics unless further experiments show that ASGI behavior cannot otherwise
be preserved.

## Open design questions

- Which existing ASGI extensions rely on coroutine- or event-loop-specific
  behavior despite using generic event dictionaries?
- Which ASGI extension should be the first conformance example?
- Do native-server or framework experiments expose another non-mechanical
  difference that the adapter does not?

## Adoption strategy

This repository operates as an independent experiment. The original intended
sequence is retained below as adoption context, not the current task list.
The draft, adapter, and realistic Django experiments now exist; extension
conformance remains incomplete. The current direction is production-shaped
Django compatibility and performance validation after removing the native
worker's small fixed pool. Publishing and outreach require a separate decision.

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

1. Incorporating the ASGI 3 coroutine specification by reference.
2. The mechanical translation to an ordinary-function calling convention.
3. Removing the coroutine profile's event-loop and main-thread requirements.
4. Replacing lifespan's event-loop affinity rule.
5. Demonstrating semantic preservation with an adapter and tests.

All other ASGI clarification and execution-policy questions remain separate
unless implementation proves that a mechanical translation cannot preserve an
existing requirement.

## Reference material

- [ASGI main specification](https://asgi.readthedocs.io/en/latest/specs/main.html)
- [ASGI HTTP and WebSocket specification](https://asgi.readthedocs.io/en/latest/specs/www.html)
- [ASGI lifespan specification](https://asgi.readthedocs.io/en/latest/specs/lifespan.html)
- [ASGI extensions](https://asgi.readthedocs.io/en/latest/extensions.html)
- [asgiref repository](https://github.com/django/asgiref)
- [Python free-threading HOWTO](https://docs.python.org/3/howto/free-threading-python.html)
- [Mitti](https://github.com/grandimam/mitti), related exploratory framework
  work pursuing ordinary synchronous application code over an ASGI boundary
