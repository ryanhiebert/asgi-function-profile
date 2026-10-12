"""Experimental ordinary Channels consumers and a coroutine layer boundary.

Native-thread only. This is a translated subset, not django-channels itself.
Existing channel backends retain their implementation and wire semantics.
"""

import asyncio
import contextvars
import json
import os
import sys
from concurrent.futures import TimeoutError as FutureTimeout
from functools import update_wrapper
from inspect import isasyncgen, isawaitable
from queue import Empty, Full, Queue
from threading import Event, Lock, RLock, Thread


from channels.exceptions import AcceptConnection, ChannelFull, DenyConnection, MessageTooLarge, StopConsumer


class LayerClosedError(RuntimeError):
    pass


class ChannelEventOverflow(RuntimeError):
    """A slow event iterator was terminated instead of dropping notifications."""


class _BackendError(Exception):
    def __init__(self, error):
        self.error = error


def _result(future, timeout=None):
    try:
        return future.result(timeout)
    except _BackendError as failure:
        raise failure.error from None


def _ordinary(function, *args, **kwargs):
    result = function(*args, **kwargs)
    if isawaitable(result) or isasyncgen(result):
        if hasattr(result, "close"):
            result.close()
        raise TypeError("function-profile handlers must be ordinary functions")
    return result


def _cleanup(function):
    # Cleanup errors matter, but must not replace the active application error.
    failed = sys.exception() is not None
    try:
        function()
    except Exception:
        if not failed:
            raise


class FunctionChannelLayer:
    """Own one coroutine backend and loop; expose completed ordinary calls.

    Construct after fork and close after all consumers finish. backend_factory
    runs on the owned loop: do not pass a backend already used on another loop.
    """

    def __init__(self, backend_factory):
        self.pid = os.getpid()
        self.loop = asyncio.new_event_loop()
        self.lock = Lock()
        self.closed = False
        self.thread = Thread(target=self._serve, name="function-channel-layer")
        self.thread.start()
        try:
            async def create():
                return backend_factory()
            self.backend = self._call(create)
            self.extensions = list(self.backend.extensions)
            self.ChannelFull = ChannelFull
            self.MessageTooLarge = MessageTooLarge
            self.group_expiry = getattr(self.backend, "group_expiry", None)
        except BaseException:
            self.close()
            raise

    def _serve(self):
        asyncio.set_event_loop(self.loop)
        try:
            self.loop.run_forever()
            tasks = asyncio.all_tasks(self.loop)
            for task in tasks:
                task.cancel()
            if tasks:
                self.loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            self.loop.run_until_complete(self.loop.shutdown_asyncgens())
        finally:
            self.loop.close()

    def _submit(self, operation):
        with self.lock:
            if self.closed or self.pid != os.getpid():
                raise LayerClosedError("channel layer closed or used after fork")
            async def run():
                try:
                    return await operation()
                except Exception as error:
                    # Python 3.11's asyncio/concurrent-future conversion copies
                    # builtin TimeoutError, losing its traceback. Carry it intact.
                    raise _BackendError(error) from error
            wrapper = run()
            try:
                return asyncio.run_coroutine_threadsafe(wrapper, self.loop)
            except BaseException:
                wrapper.close()
                raise

    def _call(self, operation, *, cancelled=None):
        future = self._submit(operation)
        try:
            if cancelled is None:
                return _result(future)
            while not cancelled.is_set():
                try:
                    return _result(future, timeout=0.05)
                except FutureTimeout:
                    if future.done():
                        return _result(future)  # Preserve a backend TimeoutError.
            raise InterruptedError("channel receive cancelled")
        except BaseException:
            future.cancel()
            raise

    def _method(self, name, *args, **kwargs):
        async def call():
            return await getattr(self.backend, name)(*args, **kwargs)
        return self._call(call)

    def new_channel(self, prefix="specific"):
        return self._method("new_channel", prefix)

    def send(self, channel, message):
        return self._method("send", channel, message)

    def receive(self, channel, *, cancelled=None):
        return self._call(lambda: self.backend.receive(channel), cancelled=cancelled)

    def _receive_future(self, channel):
        return self._submit(lambda: self.backend.receive(channel))

    def group_add(self, group, channel):
        return self._method("group_add", group, channel)

    def group_discard(self, group, channel):
        return self._method("group_discard", group, channel)

    def group_send(self, group, message):
        return self._method("group_send", group, message)

    def flush(self):
        return self._method("flush")

    def close(self):
        if self.pid != os.getpid():
            raise LayerClosedError("cannot close a channel layer inherited across fork")
        with self.lock:
            if self.closed:
                return
            self.closed = True
        # Cancel receives and close backend connections before stopping its loop.
        # Submission shares the close lock: every accepted task is scheduled
        # before cleanup, so the loop owns cancellation without a future registry.
        async def cleanup():
            tasks = [task for task in asyncio.all_tasks() if task is not asyncio.current_task()]
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            backend = getattr(self, "backend", None)
            if backend is not None and hasattr(backend, "close_pools"):
                await backend.close_pools()
        try:
            asyncio.run_coroutine_threadsafe(cleanup(), self.loop).result()
        finally:
            self.loop.call_soon_threadsafe(self.loop.stop)
            self.thread.join()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        _cleanup(self.close)


class _Groups:
    """Renew and change memberships on the backend loop, in a fixed order."""

    def __init__(self, layer, channel):
        self.layer, self.channel = layer, channel
        self.groups = set()
        self.lock = asyncio.Lock()
        self.renewal = layer._submit(self._renew) if layer.group_expiry else None

    async def _renew(self):
        while True:
            await asyncio.sleep(self.layer.group_expiry / 2)
            async with self.lock:
                for group in self.groups:
                    await self.layer.backend.group_add(group, self.channel)

    def add(self, group):
        async def add():
            async with self.lock:
                await self.layer.backend.group_add(group, self.channel)
                self.groups.add(group)
        self.layer._call(add)

    def discard(self, group):
        async def discard():
            async with self.lock:
                await self.layer.backend.group_discard(group, self.channel)
                self.groups.discard(group)
        self.layer._call(discard)

    def close(self):
        if self.renewal is not None:
            self.renewal.cancel()
        async def discard():
            async with self.lock:
                error = None
                for group in tuple(self.groups):
                    try:
                        await self.layer.backend.group_discard(group, self.channel)
                    except Exception as failure:
                        error = error or failure
                    else:
                        self.groups.discard(group)
                if error is not None:
                    raise error
        if not self.layer.closed:
            self.layer._call(discard)


_END = object()


class ChannelEvents:
    """A cancellable group-event iterator, with bounded local buffering.

    Construct and close on the iterator's owning thread. cancel() is safe from
    another thread and wakes next(). Overflow ends the stream with an error;
    it never silently evicts data or control events. Backend delivery remains
    at most once. The layer is owned by the application's lifespan.
    """

    def __init__(self, groups, *, channel_layer, limit=1024):
        if not isinstance(limit, int) or limit < 1:
            raise ValueError("channel event limit must be a positive integer")
        self.layer = channel_layer
        self.queue = Queue(maxsize=limit)
        self.lock = Lock()
        self.stopped = Event()
        self.error = None
        self.pending = None
        self.closed = False
        self.channel = self.layer.new_channel()
        self.membership = _Groups(self.layer, self.channel)
        try:
            for group in dict.fromkeys(groups):
                self.membership.add(group)
            if self.membership.renewal is not None:
                self.membership.renewal.add_done_callback(self._renewed)
            self._arm()
        except BaseException:
            _cleanup(self.close)
            raise

    def _arm(self):
        with self.lock:
            if self.stopped.is_set():
                return
            future = self.layer._receive_future(self.channel)
            self.pending = future
        future.add_done_callback(self._received)

    def _received(self, future):
        try:
            event = _result(future)
            with self.lock:
                if self.stopped.is_set():
                    return
                self.queue.put_nowait(event)
            self._arm()
        except Full:
            self._finish(ChannelEventOverflow("channel event buffer full; stream terminated"))
        except BaseException as error:
            self._finish(error)

    def _renewed(self, future):
        if not future.cancelled():
            try:
                _result(future)
            except BaseException as error:
                self._finish(error)

    def _finish(self, error=None):
        with self.lock:
            if self.stopped.is_set():
                return
            self.error = error
            self.stopped.set()
            while True:
                try:
                    self.queue.get_nowait()
                except Empty:
                    break
            self.queue.put_nowait(_END)
            pending = self.pending
        if pending is not None:
            pending.cancel()
        if self.membership.renewal is not None:
            self.membership.renewal.cancel()

    def __iter__(self):
        return self

    def __next__(self):
        event = _END if self.stopped.is_set() else self.queue.get()
        if event is _END or self.stopped.is_set():
            if self.error is not None:
                raise self.error
            raise StopIteration
        return event

    def cancel(self):
        self._finish()

    def close(self):
        if self.closed:
            return
        self.closed = True
        self.cancel()
        self.membership.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        _cleanup(self.close)


def _database_cleanup():
    # Framework convenience; never initialize Django just for a bare consumer.
    if "django.db" in sys.modules:
        from django.conf import settings
        if settings.configured:
            from django.db import connections
            connections.close_all()


class Consumer:
    """Dispatch both WebSocket input sources on the invocation thread.

    One helper reads socket events; the shared backend loop reads layer events.
    Each source has at most one undelivered event. Handlers stay on this thread;
    a wrapped transport may also send from its operation threads. Sends are
    serialized and wait for completion. Handlers must cooperate with shutdown.
    """

    channel_layer = None

    @classmethod
    def as_asgi(cls, **initkwargs):
        def application(scope, receive, send):
            consumer = cls(**initkwargs)
            return consumer(scope, receive, send)
        application.consumer_class = cls
        application.consumer_initkwargs = initkwargs
        return update_wrapper(application, cls, updated=())

    def __call__(self, scope, receive, send):
        self.scope = scope
        self.base_send = send
        self._send_lock = RLock()
        self._closed = False
        self.channel_name = self.channel_layer.new_channel() if self.channel_layer else None
        self._membership = _Groups(self.channel_layer, self.channel_name) if self.channel_layer else None
        try:
            if self.channel_layer:
                if scope["type"] != "websocket":
                    raise TypeError("channel-layer multiplexing is only implemented for WebSockets")
                self._run(receive)
            else:
                _ordinary(self.run, receive)
        except StopConsumer:
            pass
        finally:
            try:
                if self._membership is not None:
                    _cleanup(self._membership.close)
            finally:
                _cleanup(_database_cleanup)

    def run(self, receive):
        """Override to wrap an ordinary transport with channel-event dispatch."""
        while True:
            self.dispatch(receive())

    def _run(self, receive):
        inbox = Queue(maxsize=3)  # One event per source, plus a renewal failure.
        stopped, acknowledged, disconnected = Event(), Event(), Event()

        def socket_events():
            try:
                while not stopped.is_set():
                    message = receive()
                    if message["type"] == "websocket.disconnect":
                        disconnected.set()
                    inbox.put_nowait(("socket", message))
                    if disconnected.is_set():
                        return
                    acknowledged.wait()
                    acknowledged.clear()
            except BaseException as error:
                inbox.put_nowait(("error", error))

        def channel_received(future):
            if not stopped.is_set():
                inbox.put_nowait(("channel", future))

        def channel_receive():
            future = self.channel_layer._receive_future(self.channel_name)
            future.add_done_callback(channel_received)
            return future

        reader = None
        pending = None
        close_code = 1000
        first = True
        delivered_socket = False

        def renewed(future):
            if not stopped.is_set() and not future.cancelled():
                try:
                    _result(future)
                except BaseException as error:
                    inbox.put_nowait(("error", error))

        def next_event():
            nonlocal first, pending, delivered_socket, reader
            if first:
                first = False
                # The transport sees its handshake before any layer events.
                return receive()
            if reader is None:
                reader = Thread(target=contextvars.copy_context().run, args=(socket_events,),
                                name="function-consumer-socket")
                reader.start()
                pending = channel_receive()
                if self._membership.renewal is not None:
                    self._membership.renewal.add_done_callback(renewed)
            elif delivered_socket:
                acknowledged.set()
            delivered_socket = False
            while True:
                source, value = inbox.get()
                if source == "error":
                    raise value
                if source == "channel":
                    self.dispatch(_result(value))
                    pending = channel_receive()
                else:
                    delivered_socket = True
                    return value

        try:
            _ordinary(self.run, next_event)
        except StopConsumer:
            raise
        except BaseException:
            close_code = 1011
            raise
        finally:
            stopped.set()
            acknowledged.set()
            if pending is not None:
                pending.cancel()
            try:
                if not disconnected.is_set() and not self._closed:
                    # Ordinary receive has no cancellation API. Closing a real
                    # WebSocket releases its reader via the server's disconnect.
                    _cleanup(lambda: Consumer.send(self, {"type": "websocket.close", "code": close_code}))
            finally:
                if reader is not None and reader.ident is not None:
                    reader.join()

    def dispatch(self, message):
        kind = message.get("type")
        if not isinstance(kind, str) or not kind or kind.replace(".", "_").startswith("_"):
            raise ValueError("invalid event type")
        handler = getattr(self, kind.replace(".", "_"), None)
        if handler is None:
            raise ValueError(f"No handler for message type {kind}")
        _database_cleanup()
        try:
            _ordinary(handler, message)
        finally:
            _cleanup(_database_cleanup)

    def send(self, message):
        with self._send_lock:
            result = self.base_send(message)
            if message["type"] == "websocket.close":
                self._closed = True
            return result

    def join_group(self, group):
        self._membership.add(group)

    def leave_group(self, group):
        self._membership.discard(group)


class TransportConsumer(Consumer):
    """Wrap an ordinary WebSocket transport with channel-event dispatch.

    The provider's get() runs on invocation, not construction. Its layer is
    borrowed; the provider owner closes it after all consumers finish. groups
    uses the same class convention as WebsocketConsumer, with an optional
    constructor override. Named channel handlers and deployment policy belong
    in subclasses. The transport owns socket input and protocol handling.
    """

    groups = ()

    def __init__(self, *, transport, channel_layer_provider, groups=None):
        self.transport = transport
        self.channel_layer_provider = channel_layer_provider
        if groups is not None:
            self.groups = tuple(groups)

    def __call__(self, scope, receive, send):
        self.channel_layer = _ordinary(self.channel_layer_provider.get)
        return super().__call__(scope, receive, send)

    def run(self, receive):
        for group in self.groups:
            self.join_group(group)
        return _ordinary(self.transport, self.scope, receive, self.send)


class WebsocketConsumer(Consumer):
    groups = ()

    def websocket_connect(self, message):
        for group in self.groups:
            self.join_group(group)
        try:
            _ordinary(self.connect)
        except AcceptConnection:
            self.accept()
        except DenyConnection:
            self.close()

    def connect(self):
        self.accept()

    def accept(self, subprotocol=None, headers=None):
        message = {"type": "websocket.accept", "subprotocol": subprotocol}
        if headers is not None:
            message["headers"] = list(headers)
        Consumer.send(self, message)

    def websocket_receive(self, message):
        if message.get("text") is not None:
            _ordinary(self.receive, text_data=message["text"])
        else:
            _ordinary(self.receive, bytes_data=message["bytes"])

    def receive(self, text_data=None, bytes_data=None):
        pass

    def send(self, text_data=None, bytes_data=None, close=False):
        if text_data is not None:
            message = {"type": "websocket.send", "text": text_data}
        elif bytes_data is not None:
            message = {"type": "websocket.send", "bytes": bytes_data}
        else:
            raise ValueError("text_data or bytes_data is required")
        Consumer.send(self, message)
        if close:
            self.close(code=None if close is True else close)

    def close(self, code=None, reason=None):
        message = {"type": "websocket.close"}
        if code is not None:
            message["code"] = code
        if reason is not None:
            message["reason"] = reason
        Consumer.send(self, message)

    def websocket_disconnect(self, message):
        _ordinary(self.disconnect, message["code"])
        raise StopConsumer()

    def disconnect(self, code):
        pass


class JsonWebsocketConsumer(WebsocketConsumer):
    def receive(self, text_data=None, bytes_data=None):
        if text_data is None:
            raise ValueError("JSON sockets require text frames")
        _ordinary(self.receive_json, _ordinary(self.decode_json, text_data))

    def receive_json(self, content):
        pass

    def send_json(self, content, close=False):
        self.send(text_data=_ordinary(self.encode_json, content), close=close)

    @classmethod
    def decode_json(cls, text_data):
        return json.loads(text_data)

    @classmethod
    def encode_json(cls, content):
        return json.dumps(content)
