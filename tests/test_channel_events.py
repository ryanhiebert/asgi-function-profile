"""Renewal, cancellable iterators and the comparison's correctness findings."""

import asyncio
import importlib.util
from queue import Queue
from threading import Event, Thread
import unittest
from unittest.mock import patch


AVAILABLE = importlib.util.find_spec("channels") is not None


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class ChannelEventsTests(unittest.TestCase):
    def layer(self, factory=None):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer
        layer = FunctionChannelLayer(factory or InMemoryChannelLayer)
        self.addCleanup(layer.close)
        return layer

    def source(self, layer, **kwargs):
        from asgi_function_profile.channels import ChannelEvents
        source = ChannelEvents(["room", "room"], channel_layer=layer, **kwargs)
        self.addCleanup(source.close)
        return source

    def test_two_iterators_receive_and_cancel_wakes_idle_reader(self):
        layer = self.layer()
        one, two = self.source(layer), self.source(layer)
        layer.group_send("room", {"type": "notice", "value": b"binary"})
        self.assertEqual(next(one), next(two))
        outcome, entered = Queue(), Event()
        def read():
            entered.set()
            try:
                outcome.put(next(one))
            except BaseException as error:
                outcome.put(error)
        thread = Thread(target=read)
        thread.start()
        self.assertTrue(entered.wait(2))
        one.cancel()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(outcome.get(timeout=2), StopIteration)
        one.close(); two.close()
        async def groups():
            return layer.backend.groups
        self.assertEqual(layer._call(groups), {})

    def test_overflow_terminates_instead_of_evicting_control_event(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import ChannelEventOverflow
        delivered = Queue()
        class Backend(InMemoryChannelLayer):
            async def receive(self, channel):
                event = await super().receive(channel)
                delivered.put(event)
                return event
        layer = self.layer(Backend)
        source = self.source(layer, limit=2)
        for event in ({"type": "unsubscribe"}, {"type": "broadcast", "number": 1},
                      {"type": "broadcast", "number": 2}):
            layer.group_send("room", event)
            self.assertEqual(delivered.get(timeout=2), event)
        self.assertTrue(source.stopped.wait(2))
        with self.assertRaisesRegex(ChannelEventOverflow, "buffer full"):
            next(source)
        with self.assertRaises(ChannelEventOverflow):
            next(source)
        source.close()
        layer.group_send("room", {"type": "after-close"})

    def test_backend_failure_wakes_iterator_with_original_error(self):
        from channels.layers import InMemoryChannelLayer
        failure = TimeoutError("backend timeout")
        class Backend(InMemoryChannelLayer):
            async def receive(self, channel):
                raise failure
        source = self.source(self.layer(Backend))
        self.assertTrue(source.stopped.wait(2))
        try:
            next(source)
        except TimeoutError as error:
            self.assertIs(error, failure)
            import traceback
            self.assertIn("receive", [frame.name for frame in traceback.extract_tb(error.__traceback__)])
        else:
            self.fail("backend timeout did not propagate")

    def test_renewal_preserves_idle_membership_and_leaving_is_not_undone(self):
        from channels.layers import InMemoryChannelLayer
        renewed = Event()
        class Backend(InMemoryChannelLayer):
            def __init__(self):
                super().__init__(group_expiry=0.1)
                self.adds = 0
            async def group_add(self, group, channel):
                await super().group_add(group, channel)
                self.adds += 1
                if self.adds > 1:
                    renewed.set()
        layer = self.layer(Backend)
        source = self.source(layer)
        async def age():
            layer.backend.groups["room"][source.channel] = 1
        layer._call(age)
        self.assertTrue(renewed.wait(2))
        layer.group_send("room", {"type": "notice"})
        self.assertEqual(next(source), {"type": "notice"})
        source.membership.discard("room")
        renewed.clear()
        async def barrier():
            # Wait through another renewal tick without any group to re-add.
            await asyncio.sleep(0.15)
            return layer.backend.groups
        self.assertEqual(layer._call(barrier), {})
        self.assertFalse(renewed.is_set())

    def test_renewal_error_terminates_idle_iterator(self):
        from channels.layers import InMemoryChannelLayer
        failure = ConnectionError("renewal failed")
        class Backend(InMemoryChannelLayer):
            def __init__(self):
                super().__init__(group_expiry=0.01)
                self.adds = 0
            async def group_add(self, group, channel):
                self.adds += 1
                if self.adds > 1:
                    raise failure
                await super().group_add(group, channel)
        source = self.source(self.layer(Backend))
        self.assertTrue(source.stopped.wait(2))
        with self.assertRaises(ConnectionError) as raised:
            next(source)
        self.assertIs(raised.exception, failure)


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class ConsumerComparisonTests(unittest.TestCase):
    def test_failed_close_needs_outer_cancellation_to_release_blocked_reader(self):
        from concurrent.futures import ThreadPoolExecutor
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.adapter import FunctionProfileAdapter
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer
        failure = ValueError("handler failed")
        outcome = Queue()
        with FunctionChannelLayer(InMemoryChannelLayer) as layer:
            class Broken(WebsocketConsumer):
                channel_layer = layer
                groups = ("room",)
                def notice(self, event):
                    raise failure
            def application(*args):
                try:
                    Broken()(*args)
                except BaseException as error:
                    outcome.put(error)
            async def probe():
                accepted, receiving, close_failed = asyncio.Event(), asyncio.Event(), asyncio.Event()
                first = True
                async def receive():
                    nonlocal first
                    if first:
                        first = False
                        return {"type": "websocket.connect"}
                    receiving.set()
                    await asyncio.Future()
                async def send(event):
                    if event["type"] == "websocket.accept":
                        accepted.set()
                    elif event["type"] == "websocket.close":
                        close_failed.set()
                        raise OSError("close failed without a disconnect")
                with ThreadPoolExecutor(max_workers=1) as executor:
                    task = asyncio.create_task(FunctionProfileAdapter(application, executor=executor)(
                        {"type": "websocket"}, receive, send))
                    try:
                        await asyncio.wait_for(accepted.wait(), 2)
                        await asyncio.wait_for(receiving.wait(), 2)
                        layer.group_send("room", {"type": "notice"})
                        await asyncio.wait_for(close_failed.wait(), 2)
                        self.assertFalse(task.done())
                    finally:
                        task.cancel()
                        with self.assertRaises(asyncio.CancelledError):
                            await task
                    self.assertIs(await asyncio.to_thread(outcome.get, True, 2), failure)
            asyncio.run(probe())

    def test_awaitables_and_delegated_async_hooks_are_rejected(self):
        from asgi_function_profile.channels import Consumer, JsonWebsocketConsumer, WebsocketConsumer
        class Awaitable:
            def __await__(self):
                yield
        class Custom(Consumer):
            def event(self, message):
                return Awaitable()
        class AsyncConnect(WebsocketConsumer):
            async def connect(self):
                pass
        class AsyncReceive(WebsocketConsumer):
            async def receive(self, **kwargs):
                pass
        class AsyncDisconnect(WebsocketConsumer):
            async def disconnect(self, code):
                pass
        class AsyncJson(JsonWebsocketConsumer):
            async def receive_json(self, content):
                pass
        cases = ((Custom, {"type": "event"}), (AsyncConnect, {"type": "websocket.connect"}),
                 (AsyncReceive, {"type": "websocket.receive", "text": "x"}),
                 (AsyncDisconnect, {"type": "websocket.disconnect", "code": 1000}),
                 (AsyncJson, {"type": "websocket.receive", "text": "{}"}))
        for consumer, message in cases:
            with self.subTest(consumer=consumer), self.assertRaisesRegex(TypeError, "ordinary functions"):
                consumer()({}, lambda: message, lambda event: None)

    def test_database_cleanup_preserves_handler_failure_and_otherwise_raises(self):
        from asgi_function_profile.channels import Consumer
        handler_error, cleanup_error = ValueError("handler"), RuntimeError("cleanup")
        class Broken(Consumer):
            def event(self, event):
                raise handler_error
        # The first cleanup precedes dispatch; the second is the failing finalizer.
        with patch("asgi_function_profile.channels._database_cleanup", side_effect=[None, cleanup_error]):
            with self.assertRaises(ValueError) as raised:
                Broken().dispatch({"type": "event"})
        self.assertIs(raised.exception, handler_error)
        class Ordinary(Consumer):
            def event(self, event):
                pass
        with patch("asgi_function_profile.channels._database_cleanup", side_effect=[None, cleanup_error]):
            with self.assertRaises(RuntimeError) as raised:
                Ordinary().dispatch({"type": "event"})
        self.assertIs(raised.exception, cleanup_error)

    def test_transport_wrapper_receives_handshake_first_and_handles_drain(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import Consumer, FunctionChannelLayer
        with FunctionChannelLayer(InMemoryChannelLayer) as layer:
            incoming, outgoing, result = Queue(), Queue(), Queue()
            seen = []
            class Wrapper(Consumer):
                channel_layer = layer
                def run(self, receive):
                    seen.append(receive()["type"])
                    self.join_group("room")
                    self.send({"type": "websocket.accept"})
                    seen.append(receive()["type"])
                def drain(self, event):
                    self.send({"type": "websocket.close", "code": 1001})
            def send(event):
                outgoing.put(event)
                if event["type"] == "websocket.close":
                    incoming.put({"type": "websocket.disconnect", "code": event["code"]})
            def run():
                try:
                    Wrapper()({"type": "websocket"}, incoming.get, send)
                except BaseException as error:
                    result.put(error)
                else:
                    result.put(None)
            incoming.put({"type": "websocket.connect"})
            thread = Thread(target=run)
            thread.start()
            try:
                self.assertEqual(outgoing.get(timeout=2)["type"], "websocket.accept")
                layer.group_send("room", {"type": "drain"})
                self.assertEqual(outgoing.get(timeout=2), {"type": "websocket.close", "code": 1001})
            finally:
                incoming.put({"type": "websocket.disconnect", "code": 1000})
                thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(result.get(timeout=2))
            self.assertEqual(seen, ["websocket.connect", "websocket.disconnect"])

    def test_socket_reader_inherits_context_after_handshake(self):
        import contextvars
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer
        label = contextvars.ContextVar("handshake-label", default="before")
        seen, sent, result = Queue(), Queue(), Queue()
        with FunctionChannelLayer(InMemoryChannelLayer) as layer:
            class Room(WebsocketConsumer):
                channel_layer = layer
                def connect(self):
                    label.set("after")
                    self.accept()
            first = True
            def receive():
                nonlocal first
                if first:
                    first = False
                    return {"type": "websocket.connect"}
                seen.put(label.get())
                return {"type": "websocket.disconnect", "code": 1000}
            def run():
                try:
                    Room()({"type": "websocket"}, receive, sent.put)
                except BaseException as error:
                    result.put(error)
                else:
                    result.put(None)
            thread = Thread(target=run)
            thread.start()
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertIsNone(result.get(timeout=2))
            self.assertEqual(seen.get(timeout=2), "after")
