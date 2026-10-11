"""Force both receive completion orders; timeouts only detect deadlocks."""
from contextlib import contextmanager
import importlib.util
from queue import Queue
from threading import Event, Thread, current_thread
from types import SimpleNamespace
import unittest
from unittest.mock import patch


AVAILABLE = importlib.util.find_spec("channels") is not None


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class ConsumerReceiveRaceTests(unittest.TestCase):
    @contextmanager
    def connection(self, *, pause_dispatch=False, hold=None, stop=None):
        from channels.exceptions import StopConsumer
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer

        dispatch, entered, release = Event(), Event(), Event()
        if not pause_dispatch:
            dispatch.set()
        incoming, queued, handled, result, sent = Queue(), Queue(), Queue(), Queue(), Queue()
        history, futures, readers = [], [], set()

        class Inbox(Queue):
            def put_nowait(self, item):
                super().put_nowait(item)
                queued.put(item[0])  # Signal only after the event is in the inbox.

            def get(self, *args, **kwargs):
                if not dispatch.wait(5):
                    raise AssertionError("test did not release dispatch")
                return super().get(*args, **kwargs)

        inbox = Inbox(maxsize=2)

        class Layer(FunctionChannelLayer):
            def _receive_future(self, channel):
                future = super()._receive_future(channel)
                futures.append(future)
                return future

        with Layer(InMemoryChannelLayer) as layer:
            def acted(source):
                if source == hold:
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError("test did not release handler")
                history.append(source)
                handled.put(source)
                if source == stop:
                    raise StopConsumer()

            class Room(WebsocketConsumer):
                channel_layer = layer
                groups = ("room",)

                def receive(self, text_data=None, bytes_data=None):
                    acted("socket")

                def notice(self, message):
                    acted("channel")

                def disconnect(self, code):
                    acted("disconnect")

            def receive():
                readers.add(current_thread())
                return incoming.get(timeout=5)

            def send(message):
                sent.put(message)
                if message["type"] == "websocket.close":
                    incoming.put({"type": "websocket.disconnect", "code": message["code"]})

            def run():
                try:
                    Room()({"type": "websocket"}, receive, send)
                except BaseException as error:
                    result.put(error)
                else:
                    result.put(None)

            def emit(source):
                if source == "channel":
                    layer.group_send("room", {"type": "notice"})
                elif source == "disconnect":
                    incoming.put({"type": "websocket.disconnect", "code": 1000})
                else:
                    incoming.put({"type": "websocket.receive", "text": "socket"})
                self.assertEqual(queued.get(timeout=5), "socket" if source == "disconnect" else source)

            incoming.put({"type": "websocket.connect"})
            thread = Thread(target=run)
            with patch("asgi_function_profile.channels.Queue", return_value=inbox):
                thread.start()
                try:
                    self.assertEqual(sent.get(timeout=5)["type"], "websocket.accept")
                    yield SimpleNamespace(emit=emit, dispatch=dispatch, entered=entered,
                                          release=release, handled=handled, history=history,
                                          futures=futures, inbox=inbox, thread=thread)
                finally:
                    dispatch.set()
                    release.set()
                    incoming.put({"type": "websocket.disconnect", "code": 1000})
                    thread.join(5)
                    self.assertFalse(thread.is_alive(), "consumer did not stop")
                    self.assertIsNone(result.get(timeout=5))
                    self.assertTrue(all(not reader.is_alive() for reader in readers))
                    self.assertTrue(all(future.done() for future in futures))
                    async def groups():
                        return layer.backend.groups
                    self.assertEqual(layer._call(groups), {})

    def test_both_reads_complete_before_dispatch_in_either_order(self):
        for first, second in (("socket", "channel"), ("channel", "socket")):
            with self.subTest(first=first):
                with self.connection(pause_dispatch=True) as connection:
                    connection.emit(first)
                    connection.emit(second)
                    self.assertEqual(connection.inbox.qsize(), 2)
                    self.assertEqual(connection.history, [])
                    self.assertTrue(connection.futures[0].done())
                    connection.dispatch.set()
                    self.assertEqual(connection.handled.get(timeout=5), first)
                    self.assertEqual(connection.handled.get(timeout=5), second)
                self.assertEqual(connection.history, [first, second, "disconnect"])

    def test_other_receive_completes_while_first_handler_is_waiting(self):
        for first, second in (("socket", "channel"), ("channel", "socket")):
            with self.subTest(first=first):
                with self.connection(hold=first) as connection:
                    connection.emit(first)
                    self.assertTrue(connection.entered.wait(5))
                    connection.emit(second)
                    self.assertEqual(connection.history, [])
                    self.assertTrue(connection.futures[0].done())
                    self.assertFalse(connection.futures[0].cancelled())
                    connection.release.set()
                    self.assertEqual(connection.handled.get(timeout=5), first)
                    self.assertEqual(connection.handled.get(timeout=5), second)
                self.assertEqual(connection.history, [first, second, "disconnect"])

    def test_stopping_connection_leaves_other_completed_event_unhandled(self):
        for first, second in (("socket", "channel"), ("channel", "socket")):
            with self.subTest(first=first):
                with self.connection(pause_dispatch=True, stop=first) as connection:
                    connection.emit(first)
                    connection.emit(second)
                    connection.dispatch.set()
                    connection.thread.join(5)
                    self.assertFalse(connection.thread.is_alive())
                    self.assertEqual(connection.history, [first])
                    self.assertFalse(connection.futures[0].cancelled())
                    self.assertEqual(connection.inbox.get_nowait()[0], second)
                self.assertEqual(connection.history, [first])

    def test_disconnect_leaves_completed_channel_notification_unhandled(self):
        with self.connection(pause_dispatch=True) as connection:
            connection.emit("disconnect")
            connection.emit("channel")
            connection.dispatch.set()
            connection.thread.join(5)
            self.assertFalse(connection.thread.is_alive())
            self.assertEqual(connection.history, ["disconnect"])
            self.assertTrue(connection.futures[0].done())
            self.assertFalse(connection.futures[0].cancelled())
            self.assertEqual(connection.inbox.get_nowait()[0], "channel")
        self.assertEqual(connection.history, ["disconnect"])
