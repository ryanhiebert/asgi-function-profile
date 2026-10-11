"""Independent function Channels experiment; no application-specific fixtures."""
import asyncio
import contextvars
import importlib.util
import json
import os
from queue import Queue
from threading import Event, Thread, get_ident
import unittest

AVAILABLE = importlib.util.find_spec("channels") is not None


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class ChannelLayerTests(unittest.TestCase):
    def layer(self, **options):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer
        layer = FunctionChannelLayer(lambda: InMemoryChannelLayer(**options))
        self.addCleanup(layer.close)
        return layer

    def test_capacity_group_drop_discard_and_bytes(self):
        layer = self.layer(capacity=1)
        channel = layer.new_channel()
        layer.group_add("one", channel)
        message = {"type": "notice", "data": b"binary"}
        layer.send(channel, message)
        with self.assertRaises(layer.ChannelFull):
            layer.send(channel, message)
        layer.group_send("one", {"type": "dropped"})
        self.assertEqual(layer.receive(channel), message)
        layer.group_discard("one", channel)
        layer.group_send("one", message)
        layer.flush()

    def test_receive_cancel_recovery_and_loop_shutdown(self):
        layer = self.layer()
        channel = layer.new_channel()
        stop = Event()
        received = Queue()
        def wait():
            try:
                layer.receive(channel, cancelled=stop)
            except BaseException as error:
                received.put(error)
        thread = Thread(target=wait)
        thread.start()
        stop.set()
        thread.join(2)
        self.assertFalse(thread.is_alive())
        self.assertIsInstance(received.get(timeout=1), InterruptedError)
        layer.send(channel, {"type": "recovered"})
        self.assertEqual(layer.receive(channel), {"type": "recovered"})
        layer.close()
        self.assertFalse(layer.thread.is_alive())
        self.assertTrue(layer.loop.is_closed())

    def test_backend_errors_and_closed_layer(self):
        from asgi_function_profile.channels import LayerClosedError
        layer = self.layer()
        with self.assertRaises(AssertionError):
            layer.send("one", "not a dict")
        layer.close()
        with self.assertRaises(LayerClosedError):
            layer.new_channel()

    def test_pending_receive_released_on_layer_close(self):
        layer = self.layer()
        channel = layer.new_channel()
        outcome = Queue()
        def receive():
            try:
                outcome.put(layer.receive(channel))
            except BaseException as error:
                outcome.put(error)
        reader = Thread(target=receive)
        reader.start()
        layer.close()
        reader.join(2)
        self.assertFalse(reader.is_alive())
        self.assertIsInstance(outcome.get(timeout=1), BaseException)


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class ConsumerTests(unittest.TestCase):
    def test_native_group_events_context_and_cleanup(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, JsonWebsocketConsumer
        tag = contextvars.ContextVar("tag", default="missing")
        tag.set("connection")
        with FunctionChannelLayer(InMemoryChannelLayer) as layer:
            incoming = Queue()
            sent = Queue()
            contexts, handler_threads = [], []
            disconnected = Event()
            class Echo(JsonWebsocketConsumer):
                channel_layer = layer
                groups = ("room",)
                def connect(self):
                    handler_threads.append(get_ident())
                    self.accept()
                def receive_json(self, content):
                    handler_threads.append(get_ident())
                    contexts.append(tag.get())
                    tag.set("client-event")
                    self.channel_layer.group_send("room", {"type": "room.notice", "value": content})
                def room_notice(self, event):
                    handler_threads.append(get_ident())
                    contexts.append(tag.get())
                    tag.set("layer-event")
                    self.send_json(event["value"])
                def disconnect(self, code):
                    handler_threads.append(get_ident())
                    contexts.append(tag.get())
                    disconnected.set()
            incoming.put({"type": "websocket.connect"})
            def run():
                Echo.as_asgi()({"type": "websocket"}, incoming.get, sent.put)
            ctx = contextvars.copy_context()
            thread = Thread(target=lambda: ctx.run(run))
            thread.start()
            self.assertEqual(sent.get(timeout=2)["type"], "websocket.accept")
            incoming.put({"type": "websocket.receive", "text": '{"value": 7}'})
            self.assertEqual(json.loads(sent.get(timeout=2)["text"]), {"value": 7})
            self.assertEqual(contexts, ["connection", "client-event"])
            incoming.put({"type": "websocket.disconnect", "code": 1000})
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertTrue(disconnected.is_set())
            self.assertEqual(contexts[-1], "layer-event")
            self.assertEqual(handler_threads, [thread.ident] * 4)
            async def groups():
                return layer.backend.groups
            self.assertEqual(layer._call(groups), {})

    def test_send_waits_and_preserves_exception(self):
        from asgi_function_profile.channels import WebsocketConsumer
        entered, release, done = Event(), Event(), Event()
        failure = OSError("transport")
        result = Queue()
        class Echo(WebsocketConsumer):
            def connect(self):
                self.accept()
                done.set()
        def send(message):
            entered.set()
            release.wait(2)
            raise failure
        def run():
            try:
                Echo()({"type": "websocket"}, lambda: {"type": "websocket.connect"}, send)
            except BaseException as error:
                result.put(error)
        thread = Thread(target=run)
        thread.start()
        self.assertTrue(entered.wait(2))
        self.assertFalse(done.is_set())
        release.set()
        thread.join(2)
        self.assertIs(result.get(timeout=1), failure)

    def test_async_handler_rejected_and_bad_type(self):
        from asgi_function_profile.channels import Consumer
        class Wrong(Consumer):
            async def event(self, message):
                pass
        for message, error in [({"type": "event"}, TypeError), ({"type": "_private"}, ValueError)]:
            with self.assertRaises(error):
                Wrong()({}, lambda: message, lambda message: None)

    def test_read_ahead_is_bounded_while_handler_waits(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer
        entered, release, layer_received = Event(), Event(), Event()
        reads = []
        class Backend(InMemoryChannelLayer):
            async def receive(self, channel):
                message = await super().receive(channel)
                reads.append("layer")
                layer_received.set()
                return message
        with FunctionChannelLayer(Backend) as layer:
            incoming, sent = Queue(), Queue()
            incoming.put({"type": "websocket.connect"})
            class Room(WebsocketConsumer):
                channel_layer = layer
                groups = ("room",)
                def receive(self, **kwargs):
                    entered.set()
                    release.wait(3)
                def notice(self, event):
                    pass
            def receive():
                message = incoming.get()
                reads.append("socket")
                return message
            thread = Thread(target=lambda: Room()({"type": "websocket"}, receive, sent.put))
            thread.start()
            try:
                sent.get(timeout=2)
                incoming.put({"type": "websocket.receive", "text": "wait"})
                self.assertTrue(entered.wait(2))
                for i in range(20):
                    incoming.put({"type": "websocket.receive", "text": str(i)})
                    layer.group_send("room", {"type": "notice", "number": i})
                self.assertTrue(layer_received.wait(2))
                self.assertEqual(reads.count("socket"), 2)
                self.assertEqual(reads.count("layer"), 1)
            finally:
                incoming.put({"type": "websocket.disconnect", "code": 1000})
                release.set()
                thread.join(3)
            self.assertFalse(thread.is_alive())


SERVER = AVAILABLE and importlib.util.find_spec("uvicorn_worker") and importlib.util.find_spec("websockets")


@unittest.skipUnless(SERVER, "requires channels, gunicorn and WebSocket extras")
class ChannelsServerTests(unittest.TestCase):
    def start(self, **env):
        from deployment_support import NativeServer
        # Memory-only tests must not accidentally use infrastructure configuration.
        server = NativeServer(target="channels_server:application", env={"ASGI_FUNCTION_REDIS_URL": "", **env})
        self.addCleanup(server.close)
        return server

    def socket(self, server):
        from websockets.sync.client import connect
        return connect(f"ws://127.0.0.1:{server.port}/room", proxy=None, open_timeout=5)

    def test_two_idle_connections_group_dispatch_and_graceful_shutdown(self):
        import urllib.request
        from websockets.exceptions import ConnectionClosed
        server = self.start()
        with self.socket(server) as one, self.socket(server) as two:
            json.loads(one.recv(timeout=2)); json.loads(two.recv(timeout=2))
            one.send('{"value": 4}')
            self.assertEqual(json.loads(one.recv(timeout=2)), {"value": 4})
            self.assertEqual(json.loads(two.recv(timeout=2)), {"value": 4})
            self.assertEqual(urllib.request.urlopen(f"http://127.0.0.1:{server.port}/", timeout=2).read(), b"ok")
            server.stop()
            for socket in (one, two):
                with self.assertRaises(ConnectionClosed) as error:
                    socket.recv(timeout=2)
                self.assertEqual(error.exception.rcvd.code, 1012)
            self.assertTrue((server.path / "consumer-disconnected").exists())

    @unittest.skipUnless(os.environ.get("ASGI_FUNCTION_REDIS_URL"), "requires disposable Redis")
    def test_existing_channels_publisher_reaches_two_worker_processes_and_drains(self):
        import subprocess
        import sys
        from uuid import uuid4
        from websockets.exceptions import ConnectionClosed
        url = os.environ["ASGI_FUNCTION_REDIS_URL"]
        prefix = "function-channels-test:" + uuid4().hex
        env = {"ASGI_FUNCTION_REDIS_URL": url, "ASGI_FUNCTION_CHANNEL_PREFIX": prefix,
               "ASGI_FUNCTION_CHANNEL_GROUP": "shared"}
        one, two = self.start(**env), self.start(**env)
        publisher = '''
import asyncio,sys
from channels_redis.core import RedisChannelLayer
async def main():
    layer = RedisChannelLayer(hosts=[sys.argv[1]], prefix=sys.argv[2])
    try:
        await layer.group_send("shared", {"type":sys.argv[3], "value":{"external":True}})
    finally:
        await layer.close_pools()
asyncio.run(main())
'''
        def publish(kind):
            result = subprocess.run([sys.executable, "-c", publisher, url, prefix, kind],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
        with self.socket(one) as a, self.socket(two) as b:
            json.loads(a.recv(timeout=2)); json.loads(b.recv(timeout=2))
            publish("room.message")
            for socket in (a,b):
                self.assertEqual(json.loads(socket.recv(timeout=2)), {"external": True})
            publish("ws.drain")
            for socket in (a,b):
                with self.assertRaises(ConnectionClosed) as error:
                    socket.recv(timeout=2)
                self.assertEqual(error.exception.rcvd.code, 1001)
        import redis
        with redis.Redis.from_url(url) as client:
            from deployment_support import wait
            wait(lambda: client.zcard(prefix + ":group:shared") == 0)
        one.stop(); two.stop()


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class ConsumerFailureTests(unittest.TestCase):
    def test_layer_handler_error_closes_socket_and_propagates_after_cleanup(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer
        with FunctionChannelLayer(InMemoryChannelLayer) as layer:
            incoming, sent, result = Queue(), Queue(), Queue()
            failure = ValueError("handler failed")
            class Wrong(WebsocketConsumer):
                channel_layer = layer
                groups = ("room",)
                def notice(self, event):
                    raise failure
            def send(event):
                sent.put(event)
                if event["type"] == "websocket.close":
                    incoming.put({"type": "websocket.disconnect", "code": event["code"]})
            def run():
                try:
                    Wrong()({"type": "websocket"}, incoming.get, send)
                except BaseException as error:
                    result.put(error)
            incoming.put({"type": "websocket.connect"})
            thread = Thread(target=run)
            thread.start()
            self.assertEqual(sent.get(timeout=2)["type"], "websocket.accept")
            layer.group_send("room", {"type": "notice"})
            self.assertEqual(sent.get(timeout=2), {"type": "websocket.close", "code": 1011})
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertIs(result.get(timeout=1), failure)
            async def groups():
                return layer.backend.groups
            self.assertEqual(layer._call(groups), {})

    def test_group_send_backpressure_and_exact_transport_error(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer
        entered, release, returned = Event(), Event(), Event()
        failure = OSError("group transport failed")
        with FunctionChannelLayer(InMemoryChannelLayer) as layer:
            incoming, sent, result = Queue(), Queue(), Queue()
            class Room(WebsocketConsumer):
                channel_layer = layer
                groups = ("room",)
                def notice(self, event):
                    self.send(text_data="notice")
                    returned.set()
            def send(message):
                if message["type"] == "websocket.send":
                    entered.set()
                    release.wait(3)
                    raise failure
                sent.put(message)
                if message["type"] == "websocket.close":
                    incoming.put({"type": "websocket.disconnect", "code": message["code"]})
            def run():
                try:
                    Room()({"type": "websocket"}, incoming.get, send)
                except BaseException as error:
                    result.put(error)
            incoming.put({"type": "websocket.connect"})
            thread = Thread(target=run)
            thread.start()
            try:
                sent.get(timeout=2)
                layer.group_send("room", {"type": "notice"})
                self.assertTrue(entered.wait(2))
                self.assertFalse(returned.is_set())
            finally:
                release.set()
                thread.join(3)
            self.assertFalse(thread.is_alive())
            self.assertIs(result.get(timeout=1), failure)

    def test_backend_receive_failure_preserved_when_cleanup_also_fails(self):
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer, WebsocketConsumer
        failure = ConnectionError("backend unavailable")
        class Broken(InMemoryChannelLayer):
            async def receive(self, channel):
                await super().receive(channel)
                raise failure
            async def group_discard(self, group, channel):
                raise RuntimeError("cleanup unavailable")
        with FunctionChannelLayer(Broken) as layer:
            incoming, sent, result = Queue(), Queue(), Queue()
            class Room(WebsocketConsumer):
                channel_layer = layer
                groups = ("room",)
            def send(message):
                sent.put(message)
                if message["type"] == "websocket.close":
                    incoming.put({"type": "websocket.disconnect", "code": message["code"]})
            def run():
                try:
                    Room()({"type": "websocket"}, incoming.get, send)
                except BaseException as error:
                    result.put(error)
            incoming.put({"type": "websocket.connect"})
            thread = Thread(target=run)
            thread.start()
            self.assertEqual(sent.get(timeout=2)["type"], "websocket.accept")
            layer.group_send("room", {"type": "notice"})
            self.assertEqual(sent.get(timeout=2), {"type": "websocket.close", "code": 1011})
            thread.join(2)
            self.assertFalse(thread.is_alive())
            self.assertIs(result.get(timeout=1), failure)

    def test_disconnect_exception_is_not_suppressed(self):
        from asgi_function_profile.channels import WebsocketConsumer
        failure = ValueError("cleanup failed")
        class Wrong(WebsocketConsumer):
            def disconnect(self, code):
                raise failure
        with self.assertRaises(ValueError) as raised:
            Wrong()({"type": "websocket"}, lambda: {"type": "websocket.disconnect", "code": 1000}, lambda message: None)
        self.assertIs(raised.exception, failure)


@unittest.skipUnless(SERVER and all(importlib.util.find_spec(name) for name in ("django", "sentry_sdk")),
                     "requires Django, Channels, Sentry and native worker extras")
class ChannelsDjangoTests(unittest.TestCase):
    def test_authenticated_orm_context_rollback_sentry_and_revocation(self):
        from deployment_support import NativeServer, wait
        from test_django_auth_socket import Browser, ORIGIN
        from websockets.sync.client import connect
        from websockets.exceptions import ConnectionClosed, InvalidStatus
        server = NativeServer(target="channels_django_server:application", env={"ASGI_FUNCTION_REDIS_URL": ""})
        self.addCleanup(server.close)
        alice, bob = Browser(server.port), Browser(server.port)
        alice.login("alice"); bob.login("bob")
        def socket(browser=None):
            headers = [("Cookie", browser.socket_cookie())] if browser else []
            return connect(f"ws://127.0.0.1:{server.port}/room", origin=ORIGIN,
                           additional_headers=headers, proxy=None, open_timeout=5)
        with self.assertRaises(InvalidStatus):
            socket()
        with socket(alice) as a, socket(bob) as b:
            a.recv(timeout=2); b.recv(timeout=2)
            for sock, name in ((a, "alice"), (b, "bob")):
                sock.send(json.dumps({"label": name, "text": name + " note"}))
                self.assertEqual(json.loads(sock.recv(timeout=2)), {"username": name, "label": name})
            for sock, name in ((a, "alice"), (b, "bob")):
                sock.send(json.dumps({"label": name + "-failure", "text": "rollback", "fail": True}))
                with self.assertRaises(ConnectionClosed) as raised:
                    sock.recv(timeout=2)
                self.assertEqual(raised.exception.rcvd.code, 1011)
        self.assertEqual(alice.me()["notes"], ["alice note"])
        self.assertEqual(bob.me()["notes"], ["bob note"])
        path = server.path / "sentry-events"
        def errors():
            if not path.exists():
                return []
            events = [json.loads(line) for line in path.read_text().splitlines()]
            return [event for event in events if event.get("exception")]
        wait(lambda: len(errors()) == 2)
        events = errors()
        self.assertEqual({(event["user"]["username"], event["tags"]["consumer-label"]) for event in events},
                         {("alice", "alice-failure"), ("bob", "bob-failure")})
        with socket(alice) as a:
            a.recv(timeout=2)
            with alice.post("/logout/") as response:
                response.read()
            a.send(json.dumps({"label": "revoked", "text": "must not commit"}))
            with self.assertRaises(ConnectionClosed) as raised:
                a.recv(timeout=2)
            self.assertEqual(raised.exception.rcvd.code, 1008)
        with self.assertRaises(InvalidStatus):
            socket(alice)
        server.stop()
