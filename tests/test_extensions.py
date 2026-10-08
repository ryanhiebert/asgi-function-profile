import asyncio
import importlib.util
from threading import Event
import unittest

from asgi_function_profile import FunctionProfileAdapter
from examples.denial import application
import test_server


class DenialAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_capability_selection_and_unchanged_extension_events(self):
        for advertised in (False, True):
            sent = []
            scope = {"type": "websocket", "extensions": {"websocket.http.response": {}} if advertised else {}}

            async def receive():
                return {"type": "websocket.connect"}

            async def send(message):
                sent.append(message)

            await FunctionProfileAdapter(application)(scope, receive, send)
            if advertised:
                self.assertEqual(sent[0]["status"], 429)
                self.assertEqual([row["type"] for row in sent], ["websocket.http.response.start",
                                "websocket.http.response.body", "websocket.http.response.body"])
                self.assertEqual(b"".join(row.get("body", b"") for row in sent), b"try again later")
            else:
                self.assertEqual(sent, [{"type": "websocket.close", "code": 1008}])

    async def test_extension_send_completion_and_exception_propagation(self):
        entered = asyncio.Event()
        release = asyncio.Event()
        returned = Event()
        failure = OSError("denial transport failed")

        def app(scope, receive, send):
            application(scope, receive, send)
            returned.set()

        async def receive():
            return {"type": "websocket.connect"}

        async def send(message):
            if message["type"] == "websocket.http.response.body":
                entered.set()
                await release.wait()
                raise failure

        task = asyncio.create_task(FunctionProfileAdapter(app)(
            {"type": "websocket", "extensions": {"websocket.http.response": {}}}, receive, send))
        await asyncio.wait_for(entered.wait(), 1)
        self.assertFalse(task.done())
        self.assertFalse(returned.is_set())
        release.set()
        with self.assertRaises(OSError) as error:
            await asyncio.wait_for(task, 1)
        self.assertIs(error.exception, failure)
        self.assertFalse(returned.is_set())


@unittest.skipUnless(test_server.UVICORN_AVAILABLE and importlib.util.find_spec("websockets"),
                     "requires server extra and WebSocket test dependency")
class DenialServerTests(test_server.UvicornTestCase):
    application_target = "extension_server:application"

    def test_denial_status_headers_and_two_part_body_over_real_tcp(self):
        from websockets.sync.client import connect
        from websockets.exceptions import InvalidStatus

        with self.assertRaises(InvalidStatus) as error:
            connect(f"ws://127.0.0.1:{self.port}/denied", proxy=None, open_timeout=5)
        response = error.exception.response
        self.assertEqual(response.status_code, 429)
        self.assertEqual(response.headers["Retry-After"], "5")
        self.assertEqual(response.body, b"try again later")
