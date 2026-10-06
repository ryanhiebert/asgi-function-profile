from __future__ import annotations

import asyncio
import threading
import unittest

from typing import Any

from asgi_function_profile import BridgeClosedError
from asgi_function_profile import FunctionProfileAdapter


Scope = dict[str, Any]
Message = dict[str, Any]


async def wait_until(event: threading.Event, timeout: float = 1.0) -> None:
    async def poll() -> None:
        while not event.is_set():
            await asyncio.sleep(0.001)

    await asyncio.wait_for(poll(), timeout)


class AdapterTests(unittest.IsolatedAsyncioTestCase):
    async def test_receive_waits_for_the_underlying_awaitable(self) -> None:
        incoming: asyncio.Queue[Message] = asyncio.Queue()
        application_started = threading.Event()
        received: list[Message] = []

        def application(scope, receive, send) -> None:
            application_started.set()
            received.append(receive())

        async def receive() -> Message:
            return await incoming.get()

        async def send(message: Message) -> None:
            self.fail("send was not expected")

        task = asyncio.create_task(
            FunctionProfileAdapter(application)({"type": "test"}, receive, send)
        )
        await wait_until(application_started)
        await asyncio.sleep(0)
        self.assertFalse(task.done())

        event = {"type": "test.event"}
        await incoming.put(event)
        await asyncio.wait_for(task, 1)
        self.assertEqual(received, [event])

    async def test_send_preserves_backpressure(self) -> None:
        send_started = asyncio.Event()
        release_send = asyncio.Event()
        send_returned = threading.Event()

        def application(scope, receive, send) -> None:
            send({"type": "test.event"})
            send_returned.set()

        async def receive() -> Message:
            self.fail("receive was not expected")

        async def send(message: Message) -> None:
            send_started.set()
            await release_send.wait()

        task = asyncio.create_task(
            FunctionProfileAdapter(application)({"type": "test"}, receive, send)
        )
        await asyncio.wait_for(send_started.wait(), 1)
        self.assertFalse(send_returned.is_set())

        release_send.set()
        await asyncio.wait_for(task, 1)
        self.assertTrue(send_returned.is_set())

    async def test_send_exception_is_raised_by_the_ordinary_call(self) -> None:
        class SendFailed(Exception):
            pass

        def application(scope, receive, send) -> None:
            send({"type": "test.event"})

        async def receive() -> Message:
            self.fail("receive was not expected")

        async def send(message: Message) -> None:
            raise SendFailed("closed")

        with self.assertRaisesRegex(SendFailed, "closed"):
            await FunctionProfileAdapter(application)(
                {"type": "test"}, receive, send
            )

    async def test_streamed_http_request_and_response(self) -> None:
        incoming: asyncio.Queue[Message] = asyncio.Queue()
        outgoing: list[Message] = []

        await incoming.put(
            {"type": "http.request", "body": b"hel", "more_body": True}
        )
        await incoming.put(
            {"type": "http.request", "body": b"lo", "more_body": False}
        )

        def application(scope, receive, send) -> None:
            body = bytearray()
            while True:
                event = receive()
                body.extend(event["body"])
                if not event["more_body"]:
                    break
            send({"type": "http.response.start", "status": 200, "headers": []})
            send(
                {
                    "type": "http.response.body",
                    "body": bytes(body[:3]),
                    "more_body": True,
                }
            )
            send(
                {
                    "type": "http.response.body",
                    "body": bytes(body[3:]),
                    "more_body": False,
                }
            )

        async def receive() -> Message:
            return await incoming.get()

        async def send(message: Message) -> None:
            outgoing.append(message)

        await FunctionProfileAdapter(application)(
            {"type": "http"}, receive, send
        )

        self.assertEqual(
            outgoing,
            [
                {"type": "http.response.start", "status": 200, "headers": []},
                {
                    "type": "http.response.body",
                    "body": b"hel",
                    "more_body": True,
                },
                {
                    "type": "http.response.body",
                    "body": b"lo",
                    "more_body": False,
                },
            ],
        )

    async def test_bidirectional_websocket(self) -> None:
        incoming: asyncio.Queue[Message] = asyncio.Queue()
        outgoing: list[Message] = []

        for event in (
            {"type": "websocket.connect"},
            {"type": "websocket.receive", "text": "hello"},
            {"type": "websocket.disconnect", "code": 1000},
        ):
            await incoming.put(event)

        def application(scope, receive, send) -> None:
            assert receive()["type"] == "websocket.connect"
            send({"type": "websocket.accept"})
            event = receive()
            send({"type": "websocket.send", "text": event["text"]})
            assert receive()["type"] == "websocket.disconnect"

        async def receive() -> Message:
            return await incoming.get()

        async def send(message: Message) -> None:
            outgoing.append(message)

        await FunctionProfileAdapter(application)(
            {"type": "websocket"}, receive, send
        )

        self.assertEqual(
            outgoing,
            [
                {"type": "websocket.accept"},
                {"type": "websocket.send", "text": "hello"},
            ],
        )

    async def test_lifespan(self) -> None:
        incoming: asyncio.Queue[Message] = asyncio.Queue()
        outgoing: list[Message] = []
        scope: Scope = {"type": "lifespan", "state": {}}

        await incoming.put({"type": "lifespan.startup"})
        await incoming.put({"type": "lifespan.shutdown"})

        def application(scope, receive, send) -> None:
            while True:
                event = receive()
                if event["type"] == "lifespan.startup":
                    scope["state"]["service"] = "ready"
                    send({"type": "lifespan.startup.complete"})
                elif event["type"] == "lifespan.shutdown":
                    send({"type": "lifespan.shutdown.complete"})
                    return

        async def receive() -> Message:
            return await incoming.get()

        async def send(message: Message) -> None:
            outgoing.append(message)

        await FunctionProfileAdapter(application)(scope, receive, send)

        self.assertEqual(scope["state"], {"service": "ready"})
        self.assertEqual(
            outgoing,
            [
                {"type": "lifespan.startup.complete"},
                {"type": "lifespan.shutdown.complete"},
            ],
        )

    async def test_scope_invocations_can_overlap(self) -> None:
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        thread_ids: set[int] = set()
        outgoing: list[int] = []

        def application(scope, receive, send) -> None:
            with lock:
                thread_ids.add(threading.get_ident())
            barrier.wait(timeout=1)
            send({"type": "test.event", "id": scope["id"]})

        async def receive() -> Message:
            self.fail("receive was not expected")

        async def send(message: Message) -> None:
            outgoing.append(message["id"])

        adapter = FunctionProfileAdapter(application)
        await asyncio.gather(
            adapter({"type": "test", "id": 1}, receive, send),
            adapter({"type": "test", "id": 2}, receive, send),
        )

        self.assertEqual(len(thread_ids), 2)
        self.assertCountEqual(outgoing, [1, 2])

    async def test_cancellation_releases_a_blocked_receive(self) -> None:
        application_started = threading.Event()
        application_released = threading.Event()
        never = asyncio.Event()

        def application(scope, receive, send) -> None:
            application_started.set()
            try:
                receive()
            except BridgeClosedError:
                application_released.set()

        async def receive() -> Message:
            await never.wait()
            raise AssertionError("unreachable")

        async def send(message: Message) -> None:
            self.fail("send was not expected")

        task = asyncio.create_task(
            FunctionProfileAdapter(application)({"type": "test"}, receive, send)
        )
        await wait_until(application_started)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        await wait_until(application_released)


if __name__ == "__main__":
    unittest.main()
