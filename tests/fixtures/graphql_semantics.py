"""Deterministic bridge checks in an isolated Django process."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
from threading import Event
import unittest

from django.conf import settings
settings.configure(SECRET_KEY="test", INSTALLED_APPS=["django.contrib.auth",
                   "django.contrib.contenttypes", "django.contrib.sessions"])
import django
django.setup()

import graphene
import strawberry
from asgi_function_profile.adapter import FunctionProfileAdapter
from asgi_function_profile.graphene import GrapheneDjangoWebSocket
from asgi_function_profile.strawberry import StrawberryDjangoWebSocket

progress = []
finished = Event()


def source():
    try:
        for value in range(3):
            progress.append(value)
            yield value
    finally:
        finished.set()


class Query(graphene.ObjectType):
    answer = graphene.Int()
    origin = graphene.String()

    def resolve_origin(root, info):
        return info.context.headers["origin"]


class Subscription(graphene.ObjectType):
    ticks = graphene.Int()

    def subscribe_ticks(root, info):
        return source()


@strawberry.type
class SQuery:
    answer: int = 42

    @strawberry.field
    def origin(self, info: strawberry.Info) -> str:
        return info.context.request.headers["origin"]


@strawberry.type
class SSubscription:
    @strawberry.subscription(graphql_type=int)
    def ticks(self):
        return source()


apps = [GrapheneDjangoWebSocket(graphene.Schema(query=Query, subscription=Subscription),
                               allowed_origins={"http://test"}),
        StrawberryDjangoWebSocket(strawberry.Schema(query=SQuery, subscription=SSubscription),
                                 allowed_origins={"http://test"})]
scope = {"type": "websocket", "path": "/graphql", "root_path": "", "scheme": "ws",
         "query_string": b"", "headers": [(b"origin", b"http://test")],
         "subprotocols": ["graphql-transport-ws"]}


class Checks(unittest.TestCase):
    def test_each_library_retains_its_default_django_context_shape(self):
        for application in apps:
            async def check():
                incoming = asyncio.Queue()
                for event in ({"type": "websocket.connect"},
                              {"type": "websocket.receive", "text": '{"type":"connection_init"}'},
                              {"type": "websocket.receive", "text": json.dumps({"type": "subscribe", "id": "q", "payload": {"query": "{ origin }"}})}):
                    incoming.put_nowait(event)
                messages = []
                async def send(event):
                    if event["type"] == "websocket.send":
                        message = json.loads(event["text"])
                        messages.append(message)
                        if message["type"] == "complete":
                            incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
                with ThreadPoolExecutor(max_workers=1) as executor:
                    await asyncio.wait_for(FunctionProfileAdapter(application, executor=executor)(scope, incoming.get, send), 3)
                self.assertEqual(messages[1]["payload"], {"data": {"origin": "http://test"}})
            asyncio.run(check())

    def test_send_backpressure_prevents_requesting_the_next_source_item(self):
        for application in apps:
            progress.clear()
            finished.clear()
            async def check():
                incoming = asyncio.Queue()
                for event in ({"type": "websocket.connect"},
                              {"type": "websocket.receive", "text": '{"type":"connection_init"}'},
                              {"type": "websocket.receive", "text": json.dumps({"type": "subscribe", "id": "s", "payload": {"query": "subscription { ticks }"}})}):
                    incoming.put_nowait(event)
                blocked = asyncio.Event()
                release = asyncio.Event()
                complete = asyncio.Event()
                async def send(event):
                    if event["type"] != "websocket.send":
                        return
                    message = json.loads(event["text"])
                    if message["type"] == "next" and message["payload"]["data"]["ticks"] == 0:
                        blocked.set()
                        await release.wait()
                    if message["type"] == "complete":
                        complete.set()
                with ThreadPoolExecutor(max_workers=1) as executor:
                    task = asyncio.create_task(FunctionProfileAdapter(application, executor=executor)(scope, incoming.get, send))
                    try:
                        await asyncio.wait_for(blocked.wait(), 3)
                        self.assertEqual(progress, [0])
                        release.set()
                        await asyncio.wait_for(complete.wait(), 3)
                        incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
                        await asyncio.wait_for(task, 3)
                    finally:
                        release.set()
                        incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
                self.assertEqual(progress, [0, 1, 2])
                self.assertTrue(finished.is_set())
            asyncio.run(check())

    def test_send_error_propagates_and_closes_the_source(self):
        for application in apps:
            progress.clear()
            finished.clear()
            async def check():
                failed = asyncio.Event()
                events = iter([{"type": "websocket.connect"},
                               {"type": "websocket.receive", "text": '{"type":"connection_init"}'},
                               {"type": "websocket.receive", "text": json.dumps({"type": "subscribe", "id": "s", "payload": {"query": "subscription { ticks }"}})}])
                async def receive():
                    try:
                        return next(events)
                    except StopIteration:
                        await failed.wait()
                        return {"type": "websocket.disconnect", "code": 1006}
                async def send(event):
                    if event["type"] == "websocket.send" and json.loads(event["text"])["type"] == "next":
                        failed.set()
                        raise OSError("network failed")
                with ThreadPoolExecutor(max_workers=1) as executor:
                    with self.assertRaisesRegex(OSError, "network failed"):
                        await asyncio.wait_for(FunctionProfileAdapter(application, executor=executor)(scope, receive, send), 3)
                self.assertEqual(progress, [0])
                self.assertTrue(finished.is_set())
            asyncio.run(check())


if __name__ == "__main__":
    unittest.main(verbosity=2)
