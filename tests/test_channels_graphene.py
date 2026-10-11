"""Public Channels GraphQL APIs, wire compatibility and native composition."""

import importlib.util
import json
import os
import subprocess
import sys
from contextlib import contextmanager
from types import SimpleNamespace
import unittest
from unittest.mock import patch


AVAILABLE = all(importlib.util.find_spec(name) is not None for name in
                ("channels_graphql_ws", "graphene_django", "channels"))


@unittest.skipUnless(AVAILABLE, "requires channels-graphene extra")
class SubscriptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import django
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
        django.setup()

    def source(self):
        import graphene
        from channels.layers import InMemoryChannelLayer
        from asgi_function_profile.channels import FunctionChannelLayer
        from asgi_function_profile.channels_graphene import Subscription, bind_subscriptions
        layer = FunctionChannelLayer(InMemoryChannelLayer)
        self.addCleanup(layer.close)
        calls = []
        class Message(Subscription):
            value = graphene.String()
            notification_queue_limit = 2
            class Arguments:
                topic = graphene.String()
            def subscribe(root, info, topic):
                return [topic]
            def check_access(root, info, topic):
                return info.context.allowed
            def publish(payload, info, topic):
                return Message(value=payload)
            def unsubscribed(root, info, topic):
                calls.append(topic)
        class Query(graphene.ObjectType):
            value = graphene.String()
        class Root(graphene.ObjectType):
            message = Message.Field(name="customMessage")
        schema = bind_subscriptions(graphene.Schema(query=Query, subscription=Root), channel_layer=layer)
        info = SimpleNamespace(context=SimpleNamespace(allowed=True))
        source = schema.graphql_schema.subscription_type.fields["customMessage"].subscribe(None, info, topic="one")
        self.addCleanup(source.close)
        return layer, Message, schema, source, info, calls

    def test_existing_field_publishers_and_unsubscribe_are_ordinary(self):
        from inspect import isawaitable
        layer, Message, schema, source, info, calls = self.source()
        self.assertEqual(source.events.queue.maxsize, 2)
        self.assertFalse(isawaitable(Message.broadcast(group="one", payload="hello")))
        payload = next(source)
        result = schema.execute('subscription { renamed: customMessage(topic:"one") { value } }', root_value=payload)
        self.assertEqual(result.data, {"renamed": {"value": "hello"}})
        Message.unsubscribe_sync(group="one")
        with self.assertRaises(StopIteration):
            next(source)
        source.close()
        self.assertEqual(calls, ["one"])

    def test_group_identity_and_legacy_serialized_envelope(self):
        import graphene
        from datetime import datetime
        from channels_graphql_ws import Subscription as Original
        from channels_graphql_ws.serializer import Serializer
        layer, Message, schema, source, info, calls = self.source()
        class Legacy(Original):
            value = graphene.String()
            def publish(payload, info):
                return Legacy(value=payload)
        Legacy.__module__, Legacy.__qualname__ = Message.__module__, Message.__qualname__
        self.assertEqual(Message._group_name("one"), Legacy._group_name("one"))
        self.assertEqual(Message._group_name(), Legacy._group_name())
        value = {"date": datetime(2026, 1, 2), "binary": b"hello"}
        group = Legacy._group_name("one")
        layer.group_send(group, {"type": "broadcast", "group": group, "payload": Serializer.serialize(value)})
        self.assertEqual(next(source), value)

    def test_access_revocation_ends_source_and_cleans_groups(self):
        from graphql import GraphQLError
        layer, Message, schema, source, info, calls = self.source()
        info.context.allowed = False
        Message.broadcast_sync(group="one", payload="denied")
        with self.assertRaisesRegex(GraphQLError, "access revoked"):
            next(source)
        self.assertTrue(source.closed)
        self.assertEqual(calls, ["one"])
        async def groups():
            return layer.backend.groups
        self.assertEqual(layer._call(groups), {})

    def test_async_publish_hook_is_rejected_by_graphene(self):
        import graphene
        from asgi_function_profile.channels_graphene import Subscription
        class Async(Subscription):
            value = graphene.String()
            async def publish(payload, info):
                return Async(value="async")
        class Root(graphene.ObjectType):
            message = Async.Field()
        class Query(graphene.ObjectType):
            value = graphene.String()
        result = graphene.Schema(query=Query, subscription=Root).execute('subscription { message { value } }')
        self.assertIn("ordinary functions", result.errors[0].message)

    def test_async_subscribe_and_unsubscribe_hooks_are_rejected(self):
        layer, Message, schema, source, info, calls = self.source()
        async def hook(*args, **kwargs):
            pass
        with patch.dict(Message._meta.__dict__, subscribe=hook):
            with self.assertRaisesRegex(TypeError, "ordinary functions"):
                schema.graphql_schema.subscription_type.fields["customMessage"].subscribe(None, info, topic="two")
        with patch.dict(Message._meta.__dict__, unsubscribed=hook):
            Message.unsubscribe(group="one")
            with self.assertRaisesRegex(TypeError, "ordinary functions"):
                next(source)

    def test_setup_cleanup_failure_closes_created_source(self):
        layer, Message, schema, source, info, calls = self.source()
        from asgi_function_profile.channels import ChannelEvents
        created = []
        def events(*args, **kwargs):
            stream = ChannelEvents(*args, **kwargs)
            created.append(stream)
            return stream
        failure = RuntimeError("database cleanup failed")
        with patch("asgi_function_profile.channels_graphene.ChannelEvents", side_effect=events), \
                patch("asgi_function_profile.channels_graphene._database_cleanup", side_effect=failure):
            with self.assertRaises(RuntimeError) as raised:
                schema.graphql_schema.subscription_type.fields["customMessage"].subscribe(None, info, topic="two")
        self.assertIs(raised.exception, failure)
        self.assertTrue(created[0].closed)


SERVER = AVAILABLE and all(importlib.util.find_spec(name) is not None for name in
                          ("uvicorn_worker", "websockets", "strawberry", "sentry_sdk"))


@unittest.skipUnless(SERVER, "requires all server/schema extras and test group")
class ChannelsGrapheneServerTests(unittest.TestCase):
    def start(self, **env):
        from deployment_support import NativeServer
        server = NativeServer(target="channels_graphene_server:application",
                              env={"ASGI_FUNCTION_REDIS_URL": "", **env})
        self.addCleanup(server.close)
        return server

    @contextmanager
    def socket(self, server, *, protocol="graphql-transport-ws", user="alice"):
        from test_django_auth_socket import Browser, ORIGIN
        from websockets.sync.client import connect
        browser = Browser(server.port)
        browser.login(user)
        with connect(f"ws://127.0.0.1:{server.port}/channels-graphene", origin=ORIGIN,
                         additional_headers=[("Cookie", browser.socket_cookie())],
                         subprotocols=[protocol], proxy=None, open_timeout=10) as socket:
            socket.send(json.dumps({"type": "connection_init"}))
            self.assertEqual(json.loads(socket.recv(timeout=5)), {"type": "connection_ack"})
            yield socket

    def operation(self, socket, identifier, query, *, protocol="graphql-transport-ws"):
        socket.send(json.dumps({"type": "start" if protocol == "graphql-ws" else "subscribe",
                                "id": identifier, "payload": {"query": query}}))

    def message(self, socket, identifier, kind="next"):
        message = json.loads(socket.recv(timeout=5))
        self.assertEqual((message.get("id"), message["type"]), (identifier, kind))
        return message.get("payload")

    def query(self, socket, query, *, protocol="graphql-transport-ws"):
        self.operation(socket, "query", query, protocol=protocol)
        result = self.message(socket, "query", "data" if protocol == "graphql-ws" else "next")
        self.message(socket, "query", "complete")
        return result["data"]

    def test_authenticated_sources_both_protocols_and_group_drain(self):
        from deployment_support import wait
        from websockets.exceptions import ConnectionClosed
        for protocol in ("graphql-transport-ws", "graphql-ws"):
            with self.subTest(protocol=protocol):
                server = self.start()
                with self.socket(server, protocol=protocol) as one, self.socket(server, protocol=protocol, user="bob") as two:
                    self.operation(one, "live", 'subscription { notice(topic:"one") { value } }', protocol=protocol)
                    self.operation(two, "live", 'subscription { notice(topic:"one") { value } }', protocol=protocol)
                    wait(lambda: self.query(one, '{ active(topic:"one") }', protocol=protocol)["active"] == 2)
                    self.query(one, 'mutation { publish(topic:"one", value:7) }', protocol=protocol)
                    for socket in (one, two):
                        self.assertEqual(self.message(socket, "live", "data" if protocol == "graphql-ws" else "next"),
                                         {"data": {"notice": {"value": 7}}})
                    self.query(one, 'mutation { unsubscribe(topic:"one") }', protocol=protocol)
                    for socket in (one, two):
                        self.message(socket, "live", "complete")
                    wait(lambda: self.query(one, '{ active(topic:"one") }', protocol=protocol)["active"] == 0)
                    self.operation(one, "query", "mutation { drain }", protocol=protocol)
                    for socket in (one, two):
                        with self.assertRaises(ConnectionClosed) as closed:
                            while True:
                                socket.recv(timeout=5)
                        self.assertEqual(closed.exception.rcvd.code, 1001)
                server.stop()

    def test_sentry_publish_error_and_shutdown_of_idle_source(self):
        from deployment_support import wait
        server = self.start()
        with self.socket(server, user="bob") as socket:
            self.operation(socket, "live", 'subscription { notice(topic:"one") { value } }')
            wait(lambda: self.query(socket, '{ active(topic:"one") }')["active"] == 1)
            self.query(socket, 'mutation { publish(topic:"one", value:-1) }')
            self.assertEqual(self.message(socket, "live")["errors"][0]["message"], "subscription publish failed")
            server.stop()
        events = [json.loads(line) for line in (server.path / "sentry-events").read_text().splitlines()]
        failures = [event for event in events if event.get("exception", {}).get("values", [{}])[-1].get("value") == "subscription publish failed"]
        self.assertEqual(len(failures), 1)
        self.assertEqual(failures[0]["user"]["username"], "bob")
        self.assertTrue((server.path / "channels-source-closed").exists())

    @unittest.skipUnless(os.environ.get("ASGI_FUNCTION_REDIS_URL"), "requires disposable Redis")
    def test_legacy_publisher_reaches_native_redis_source(self):
        from deployment_support import wait
        from uuid import uuid4
        url = os.environ["ASGI_FUNCTION_REDIS_URL"]
        server = self.start(ASGI_FUNCTION_REDIS_URL=url, ASGI_FUNCTION_CHANNEL_PREFIX="function-graphql:" + uuid4().hex)
        with self.socket(server) as socket:
            self.operation(socket, "live", 'subscription { notice(topic:"one") { value } }')
            wait(lambda: self.query(socket, '{ active(topic:"one") }')["active"] == 1)
            group = self.query(socket, '{ groupName(topic:"one") }')["groupName"]
            publisher = '''
import asyncio, sys
from channels_redis.core import RedisChannelLayer
from channels_graphql_ws.serializer import Serializer
async def main():
    layer = RedisChannelLayer(hosts=[sys.argv[1]], prefix=sys.argv[2])
    try:
        await layer.group_send(sys.argv[3], {"type":"broadcast", "group":sys.argv[3], "payload":Serializer.serialize(9)})
    finally:
        await layer.close_pools()
asyncio.run(main())
'''
            result = subprocess.run([sys.executable, "-c", publisher, url,
                                     self.query(socket, "{ prefix }")["prefix"], group],
                                    capture_output=True, text=True, timeout=5)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(self.message(socket, "live"), {"data": {"notice": {"value": 9}}})
            server.stop()
