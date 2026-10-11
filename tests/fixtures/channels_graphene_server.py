"""Existing GraphQL transport composed with native consumers and channel sources."""

import json
import os
import threading

import graphene
import graphql_server
from django.db import connections
from django.test import override_settings
from asgi_function_profile.channels import Consumer
from asgi_function_profile.channels_graphene import Subscription, bind_subscriptions
from asgi_function_profile.django_channels import DjangoChannelLayerProvider
from asgi_function_profile.graphene import GrapheneDjangoWebSocket

layer, transport = None, None
layers = DjangoChannelLayerProvider()


class Notice(Subscription):
    value = graphene.Int()
    class Arguments:
        topic = graphene.String(required=True)
    def subscribe(root, info, topic):
        graphql_server.whoami(info.context)
        return [topic]
    def check_access(root, info, topic):
        return info.context.user.__class__.objects.filter(pk=info.context.user.pk, is_active=True).exists()
    def publish(payload, info, topic):
        graphql_server.whoami(info.context)
        if payload == -1:
            raise ValueError("subscription publish failed")
        return Notice(value=payload)
    def unsubscribed(root, info, topic):
        assert threading.get_ident() == info.context.operation_thread
        with (graphql_server.STATE / "channels-source-closed").open("a") as marker:
            marker.write("x")


class Query(graphql_server.GQuery):
    active = graphene.Int(topic=graphene.String(required=True))
    group_name = graphene.String(topic=graphene.String(required=True))
    prefix = graphene.String()
    def resolve_active(root, info, topic):
        async def count():
            if hasattr(layer.backend, "groups"):
                return len(layer.backend.groups.get(Notice._group_name(topic), {}))
            return await layer.backend.connection(0).zcard(layer.backend._group_key(Notice._group_name(topic)))
        return layer._call(count)
    def resolve_group_name(root, info, topic):
        return Notice._group_name(topic)
    def resolve_prefix(root, info):
        return layer.backend.prefix


class Mutation(graphene.ObjectType):
    publish = graphene.Boolean(topic=graphene.String(required=True), value=graphene.Int(required=True))
    unsubscribe = graphene.Boolean(topic=graphene.String(required=True))
    drain = graphene.Boolean()
    def resolve_publish(root, info, topic, value):
        Notice.broadcast(group=topic, payload=value)
        return True
    def resolve_unsubscribe(root, info, topic):
        Notice.unsubscribe(group=topic)
        return True
    def resolve_drain(root, info):
        layer.group_send("transport-drain", {"type": "transport.drain"})
        return True


class Root(graphene.ObjectType):
    notice = Notice.Field()


class Wrapper(Consumer):
    def run(self, receive):
        self.join_group("transport-drain")
        return transport(self.scope, receive, self.send)
    def transport_drain(self, event):
        self.send({"type": "websocket.close", "code": 1001})


def application(scope, receive, send):
    global layer, transport
    if scope["type"] == "websocket" and scope["path"] == "/channels-graphene":
        failed_close = bool(os.environ.get("ASGI_FUNCTION_FAIL_CLOSE"))
        def socket_send(event):
            if failed_close and event["type"] == "websocket.close":
                with (graphql_server.STATE / "channels-close-failed").open("a") as marker:
                    marker.write("x")
                raise OSError("injected websocket close failure")
            return send(event)
        try:
            return Wrapper()(scope, receive, socket_send)
        finally:
            assert all(connection.connection is None for connection in connections.all())
            if failed_close:
                async def remaining_groups():
                    return layer.backend.groups
                (graphql_server.STATE / "channels-consumer-finished").write_text(json.dumps(layer._call(remaining_groups)))
    def lifecycle_event():
        global layer, transport
        event = receive()
        if event["type"] == "lifespan.startup":
            layer = layers.get()
            Wrapper.channel_layer = layer
            schema = bind_subscriptions(graphene.Schema(query=Query, mutation=Mutation, subscription=Root),
                                        channel_layer_provider=layers)
            transport = GrapheneDjangoWebSocket(schema, **{**graphql_server.options, "connection_init_timeout": 5})
        elif event["type"] == "lifespan.shutdown":
            layers.close()
        return event
    if scope["type"] == "lifespan":
        config = {"BACKEND": "channels.layers.InMemoryChannelLayer"}
        if os.environ.get("ASGI_FUNCTION_REDIS_URL"):
            config = {"BACKEND": "channels_redis.core.RedisChannelLayer", "CONFIG": {
                "hosts": [os.environ["ASGI_FUNCTION_REDIS_URL"]],
                "prefix": os.environ["ASGI_FUNCTION_CHANNEL_PREFIX"],
            }}
        with override_settings(CHANNEL_LAYERS={"default": config}):
            return graphql_server.application(scope, lifecycle_event, send)
    return graphql_server.application(scope, receive, send)
