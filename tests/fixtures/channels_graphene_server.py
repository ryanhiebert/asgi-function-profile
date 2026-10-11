"""Existing GraphQL transport composed with native consumers and channel sources."""

import os
import threading

import graphene
import graphql_server
from channels.layers import InMemoryChannelLayer
from django.db import connections
from asgi_function_profile.channels import Consumer, FunctionChannelLayer
from asgi_function_profile.channels_graphene import Subscription, bind_subscriptions
from asgi_function_profile.graphene import GrapheneDjangoWebSocket

layer, transport = None, None


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
        try:
            return Wrapper()(scope, receive, send)
        finally:
            assert all(connection.connection is None for connection in connections.all())
    def lifecycle_event():
        global layer, transport
        event = receive()
        if event["type"] == "lifespan.startup":
            def factory():
                if os.environ.get("ASGI_FUNCTION_REDIS_URL"):
                    from channels_redis.core import RedisChannelLayer
                    return RedisChannelLayer(hosts=[os.environ["ASGI_FUNCTION_REDIS_URL"]],
                                             prefix=os.environ["ASGI_FUNCTION_CHANNEL_PREFIX"])
                return InMemoryChannelLayer()
            layer = FunctionChannelLayer(factory)
            Wrapper.channel_layer = layer
            schema = bind_subscriptions(graphene.Schema(query=Query, mutation=Mutation, subscription=Root), channel_layer=layer)
            transport = GrapheneDjangoWebSocket(schema, **{**graphql_server.options, "connection_init_timeout": 5})
        elif event["type"] == "lifespan.shutdown":
            layer.close()
        return event
    return graphql_server.application(scope, lifecycle_event if scope["type"] == "lifespan" else receive, send)
