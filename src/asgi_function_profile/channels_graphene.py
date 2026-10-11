"""Optional Channels GraphQL subscription classes with ordinary sources.

Reuse the public channels_graphql_ws Field, group identity and serializer.
Async hooks still require translation. Configure the function channel layer
after fork, before publishing or constructing sources.
"""

from functools import partial

from channels_graphql_ws import Subscription as ChannelsSubscription
from channels_graphql_ws.serializer import Serializer
from graphql import GraphQLError

from .channels import ChannelEvents, _cleanup, _database_cleanup, _ordinary


class _Source:
    def __init__(self, subscription, root, info, arguments):
        self.subscription, self.info, self.arguments = subscription, info, arguments
        self.closed = False
        self.events = None
        try:
            try:
                groups = _ordinary(subscription._meta.subscribe, root, info, **arguments) if subscription._meta.subscribe else ()
                groups = [subscription._group_name()] + [subscription._group_name(str(group)) for group in groups or ()]
                limit = subscription.notification_queue_limit
                self.events = ChannelEvents(groups, channel_layer=subscription._function_layer(),
                                            limit=1024 if limit is None else limit)
                info.context.channel_name = self.events.channel
            finally:
                _cleanup(_database_cleanup)
        except BaseException:
            if self.events is not None:
                _cleanup(self.events.close)
            raise

    def __iter__(self):
        return self

    def __next__(self):
        try:
            while True:
                event = next(self.events)
                if event["type"] == "unsubscribe":
                    break
                if event["type"] != "broadcast":
                    continue
                try:
                    check = getattr(self.subscription, "check_access", None)
                    if check is not None and not _ordinary(check, None, self.info, **self.arguments):
                        raise GraphQLError("Subscription access revoked")
                    return Serializer.deserialize(event["payload"])
                finally:
                    _cleanup(_database_cleanup)
        except StopIteration:
            pass
        except BaseException:
            _cleanup(self.close)
            raise
        self.close()
        raise StopIteration

    def cancel(self):
        self.events.cancel()

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.events.close()
        finally:
            try:
                if self.subscription._meta.unsubscribed:
                    _cleanup(lambda: _ordinary(self.subscription._meta.unsubscribed, None, self.info, **self.arguments))
            finally:
                _cleanup(_database_cleanup)


class Subscription(ChannelsSubscription):
    """Keep ordinary subscribe/publish/unsubscribed hooks and publisher calls.

    Set channel_layer to a FunctionChannelLayer, or pass it to bind_subscriptions.
    Local notification overflow terminates the source instead of dropping older
    notifications. This intentionally differs from the upstream queue policy.
    """

    channel_layer = None

    class Meta:
        abstract = True

    @classmethod
    def _function_layer(cls):
        if cls.channel_layer is None:
            raise RuntimeError("configure a function channel layer before using subscriptions")
        return cls.channel_layer

    @classmethod
    def broadcast(cls, *, group=None, payload=None):
        name = cls._group_name(group)
        cls._function_layer().group_send(name, {
            "type": "broadcast", "group": name, "payload": Serializer.serialize(payload),
        })

    broadcast_sync = broadcast

    @classmethod
    def unsubscribe(cls, *, group=None):
        name = cls._group_name(group)
        cls._function_layer().group_send(name, {"type": "unsubscribe", "group": name})

    unsubscribe_sync = unsubscribe

    @classmethod
    def Field(cls, *args, **kwargs):
        field = super().Field(*args, **kwargs)
        field.resolver = partial(_ordinary, field.resolver)
        field.function_subscription = cls
        return field


def bind_subscriptions(schema, *, channel_layer=None):
    """Bind source factories to Fields, preserving Graphene's compiled names.

    An optional layer configures publishers and sources for each subscription
    class. A class has one layer binding per process; do not bind it to different
    layers in different schemas. Otherwise configure class.channel_layer yourself.
    """
    if schema.subscription is None:
        return schema
    compiled = schema.graphql_schema.subscription_type.fields.values()
    for field in schema.subscription._meta.fields.values():
        subscription = getattr(field, "function_subscription", None)
        if subscription is None:
            continue
        if channel_layer is not None:
            subscription.channel_layer = channel_layer
        for target in compiled:
            if target.resolve is field.resolver:
                def source(root, info, _subscription=subscription, **arguments):
                    return _Source(_subscription, root, info, arguments)
                target.subscribe = source
    return schema
