"""Authenticated ORM and Sentry checks for ordinary channel consumers."""
import contextvars
import json
import threading

import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration
from sentry_sdk.transport import Transport

import channels_server


class LocalTransport(Transport):
    def capture_envelope(self, envelope):
        for item in envelope.items:
            if item.type == "event":
                with transport_lock, (channels_server.STATE / "sentry-events").open("a") as output:
                    output.write(json.dumps(item.payload.json) + "\n")


transport_lock = threading.Lock()
sentry_sdk.init(dsn="http://public@localhost/1", transport=LocalTransport(),
                integrations=[DjangoIntegration()], send_default_pii=True,
                enable_backpressure_handling=False)

import django_server
from django.conf import settings
from django.contrib.auth import get_user_model
from django.db import connection, connections, transaction
from examples.django_demo.models import PrivateNote
from examples.django_demo.sockets import session_user

label = contextvars.ContextVar("consumer_label")
local = threading.local()


class Room(channels_server.Room):
    def connect(self):
        origins = [value.decode("latin1") for name, value in self.scope["headers"] if name == b"origin"]
        user = session_user(self.scope)
        if len(origins) != 1 or origins[0] not in settings.WEBSOCKET_ALLOWED_ORIGINS or user is None:
            self.close(code=1008)
            return
        self.owner = user.pk
        # Exercise a deferred field later, in a channel-layer handler.
        self.user = get_user_model().objects.only("id").get(pk=self.owner)
        self.wrapper = connections["default"]
        local.owner = self.owner
        label.set("connected")
        sentry_sdk.set_user({"id": str(self.owner), "username": user.username})
        super().connect()

    def check(self):
        assert local.owner == self.owner
        assert connections["default"] is self.wrapper
        assert not connection.in_atomic_block
        current = session_user(self.scope)
        if current is None or current.pk != self.owner:
            self.close(code=1008)
            return False
        return True

    def receive_json(self, content):
        if not self.check():
            return
        label.set(content["label"])
        sentry_sdk.set_tag("consumer-label", label.get())
        self.channel_layer.group_send("room", {"type": "room.message", "owner": self.owner, **content})

    def room_message(self, event):
        if event["owner"] != self.owner or not self.check():
            return
        assert label.get() == event["label"]
        assert self.user.username  # Deferred ORM query in the channel handler.
        with transaction.atomic():
            PrivateNote.objects.create(owner_id=self.owner, text=event["text"])
            if event.get("fail"):
                raise ValueError("channel transaction failed")
        self.send_json({"username": self.user.username, "label": label.get()})

    def dispatch(self, message):
        try:
            super().dispatch(message)
        finally:
            assert all(wrapper.connection is None for wrapper in connections.all())
            assert not connection.in_atomic_block


def application(scope, receive, send):
    if scope["type"] == "websocket":
        thread = threading.get_ident()
        def query(execute, sql, params, many, context):
            assert threading.get_ident() == thread
            return execute(sql, params, many, context)
        with connection.execute_wrapper(query):
            return Room.as_asgi()(scope, receive, send)
    if scope["type"] == "lifespan":
        try:
            def observed(message):
                if message["type"] == "lifespan.startup.complete":
                    Room.channel_layer = channels_server.layer
                send(message)
            return channels_server.application(scope, receive, observed)
        finally:
            client = sentry_sdk.get_client()
            client.close()
            if client.session_flusher._thread is not None:
                client.session_flusher._thread.join(timeout=1)
    return django_server.application(scope, receive, send)
