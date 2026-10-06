"""Authenticated synchronous sockets; deliberately an example, not a framework."""

import json
from contextlib import contextmanager
from io import BytesIO

from django.conf import settings
from django.contrib.auth.middleware import AuthenticationMiddleware
from django.contrib.sessions.middleware import SessionMiddleware
from django.core.handlers.asgi import ASGIRequest
from django.db import connections, transaction

from .models import PrivateNote


@contextmanager
def database_work():
    """Keep database connections out of socket receive/send waits."""
    try:
        yield
    finally:
        connections.close_all()


def session_user(scope):
    # Only the request side of these middleware applies to a WebSocket.
    # A fresh request avoids retaining a cached session/user for its lifetime.
    request = ASGIRequest({**scope, "method": "GET"}, BytesIO())
    try:
        SessionMiddleware(lambda request: None).process_request(request)
        AuthenticationMiddleware(lambda request: None).process_request(request)
        user = request.user
        if user.is_authenticated and not request.session.modified:
            return user
        # Session rotation would require an HTTP response to update the cookie.
        return None
    finally:
        request.close()


def authenticated_notes(scope, receive, send):
    # A reused worker may have an HTTP request's persistent connection.
    connections.close_all()
    if receive()["type"] != "websocket.connect":
        return
    origins = [value.decode("latin1") for name, value in scope["headers"]
               if name.lower() == b"origin"]
    if len(origins) != 1 or origins[0] not in settings.WEBSOCKET_ALLOWED_ORIGINS:
        send({"type": "websocket.close", "code": 1008})
        return

    with database_work():
        user = session_user(scope)
        user_id = user.pk if user is not None else None
    if user_id is None:
        send({"type": "websocket.close", "code": 1008})
        return
    send({"type": "websocket.accept"})

    while True:
        event = receive()
        if event["type"] == "websocket.disconnect":
            return
        text = event.get("text")
        if text is None or not 1 <= len(text) <= 200:
            send({"type": "websocket.close", "code": 1008})
            return
        with database_work():
            user = session_user(scope)
            reply = None
            if user is not None and user.pk == user_id:
                with transaction.atomic():
                    note = PrivateNote.objects.create(owner=user, text=text)
                reply = json.dumps({"id": note.pk, "text": note.text,
                                    "username": user.get_username()})
        if reply is None:
            send({"type": "websocket.close", "code": 1008})
            return
        send({"type": "websocket.send", "text": reply})
