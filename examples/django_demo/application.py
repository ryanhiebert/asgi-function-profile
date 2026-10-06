"""Compose Django HTTP and a synchronous WebSocket echo application."""

import os

from asgi_function_profile.django import get_function_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
http_application = get_function_application()

from .sockets import authenticated_notes


def application(scope, receive, send):
    if scope["type"] == "http":
        return http_application(scope, receive, send)
    if scope["type"] == "websocket":
        if scope["path"] == "/ws/notes/":
            return authenticated_notes(scope, receive, send)
        return websocket(scope, receive, send)
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                send({"type": "lifespan.shutdown.complete"})
                return
    raise ValueError(f"unsupported scope: {scope['type']}")


def websocket(scope, receive, send):
    if receive()["type"] != "websocket.connect":
        return
    if scope["path"] != "/ws/":
        send({"type": "websocket.close", "code": 1008})
        return
    send({"type": "websocket.accept"})
    while True:
        event = receive()
        if event["type"] == "websocket.disconnect":
            return
        if event.get("text") is not None:
            send({"type": "websocket.send", "text": event["text"]})
        else:
            send({"type": "websocket.send", "bytes": event["bytes"]})
