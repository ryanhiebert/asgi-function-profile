"""Small native application for proxy, worker replacement, and load probes."""

import json
import os
from pathlib import Path

from asgi_function_profile.gunicorn import ThreadedUvicornWorker


class PingWorker(ThreadedUvicornWorker):
    CONFIG_KWARGS = {**ThreadedUvicornWorker.CONFIG_KWARGS,
                     "ws_ping_interval": 0.2, "ws_ping_timeout": 0.5}


def application(scope, receive, send):
    state = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                (state / "lifespan-started").touch()
                send({"type": "lifespan.startup.complete"})
            else:
                (state / "lifespan-stopped").touch()
                send({"type": "lifespan.shutdown.complete"})
                return
    if scope["type"] == "http":
        while receive().get("more_body", False):
            pass
        body = json.dumps({"pid": os.getpid(), "scheme": scope["scheme"],
                           "client": scope["client"], "path": scope["path"]}).encode()
        send({"type": "http.response.start", "status": 200,
              "headers": [(b"content-type", b"application/json"),
                          (b"content-length", str(len(body)).encode())]})
        send({"type": "http.response.body", "body": body})
        return
    if receive()["type"] != "websocket.connect":
        return
    send({"type": "websocket.accept"})
    try:
        while True:
            event = receive()
            if event["type"] == "websocket.disconnect":
                return
            send({"type": "websocket.send", "text": json.dumps({
                "pid": os.getpid(), "echo": event["text"], "scheme": scope["scheme"],
                "origin": dict(scope["headers"]).get(b"origin", b"").decode(),
                "cookie": dict(scope["headers"]).get(b"cookie", b"").decode(),
            })})
    finally:
        (state / "socket-finished").touch()
