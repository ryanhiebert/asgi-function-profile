"""Small function-profile application for the development runner."""

from typing import Any


def application(scope: dict[str, Any], receive, send) -> None:
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                send({"type": "lifespan.shutdown.complete"})
                return

    if scope["type"] == "http":
        body = bytearray()
        while True:
            event = receive()
            if event["type"] == "http.disconnect":
                return
            body.extend(event.get("body", b""))
            if not event.get("more_body", False):
                break

        send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"application/octet-stream")],
            }
        )
        send({"type": "http.response.body", "body": bytes(body)})
        return

    raise ValueError(f"unsupported scope type: {scope['type']}")
