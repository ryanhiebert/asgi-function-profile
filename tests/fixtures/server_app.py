"""Instrumented function-profile application for real-server tests."""

from __future__ import annotations

import os
import time

from pathlib import Path
from typing import Any


STATE_DIRECTORY = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])


def mark(name: str, content: str = "") -> None:
    (STATE_DIRECTORY / name).write_text(content)


def application(scope: dict[str, Any], receive, send) -> None:
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                scope["state"]["test_status"] = "ready"
                mark("lifespan-started")
                send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                mark("lifespan-stopped")
                send({"type": "lifespan.shutdown.complete"})
                return

    if scope["type"] != "http":
        raise ValueError(f"unsupported scope type: {scope['type']}")

    path = scope["path"]
    if path == "/stream":
        _stream(scope, receive, send)
    elif path == "/disconnect":
        _observe_disconnect(receive)
    elif path == "/backpressure":
        _produce_until_disconnected(receive, send)
    elif path == "/health":
        _health(scope, receive, send)
    else:
        send({"type": "http.response.start", "status": 404, "headers": []})
        send({"type": "http.response.body", "body": b"not found"})


def _consume_request(receive) -> bool:
    while True:
        event = receive()
        if event["type"] == "http.disconnect":
            return False
        if not event.get("more_body", False):
            return True


def _stream(scope, receive, send) -> None:
    if not _consume_request(receive):
        return

    state = scope.get("state", {}).get("test_status", "missing")
    send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"x-lifespan-state", state.encode("ascii"))],
        }
    )
    send({"type": "http.response.body", "body": b"hel", "more_body": True})
    mark("stream-first-sent")

    release = STATE_DIRECTORY / "stream-release"
    while not release.exists():
        time.sleep(0.005)

    send({"type": "http.response.body", "body": b"lo", "more_body": False})
    mark("stream-finished")


def _observe_disconnect(receive) -> None:
    event = receive()
    if event["type"] == "http.disconnect":
        mark("disconnect-observed")
        return

    mark("disconnect-request-received")
    while receive()["type"] != "http.disconnect":
        pass
    mark("disconnect-observed")


def _produce_until_disconnected(receive, send) -> None:
    if not _consume_request(receive):
        return

    send({"type": "http.response.start", "status": 200, "headers": []})
    mark("backpressure-started")
    progress_path = STATE_DIRECTORY / "backpressure-progress"
    chunk = b"x" * (256 * 1024)

    try:
        with progress_path.open("ab", buffering=0) as progress:
            for _ in range(1024):
                send(
                    {
                        "type": "http.response.body",
                        "body": chunk,
                        "more_body": True,
                    }
                )
                progress.write(b".")
        send({"type": "http.response.body", "body": b"", "more_body": False})
        mark("backpressure-completed")
    finally:
        mark("backpressure-released")


def _health(scope, receive, send) -> None:
    if not _consume_request(receive):
        return

    body = scope.get("state", {}).get("test_status", "missing").encode("ascii")
    send(
        {
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-length", str(len(body)).encode("ascii"))],
        }
    )
    send({"type": "http.response.body", "body": body})
