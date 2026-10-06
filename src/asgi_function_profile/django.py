"""Experimental Django 5.2 HTTP handler for the function profile."""

from tempfile import SpooledTemporaryFile

import django
from django.conf import settings
from django.core import signals
from django.core.handlers.asgi import ASGIHandler, get_script_prefix
from django.urls import set_script_prefix


def get_function_application():
    """Initialize Django and return an ordinary function-profile handler."""
    django.setup(set_prefix=False)
    return DjangoFunctionHandler()


class DjangoFunctionHandler(ASGIHandler):
    """Reuse Django's request parsing, but execute its synchronous pipeline.

    The inherited async entry points are unused. With synchronous application
    components, signals, middleware, views, iteration, and cleanup stay in the
    calling thread. Async views retain Django's own fallback adaptation.
    Non-HTTP scopes belong to an outer function-profile application.
    """

    def __init__(self):
        self.load_middleware(is_async=False)

    def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            raise ValueError("DjangoFunctionHandler only handles HTTP scopes")

        # Match Django ASGI's body buffering, including disk spill for uploads.
        with SpooledTemporaryFile(
            max_size=settings.FILE_UPLOAD_MAX_MEMORY_SIZE, mode="w+b"
        ) as body:
            while True:
                message = receive()
                if message["type"] == "http.disconnect":
                    return
                if message["type"] != "http.request":
                    raise ValueError(f"unexpected request event: {message['type']}")
                body.write(message.get("body", b""))
                if not message.get("more_body", False):
                    break
            body.seek(0)

            set_script_prefix(get_script_prefix(scope))
            request = response = None
            try:
                signals.request_started.send(sender=type(self), scope=scope)
                request, response = self.create_request(scope, body)
                if request is not None:
                    response = self.get_response(request)
                response._handler_class = type(self)
                self.send_response(response, send)
            finally:
                if response is not None:
                    # Also closes the request and emits request_finished.
                    response.close()
                else:
                    if request is not None:
                        request.close()
                    signals.request_finished.send(sender=type(self))

    def send_response(self, response, send):
        if response.streaming and response.is_async:
            raise TypeError("the function handler requires a synchronous iterator")
        headers = [
            (name.lower().encode("ascii"), value.encode("latin1"))
            for name, value in response.items()
        ]
        headers.extend(
            (b"set-cookie", cookie.output(header="").strip().encode("ascii"))
            for cookie in response.cookies.values()
        )
        send({
            "type": "http.response.start",
            "status": response.status_code,
            "headers": headers,
        })

        # Both ordinary and streaming responses are iterable in Django's sync
        # pipeline. Wait for each send before advancing the iterator.
        for part in response:
            for chunk, _ in self.chunk_bytes(part):
                send({
                    "type": "http.response.body",
                    "body": chunk,
                    "more_body": True,
                })
        send({"type": "http.response.body", "body": b"", "more_body": False})
