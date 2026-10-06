"""Compatibility and failure paths that are hard to observe over TCP."""

from __future__ import annotations

import asyncio
import importlib.util
import io
import os
import unittest
from contextlib import ExitStack
from tempfile import SpooledTemporaryFile
from unittest.mock import patch
from wsgiref.util import setup_testing_defaults


DJANGO_AVAILABLE = importlib.util.find_spec("django") is not None


@unittest.skipUnless(DJANGO_AVAILABLE, "requires the django extra")
class DjangoHandlerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
        from asgi_function_profile.django import get_function_application

        cls.handler = get_function_application()

    def scope(self, path="/", **changes):
        return {
            "type": "http", "method": "GET", "path": path,
            "root_path": "", "query_string": b"", "scheme": "http",
            "headers": [(b"host", b"testserver")],
            "server": ("testserver", 80), "client": ("127.0.0.1", 1234),
            **changes,
        }

    def invoke(self, scope, events=None, send=None):
        incoming = iter(events or [{"type": "http.request", "body": b""}])
        outgoing = []
        self.handler(scope, lambda: next(incoming), send or outgoing.append)
        return outgoing

    def test_same_views_match_wsgi_without_sync_async_adaptation(self):
        from asgi_function_profile.django import DjangoFunctionHandler
        from django.core.handlers.wsgi import WSGIHandler

        # Fail if Django silently bridges either direction in our sync path.
        with ExitStack() as stack:
            for target in (
                "django.core.handlers.base.async_to_sync",
                "django.core.handlers.base.sync_to_async",
                "django.http.response.async_to_sync",
                "django.dispatch.dispatcher.async_to_sync",
            ):
                stack.enter_context(patch(target, side_effect=AssertionError(target)))
            # Include middleware construction in the no-adaptation check.
            handler = DjangoFunctionHandler()
            stack.enter_context(patch.object(self, "handler", handler))
            for method, path, body, content_type in (
                ("GET", "/", b"", ""),
                ("POST", "/echo/", b"text=unchanged", "application/x-www-form-urlencoded"),
                ("POST", "/echo/", b"raw bytes", "application/octet-stream"),
                ("GET", "/stream/", b"", ""),
                ("GET", "/download/", b"", ""),
                ("GET", "/missing/", b"", ""),
            ):
                with self.subTest(path=path, content_type=content_type):
                    environ = {}
                    setup_testing_defaults(environ)
                    environ.update({
                        "REQUEST_METHOD": method, "PATH_INFO": path,
                        "SERVER_NAME": "testserver", "HTTP_HOST": "testserver",
                        "CONTENT_LENGTH": str(len(body)),
                        "CONTENT_TYPE": content_type, "wsgi.input": io.BytesIO(body),
                    })
                    started = []
                    response = WSGIHandler()(
                        environ, lambda status, headers: started.append((status, headers))
                    )
                    try:
                        wsgi_body = b"".join(response)
                    finally:
                        response.close()
                    headers = [
                        (b"host", b"testserver"),
                        (b"content-length", str(len(body)).encode()),
                        (b"content-type", content_type.encode()),
                    ]
                    messages = self.invoke(
                        self.scope(path, method=method, headers=headers),
                        [
                            {"type": "http.request", "body": body[:2], "more_body": True},
                            {"type": "http.request", "body": body[2:]},
                        ],
                    )
                    self.assertEqual(messages[0]["status"], int(started[0][0][:3]))
                    self.assertEqual(
                        b"".join(message.get("body", b"") for message in messages),
                        wsgi_body,
                    )
                    self.assertCountEqual(
                        [(key.lower(), value.strip()) for key, value in messages[0]["headers"]],
                        [(key.lower().encode(), value.strip().encode("latin1"))
                         for key, value in started[0][1]],
                    )

    def test_send_failure_closes_generator_request_and_finishes_once(self):
        from django.core import signals
        from django.http import StreamingHttpResponse

        closed = []
        requests = []
        finished = []

        def chunks():
            try:
                yield b"first"
                self.fail("iterator advanced after send failed")
            finally:
                closed.append(True)

        def get_response(request):
            requests.append(request)
            response = StreamingHttpResponse(chunks())
            response._resource_closers.append(request.close)
            return response

        def send(message):
            if message["type"] == "http.response.body":
                raise OSError("client disconnected")

        def on_finished(sender, **kwargs):
            finished.append(sender)

        signals.request_finished.connect(on_finished)
        self.addCleanup(signals.request_finished.disconnect, on_finished)
        with patch.object(self.handler, "get_response", get_response):
            with self.assertRaisesRegex(OSError, "client disconnected"):
                self.invoke(self.scope(), send=send)
        self.assertEqual(closed, [True])
        self.assertTrue(requests[0]._stream.closed)
        self.assertEqual(finished, [type(self.handler)])

    def test_sync_streaming_does_not_exhaust_the_iterator_before_first_send(self):
        from django.core.handlers.asgi import ASGIHandler
        from django.http import StreamingHttpResponse

        consumed = []

        def chunks():
            for chunk in (b"first", b"second"):
                consumed.append(chunk)
                yield chunk

        at_send = []

        def send(message):
            if message.get("body"):
                at_send.append(list(consumed))

        response = StreamingHttpResponse(chunks())
        try:
            self.handler.send_response(response, send)
        finally:
            response.close()
        self.assertEqual(at_send[0], [b"first"])

        # Record the difference from Django 5.2's normal ASGI response path:
        # its sync-iterator fallback materializes the entire iterator.
        consumed.clear()
        at_send.clear()

        async def async_send(message):
            send(message)

        response = StreamingHttpResponse(chunks())
        try:
            with self.assertWarnsRegex(Warning, "must consume synchronous iterators"):
                asyncio.run(ASGIHandler().send_response(response, async_send))
        finally:
            response.close()
        self.assertEqual(at_send[0], [b"first", b"second"])

    def test_early_disconnect_closes_spooled_body_without_running_django(self):
        body = SpooledTemporaryFile(max_size=1, mode="w+b")
        with patch("asgi_function_profile.django.SpooledTemporaryFile", return_value=body):
            with patch.object(self.handler, "get_response") as get_response:
                messages = self.invoke(self.scope(), [
                    {"type": "http.request", "body": b"partial", "more_body": True},
                    {"type": "http.disconnect"},
                ])
        self.assertEqual(messages, [])
        self.assertTrue(body.closed)
        get_response.assert_not_called()

    def test_invalid_request_encoding_becomes_bad_request(self):
        messages = self.invoke(self.scope(query_string=b"\xff"))
        self.assertEqual(messages[0]["status"], 400)

    def test_root_path_is_preserved_and_resolved(self):
        messages = self.invoke(self.scope("/mounted/", root_path="/mounted"))
        self.assertEqual(messages[0]["status"], 200)
        self.assertEqual(messages[1]["body"], b"Hello, world!")

    def test_non_http_scopes_are_left_to_the_outer_application(self):
        for scope_type in ("websocket", "lifespan"):
            with self.subTest(scope_type=scope_type):
                with self.assertRaisesRegex(ValueError, "only handles HTTP"):
                    self.invoke({"type": scope_type})
