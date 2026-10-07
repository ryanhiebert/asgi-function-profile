"""Executable Sentry integration checks; sends nothing to an external service."""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import time
from urllib.request import urlopen
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, get_ident
from types import SimpleNamespace
import unittest

import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration
from sentry_sdk.transport import Transport
from django.conf import settings
from django.http import HttpResponse, StreamingHttpResponse
from django.urls import path

from asgi_function_profile.adapter import FunctionProfileAdapter
from asgi_function_profile.instrumentation import instrument_django


class MemoryTransport(Transport):
    def __init__(self):
        super().__init__()
        self.events = []

    def capture_envelope(self, envelope):
        for item in envelope.items:
            if item.type in ("event", "transaction"):
                self.events.append(item.payload.json)
                if filename := os.environ.get("SENTRY_CHECK_OUTPUT"):
                    with open(filename, "a") as output:
                        output.write(json.dumps(item.payload.json) + "\n")


transport = MemoryTransport()
database_directory = tempfile.TemporaryDirectory()
settings.configure(
    SECRET_KEY="test", DEBUG=False, ALLOWED_HOSTS=["testserver"],
    ROOT_URLCONF=__name__, MIDDLEWARE=[],
    DATABASES={"default": {"ENGINE": "django.db.backends.sqlite3",
                           "NAME": str(Path(database_directory.name) / "test.sqlite3")}},
)
sentry_sdk.init(
    dsn="http://public@localhost/1", transport=transport,
    integrations=[DjangoIntegration()],
    default_integrations=os.environ.get("SENTRY_CHECK_DEFAULTS", "1") == "1",
    traces_sample_rate=1.0, send_default_pii=True,
)
from asgi_function_profile.django import get_function_application

barrier = None
threads = []


def view(request, name):
    from django.db import connection
    threads.append(get_ident())
    if name not in ("clean", "auth"):
        sentry_sdk.set_user({"id": name})
        sentry_sdk.set_tag("request-name", name)
        sentry_sdk.add_breadcrumb(message=name)
    if name == "auth":
        request.user = SimpleNamespace(
            is_authenticated=True, pk=42, email="user@example.test",
            get_username=lambda: "test-user",
        )
    if barrier is not None:
        barrier.wait(timeout=5)
    with connection.cursor() as cursor:
        cursor.execute("SELECT 1")
    if name == "error":
        raise ValueError("view failure")
    if name == "stream":
        def chunks():
            yield b"first"
            raise ValueError("stream failure")
        return StreamingHttpResponse(chunks())
    sentry_sdk.capture_message(name)
    return HttpResponse(name)


urlpatterns = [path("<str:name>/", view)]
handler = get_function_application()


def application(scope, receive, send):
    if scope["type"] == "http":
        return handler(scope, receive, send)
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                send({"type": "lifespan.shutdown.complete"})
                return


async def request(app, name):
    scope = {
        "type": "http", "method": "GET", "path": f"/{name}/",
        "root_path": "", "query_string": b"", "scheme": "http",
        "headers": [(b"host", b"testserver"),
                    (b"sentry-trace", b"0123456789abcdef0123456789abcdef-0123456789abcdef-1")],
        "server": ("testserver", 80), "client": ("127.0.0.1", 1234),
    }
    async def receive():
        return {"type": "http.request", "body": b""}
    messages = []
    async def send(event):
        messages.append(event)
    await app(scope, receive, send)
    return messages


class Checks(unittest.TestCase):
    def setUp(self):
        transport.events.clear()
        threads.clear()

    def run_requests(self, names, workers=1):
        with ThreadPoolExecutor(max_workers=workers) as executor:
            app = instrument_django(FunctionProfileAdapter(handler, executor=executor))
            async def run():
                return await asyncio.gather(*(request(app, name) for name in names))
            return asyncio.run(run())

    def test_transactions_sql_and_trace_continuation(self):
        self.run_requests(["ok"])
        transaction, = [e for e in transport.events if e.get("type") == "transaction"]
        self.assertEqual(transaction["transaction"], "/{name}/")
        self.assertEqual(transaction["contexts"]["trace"]["trace_id"], "0123456789abcdef0123456789abcdef")
        self.assertTrue(any(s["op"] == "db" for s in transaction["spans"]))
        self.assertTrue(any(s["op"] == "view.render" for s in transaction["spans"]))
        self.assertEqual(transaction["contexts"]["trace"]["status"], "ok")

    def test_view_error_captured_once_with_request_and_user(self):
        messages, = self.run_requests(["error"])
        self.assertEqual(messages[0]["status"], 500)
        event, = [e for e in transport.events if "exception" in e]
        self.assertEqual(event["exception"]["values"][-1]["value"], "view failure")
        self.assertEqual(event["user"]["id"], "error")
        self.assertTrue(event["request"]["url"].endswith("/error/"))
        transaction, = [e for e in transport.events if e.get("type") == "transaction"]
        self.assertEqual(transaction["contexts"]["trace"]["status"], "internal_error")

    def test_django_request_user_is_extracted(self):
        self.run_requests(["auth"])
        event, = [e for e in transport.events if e.get("message") == "auth"]
        self.assertEqual(str(event["user"]["id"]), "42")
        self.assertEqual(event["user"]["username"], "test-user")
        self.assertEqual(event["user"]["email"], "user@example.test")

    def test_stream_error_captured_and_transaction_finishes(self):
        with self.assertRaisesRegex(ValueError, "stream failure"):
            self.run_requests(["stream"])
        event, = [e for e in transport.events if "exception" in e]
        self.assertEqual(event["exception"]["values"][-1]["value"], "stream failure")
        self.assertEqual(event["user"]["id"], "stream")
        self.assertEqual(len([e for e in transport.events if e.get("type") == "transaction"]), 1)

    def test_reused_thread_does_not_retain_context(self):
        with ThreadPoolExecutor(max_workers=1) as executor:
            app = instrument_django(FunctionProfileAdapter(handler, executor=executor))
            async def run():
                await request(app, "first")
                await request(app, "clean")
            asyncio.run(run())
        self.assertEqual(len(set(threads)), 1)
        clean, = [e for e in transport.events if e.get("message") == "clean"]
        self.assertFalse(clean.get("user"))
        self.assertNotIn("request-name", clean.get("tags", {}))
        self.assertFalse(any(b.get("message") == "first" for b in clean.get("breadcrumbs", {}).get("values", [])))

    def test_overlapping_threads_keep_context_separate(self):
        global barrier
        barrier = Barrier(2)
        try:
            self.run_requests(["alice", "bob"], workers=2)
        finally:
            barrier = None
        self.assertEqual(len(set(threads)), 2)
        for name in ("alice", "bob"):
            event, = [e for e in transport.events if e.get("message") == name]
            self.assertEqual(event["user"]["id"], name)
            self.assertEqual(event["tags"]["request-name"], name)
            names = [b["message"] for b in event["breadcrumbs"]["values"]
                     if b.get("message") in ("alice", "bob")]
            self.assertEqual(names, [name])

    def test_without_outer_wrapper_transaction_is_missing(self):
        with sentry_sdk.isolation_scope(), ThreadPoolExecutor(max_workers=1) as executor:
            asyncio.run(request(FunctionProfileAdapter(handler, executor=executor), "baseline"))
        self.assertTrue(any(e.get("message") == "baseline" for e in transport.events))
        self.assertFalse(any(e.get("type") == "transaction" for e in transport.events))

    @unittest.skipUnless(
        importlib.util.find_spec("gunicorn") and importlib.util.find_spec("uvicorn_worker"),
        "requires the gunicorn extra",
    )
    def test_gunicorn_deployment_adds_instrumentation(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        with tempfile.TemporaryDirectory() as directory:
            filename = Path(directory) / "events.jsonl"
            with tempfile.TemporaryFile(mode="w+") as log:
                process = subprocess.Popen(
                    [sys.executable, "-m", "gunicorn", "sentry_checks:application",
                     "--chdir", str(Path(__file__).parent),
                     "--worker-class", "asgi_function_profile.gunicorn.ThreadedUvicornWorker",
                     "--bind", f"127.0.0.1:{port}", "--workers", "1"],
                    env={**os.environ, "SENTRY_CHECK_OUTPUT": str(filename)},
                    stdout=log, stderr=log,
                )
                try:
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            with socket.create_connection(("127.0.0.1", port), timeout=1):
                                pass
                            break
                        except OSError:
                            if process.poll() is not None or time.monotonic() >= deadline:
                                log.seek(0)
                                self.fail(log.read())
                            time.sleep(0.05)
                    # Send exactly one request, allowing cold SDK initialization.
                    from urllib.request import Request
                    with urlopen(Request(f"http://127.0.0.1:{port}/ok/",
                                         headers={"Host": "testserver"}), timeout=10) as response:
                        self.assertEqual(response.read(), b"ok")
                finally:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
                events = [json.loads(line) for line in filename.read_text().splitlines()]
                transaction, = [e for e in events if e.get("type") == "transaction"]
                self.assertEqual(transaction["transaction"], "/{name}/")
                self.assertTrue(any(s["op"] == "db" for s in transaction["spans"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
