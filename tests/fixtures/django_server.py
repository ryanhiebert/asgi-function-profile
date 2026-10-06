"""Test instrumentation around the runnable Django example."""

import os
import threading
import time
from pathlib import Path

STATE = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
os.environ["DJANGO_SETTINGS_MODULE"] = "examples.django_demo.settings"
os.environ["FUNCTION_PROFILE_DEMO_DATABASE"] = str(STATE / "db.sqlite3")

from django.conf import settings

settings.MIDDLEWARE = [__name__ + ".thread_check", *settings.MIDDLEWARE]
settings.ROOT_URLCONF = __name__

local = threading.local()


def thread_check(get_response):
    def middleware(request):
        from django.db import connection

        def check_query(execute, sql, params, many, context):
            assert threading.get_ident() == local.started_thread
            return execute(sql, params, many, context)

        assert threading.get_ident() == local.started_thread
        with connection.execute_wrapper(check_query):
            response = get_response(request)
        assert threading.get_ident() == local.started_thread
        response["X-Same-Thread"] = "yes"
        return response

    return middleware


from examples.django_demo.application import application as demo_application
from examples.django_demo.models import Note
from examples.django_demo.urls import urlpatterns as demo_urls
from django.core import signals
from django.db import connection
from django.http import HttpResponse, StreamingHttpResponse
from django.urls import path

with connection.schema_editor() as editor:
    editor.create_model(Note)
connection.close()


def started(sender, scope, **kwargs):
    local.started_thread = threading.get_ident()
    local.path = scope["path"]


def finished(sender, **kwargs):
    assert threading.get_ident() == local.started_thread
    # Django's own request_finished receiver has already closed this connection.
    assert connection.connection is None
    name = local.path.strip("/").replace("/", "-") or "index"
    (STATE / ("finished-" + name)).touch()


signals.request_started.connect(started, weak=False)
signals.request_finished.connect(finished, weak=False)


def streaming(request):
    def chunks():
        try:
            assert threading.get_ident() == local.started_thread
            yield b"first"
            (STATE / "stream-first-sent").touch()
            deadline = time.monotonic() + 10
            while not (STATE / "stream-release").exists():
                if time.monotonic() > deadline:
                    raise TimeoutError("test did not release the streaming response")
                time.sleep(0.005)
            yield b"last"
        finally:
            assert threading.get_ident() == local.started_thread
            (STATE / "stream-closed").touch()

    return StreamingHttpResponse(chunks())


def rollback(request):
    Note.objects.create(text="must roll back")
    raise ValueError("intentional view failure")


def count(request):
    return HttpResponse(str(Note.objects.count()))


urlpatterns = [
    path("gated-stream/", streaming),
    path("rollback/", rollback),
    path("count/", count),
    *demo_urls,
]


def application(scope, receive, send):
    if scope["type"] == "lifespan":
        def observe(message):
            if message["type"] == "lifespan.startup.complete":
                (STATE / "lifespan-started").touch()
            elif message["type"] == "lifespan.shutdown.complete":
                (STATE / "lifespan-stopped").touch()
            send(message)

        return demo_application(scope, receive, observe)
    try:
        return demo_application(scope, receive, send)
    finally:
        if scope["type"] == "websocket":
            (STATE / "websocket-finished").touch()
