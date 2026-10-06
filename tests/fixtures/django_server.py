"""Test instrumentation around the runnable Django example."""

import os
import asyncio
import contextvars
import threading
import time
from pathlib import Path

STATE = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
os.environ["DJANGO_SETTINGS_MODULE"] = "examples.django_demo.settings"
os.environ["FUNCTION_PROFILE_DEMO_DATABASE"] = str(STATE / "db.sqlite3")

from django.conf import settings

settings.MIDDLEWARE = [__name__ + ".thread_check", *settings.MIDDLEWARE]
settings.ROOT_URLCONF = __name__
settings.PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]

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
from django.contrib.auth import get_user_model
from django.core import signals
from django.core.management import call_command
from django.db import connection, connections
from django.http import HttpResponse, JsonResponse, StreamingHttpResponse
from django.urls import path

call_command("migrate", run_syncdb=True, verbosity=0)
for username in ("alice", "bob"):
    get_user_model().objects.create_user(username=username, password="test-password")
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


request_label = contextvars.ContextVar("request_label")


def isolation(request):
    label = request.GET["label"]
    request_label.set(label)
    local.label = label
    wrapper = connections["default"]
    username = request.user.username
    assert connection.in_atomic_block
    assert not os.environ.get("DJANGO_ALLOW_ASYNC_UNSAFE")
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise AssertionError("Django is running on the asyncio thread")
    (STATE / ("isolation-" + label)).touch()
    deadline = time.monotonic() + 3
    while len(list(STATE.glob("isolation-*"))) < 2:
        if time.monotonic() > deadline:
            raise AssertionError("requests did not overlap")
        time.sleep(0.005)
    time.sleep(0.02)
    assert local.label == label
    assert request_label.get() == label
    assert connections["default"] is wrapper
    assert connection.in_atomic_block
    assert request.user.username == username
    return JsonResponse({
        "username": username, "connection": id(wrapper),
        "greenlet": threading.get_ident(), "native_thread": threading.get_native_id(),
    })


urlpatterns = [
    path("isolation/", isolation),
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
    if scope["type"] != "websocket":
        return demo_application(scope, receive, send)

    thread_id = threading.get_ident()

    def check_idle():
        assert threading.get_ident() == thread_id
        assert connection.connection is None
        assert not connection.in_atomic_block

    def checked_receive():
        check_idle()
        return receive()

    def checked_send(message):
        check_idle()
        send(message)

    def check_query(execute, sql, params, many, context):
        assert threading.get_ident() == thread_id
        result = execute(sql, params, many, context)
        if (sql.startswith("INSERT INTO") and '"django_demo_privatenote"' in sql
                and (STATE / "fail-note-write").exists()):
            raise RuntimeError("injected failure after database write")
        return result

    with connection.execute_wrapper(check_query):
        try:
            return demo_application(scope, checked_receive, checked_send)
        finally:
            check_idle()
            with (STATE / "websocket-finished").open("ab") as finished:
                finished.write(b".")
