"""Identical synchronous Django views for WSGI and function-profile probes."""

import os
import time

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "benchmarks.settings")

from django.http import HttpResponse
from django.urls import path
from django.core.wsgi import get_wsgi_application
from django.core.asgi import get_asgi_application
from asgi_function_profile.django import get_function_application


def fast(request):
    return HttpResponse(b"hello from Django\n", content_type="text/plain")


def wait(request):
    time.sleep(0.01)  # Repeatable artificial waiting, not a database benchmark.
    return fast(request)


urlpatterns = [path("", fast), path("wait/", wait)]
wsgi_application = get_wsgi_application()
http_application = get_function_application()
coroutine_application = get_asgi_application()


def function_application(scope, receive, send):
    if scope["type"] == "http":
        return http_application(scope, receive, send)
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                send({"type": "lifespan.startup.complete"})
            else:
                send({"type": "lifespan.shutdown.complete"})
                return
    if receive()["type"] != "websocket.connect":
        return
    send({"type": "websocket.accept"})
    while True:
        event = receive()
        if event["type"] == "websocket.disconnect":
            return
        send({"type": "websocket.send", "text": event["text"]})
