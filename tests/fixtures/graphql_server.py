"""Two ordinary GraphQL schemas sharing the existing Django server fixture."""

import json
import asyncio
from collections.abc import AsyncIterator
import threading

import sentry_sdk
from sentry_sdk.integrations.django import DjangoIntegration
from sentry_sdk.integrations.graphene import GrapheneIntegration
from sentry_sdk.integrations.strawberry import StrawberryIntegration
from sentry_sdk.transport import Transport


class LocalTransport(Transport):
    def capture_envelope(self, envelope):
        for item in envelope.items:
            if item.type in ("event", "transaction"):
                with transport_lock, (STATE / "sentry-events").open("a") as output:
                    output.write(json.dumps(item.payload.json) + "\n")


transport_lock = threading.Lock()
sentry_sdk.init(
    dsn="http://public@localhost/1", transport=LocalTransport(), traces_sample_rate=1,
    enable_backpressure_handling=False,
    send_default_pii=True, integrations=[DjangoIntegration(), GrapheneIntegration(),
                                      StrawberryIntegration(async_execution=False)],
)

import django_server
from django.db import connections, transaction
from examples.django_demo.models import PrivateNote
from asgi_function_profile.graphene import GrapheneDjangoWebSocket
from asgi_function_profile.strawberry import StrawberryDjangoWebSocket
import graphene
import strawberry

STATE = django_server.STATE


def context(request, params, cancelled):
    request.cancelled = cancelled
    request.connection_params = params
    request.operation_thread = threading.get_ident()
    sentry_sdk.set_user({"id": str(request.user.pk), "username": request.user.username})
    sentry_sdk.set_tag("client-label", params.get("label", "none"))
    return request


def whoami(request):
    assert threading.get_ident() == request.operation_thread
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass
    else:
        raise AssertionError("resolver ran in an asyncio loop")
    assert request.user.is_authenticated
    # Exercise ORM access with Django's async-safety checks still enabled.
    PrivateNote.objects.filter(owner=request.user).count()
    return request.user.username


def add_note(request, text):
    whoami(request)
    with transaction.atomic():
        PrivateNote.objects.create(owner=request.user, text=text)
    return PrivateNote.objects.filter(owner=request.user).count()


def ticks(request, steps, fail=False):
    try:
        for value in range(steps):
            whoami(request)
            yield value
            if fail:
                raise ValueError("subscription source failed")
            if request.cancelled.wait(0.03):
                break
    finally:
        with transport_lock, (STATE / "graphql-source-closed").open("a") as marker:
            marker.write("x")


class GQuery(graphene.ObjectType):
    whoami = graphene.String(required=True)
    broken = graphene.String()

    def resolve_whoami(root, info):
        return whoami(info.context)

    def resolve_broken(root, info):
        raise ValueError("resolver failed")


class AddNote(graphene.Mutation):
    class Arguments:
        text = graphene.String(required=True)
    count = graphene.Int()

    def mutate(root, info, text):
        return AddNote(count=add_note(info.context, text))


class GMutation(graphene.ObjectType):
    add_note = AddNote.Field()


class GSubscription(graphene.ObjectType):
    ticks = graphene.Int(steps=graphene.Int(default_value=3), fail=graphene.Boolean(default_value=False))
    asynchronous = graphene.Int()

    def subscribe_ticks(root, info, steps, fail):
        if steps < 0:
            raise ValueError("source denied")
        return ticks(info.context, steps, fail)

    async def subscribe_asynchronous(root, info):
        yield 0


@strawberry.type
class SQuery:
    @strawberry.field
    def whoami(self, info: strawberry.Info) -> str:
        return whoami(info.context)

    @strawberry.field
    def broken(self) -> str | None:
        raise ValueError("resolver failed")


@strawberry.type
class SMutation:
    @strawberry.mutation
    def add_note(self, info: strawberry.Info, text: str) -> int:
        return add_note(info.context, text)


@strawberry.type
class SSubscription:
    @strawberry.subscription(graphql_type=int)
    def ticks(self, info: strawberry.Info, steps: int = 3, fail: bool = False):
        if steps < 0:
            raise ValueError("source denied")
        return ticks(info.context, steps, fail)

    @strawberry.subscription
    async def asynchronous(self) -> AsyncIterator[int]:
        yield 0


def authenticated(request, parameters):
    if parameters.get("fail_connect"):
        raise ValueError("initialization failed")
    return request.user.is_authenticated and not request.session.modified


options = {"allowed_origins": {"http://127.0.0.1:8000"}, "get_context": context,
           "on_connect": authenticated, "connection_init_timeout": 0.5}
applications = {
    "/graphene": GrapheneDjangoWebSocket(graphene.Schema(query=GQuery, mutation=GMutation,
                                                       subscription=GSubscription), **options),
    "/strawberry": StrawberryDjangoWebSocket(strawberry.Schema(query=SQuery, mutation=SMutation,
                                                             subscription=SSubscription), **options),
}


def application(scope, receive, send):
    if scope["type"] == "websocket" and scope["path"] in applications:
        try:
            return applications[scope["path"]](scope, receive, send)
        finally:
            assert all(connection.connection is None for connection in connections.all())
            with transport_lock, (STATE / "graphql-socket-finished").open("a") as marker:
                marker.write("x")
    try:
        return django_server.application(scope, receive, send)
    finally:
        if scope["type"] == "lifespan":
            client = sentry_sdk.get_client()
            client.close()
            # SDK close signals its session flusher but doesn't join it. Keep
            # the fixture's exact native-thread exit check deterministic.
            if client.session_flusher._thread is not None:
                client.session_flusher._thread.join(timeout=1)
