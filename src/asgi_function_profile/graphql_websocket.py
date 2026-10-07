"""Small native-thread GraphQL WebSocket experiment shared by both schemas.

Wire protocols: graphql-transport-ws and legacy graphql-ws. This does not
implement Channels consumers, channel layers, or a broadcast broker.
"""

import contextvars
import json
import logging
import sys
from contextlib import ExitStack
from copy import deepcopy
from dataclasses import dataclass, field
from io import BytesIO
from threading import Event, RLock, Timer

from django.contrib.auth.middleware import AuthenticationMiddleware
from django.contrib.sessions.middleware import SessionMiddleware
from django.core.handlers.asgi import ASGIRequest
from django.db import connections
from graphql import GraphQLError

from .adapter import BridgeClosedError
from .threads import GrowingThreadExecutor


class _SendFailure(Exception):
    def __init__(self, error):
        self.error = error


@dataclass
class _Operation:
    cancelled: Event = field(default_factory=Event)
    source: object = None

    def cancel(self):
        self.cancelled.set()
        # A source may expose a thread-safe wake-up. Generator.close() is only
        # called by the operation thread, after next() has returned.
        wake = getattr(self.source, "cancel", None)
        if wake is not None:
            try:
                wake()
            except Exception:
                logging.getLogger(__name__).exception("subscription source cancellation failed")


def _request(scope):
    request = ASGIRequest({**scope, "method": "GET"}, BytesIO())
    SessionMiddleware(lambda request: None).process_request(request)
    AuthenticationMiddleware(lambda request: None).process_request(request)
    return request


class DjangoGraphQLWebSocket:
    """Ordinary function application; resolvers never run in an asyncio loop.

    get_context(request, connection_params, cancelled) runs once per operation.
    By default its context is the Django request, with .cancelled (an Event),
    .connection_params and .scope attached. on_connect(request, params) can
    reject initialization by returning False or raising an exception.
    """

    protocols = ("graphql-transport-ws", "graphql-ws")

    def __init__(self, schema, *, allowed_origins, get_context=None,
                 on_connect=None, connection_init_timeout=5):
        if not allowed_origins:
            raise ValueError("configure explicit allowed_origins for cookie-authenticated sockets")
        if connection_init_timeout <= 0:
            raise ValueError("connection_init_timeout must be positive")
        self.schema = schema
        self.allowed_origins = frozenset(allowed_origins)
        self.get_context = get_context
        self.on_connect = on_connect
        self.connection_init_timeout = connection_init_timeout

    def execute(self, query, **kwargs):
        raise NotImplementedError

    def default_context(self, request):
        return request

    def __call__(self, scope, receive, send):
        if scope["type"] != "websocket":
            raise ValueError("GraphQL WebSocket applications only handle websocket scopes")
        if receive()["type"] != "websocket.connect":
            return
        origins = [v.decode("latin1") for k, v in scope.get("headers", []) if k.lower() == b"origin"]
        protocol = next((p for p in self.protocols if p in scope.get("subprotocols", [])), None)
        if len(origins) != 1 or origins[0] not in self.allowed_origins:
            send({"type": "websocket.close", "code": 1008})
            return
        if protocol is None:
            send({"type": "websocket.close", "code": 4406})
            return
        legacy = protocol == "graphql-ws"
        send({"type": "websocket.accept", "subprotocol": protocol})
        lock = RLock()
        operations = {}
        closed = False
        initialized = False
        parameters = {}
        futures = set()
        failures = []

        def frame(message):
            send({"type": "websocket.send", "text": json.dumps(message, allow_nan=False)})

        def close(code):
            nonlocal closed
            with lock:
                if not closed:
                    closed = True
                    try:
                        send({"type": "websocket.close", "code": code})
                    finally:
                        for operation in operations.values():
                            operation.cancel()

        def emit(identifier, operation, kind, payload=None, *, terminal=False):
            with lock:
                if closed or operations.get(identifier) is not operation or operation.cancelled.is_set():
                    return
                message = {"id": identifier, "type": kind}
                if payload is not None:
                    message["payload"] = payload
                try:
                    frame(message)
                except (OSError, BridgeClosedError) as error:
                    raise _SendFailure(error) from error
                if terminal:
                    operations.pop(identifier, None)

        def run(identifier, operation, payload, params):
            if operation.cancelled.is_set():
                return
            request = None
            source = None
            stack = ExitStack()
            try:
                sdk = sys.modules.get("sentry_sdk")
                if sdk is not None:
                    stack.enter_context(sdk.isolation_scope())
                    stack.enter_context(sdk.new_scope())
                connections.close_all()
                request = _request(scope)
                request.scope = scope
                request.connection_params = params
                request.cancelled = operation.cancelled
                context = self.get_context(request, params, operation.cancelled) if self.get_context else self.default_context(request)
                kwargs = {
                    "context_value": context, "operation_name": payload.get("operationName"),
                    "variable_values": payload.get("variables"),
                }
                holder = []
                try:
                    result = self.execute(payload["query"], source=holder, **kwargs)
                    if holder:
                        source = holder[0]
                        operation.source = source
                finally:
                    connections.close_all()
                if result.errors and (holder or all(error.path is None for error in result.errors)):
                    emit(identifier, operation, "error", [error.formatted for error in result.errors], terminal=True)
                    return
                if source is None:
                    emit(identifier, operation, "data" if legacy else "next", self.format_result(result))
                else:
                    while not operation.cancelled.is_set():
                        try:
                            value = next(source)
                            if operation.cancelled.is_set():
                                break
                            result = self.execute(payload["query"], root_value=value, **kwargs)
                            formatted = self.format_result(result)
                        except StopIteration:
                            break
                        finally:
                            connections.close_all()
                        # Wait for the actual network send before requesting
                        # another source item; there is no result queue.
                        emit(identifier, operation, "data" if legacy else "next", formatted)
                emit(identifier, operation, "complete", terminal=True)
            except _SendFailure as failure:
                raise failure.error
            except Exception as error:
                sdk = sys.modules.get("sentry_sdk")
                if sdk is not None:
                    sdk.capture_exception(error)
                formatted = error.formatted if isinstance(error, GraphQLError) else {"message": str(error)}
                try:
                    emit(identifier, operation, "error", [formatted], terminal=True)
                except _SendFailure as failure:
                    raise failure.error
            finally:
                try:
                    if source is not None and hasattr(source, "close"):
                        source.close()
                finally:
                    if request is not None:
                        request.close()
                    connections.close_all()
                    stack.close()

        def operation_finished(future):
            error = future.exception()
            with lock:
                futures.discard(future)
                if error is not None:
                    failures.append(error)
            if error is not None:
                try:
                    close(1011)
                except (OSError, BridgeClosedError):
                    # The original transport failure is already recorded and
                    # re-raised after all operation threads have joined.
                    pass

        timer = Timer(self.connection_init_timeout, lambda: close(4408))
        timer.daemon = True
        timer.start()
        try:
            with GrowingThreadExecutor() as executor:
                try:
                    while not closed:
                        event = receive()
                        if event["type"] == "websocket.disconnect":
                            break
                        try:
                            def invalid_constant(value):
                                raise ValueError("invalid JSON constant")
                            message = json.loads(event.get("text"), parse_constant=invalid_constant)
                            if not isinstance(message, dict) or not isinstance(message.get("type"), str):
                                raise ValueError("invalid message")
                            kind = message["type"]
                            if kind == "connection_init":
                                if initialized:
                                    close(4429)
                                    break
                                timer.cancel()
                                parameters = message.get("payload")
                                if parameters is None:
                                    parameters = {}
                                if not isinstance(parameters, dict):
                                    raise ValueError("invalid initialization payload")
                                request = None
                                try:
                                    request = _request(scope)
                                    if self.on_connect and self.on_connect(request, parameters) is False:
                                        close(4403)
                                        break
                                except Exception as error:
                                    sdk = sys.modules.get("sentry_sdk")
                                    if sdk is not None:
                                        sdk.capture_exception(error)
                                    close(4403)
                                    break
                                finally:
                                    if request is not None:
                                        request.close()
                                    connections.close_all()
                                with lock:
                                    if not closed:
                                        initialized = True
                                        frame({"type": "connection_ack"})
                            elif kind == "ping" and not legacy:
                                with lock:
                                    if not closed:
                                        reply = {"type": "pong"}
                                        if "payload" in message:
                                            if not isinstance(message["payload"], dict):
                                                raise ValueError("invalid ping payload")
                                            reply["payload"] = message["payload"]
                                        frame(reply)
                            elif kind == "pong" and not legacy:
                                if "payload" in message and not isinstance(message["payload"], dict):
                                    raise ValueError("invalid pong payload")
                            elif kind == ("start" if legacy else "subscribe"):
                                if not initialized:
                                    close(4401)
                                    break
                                identifier = message.get("id")
                                payload = message.get("payload")
                                if not isinstance(identifier, str) or not identifier or not isinstance(payload, dict):
                                    raise ValueError("invalid operation")
                                if not isinstance(payload.get("query"), str):
                                    raise ValueError("invalid query")
                                for key, expected in (("variables", dict), ("operationName", str), ("extensions", dict)):
                                    if payload.get(key) is not None and not isinstance(payload[key], expected):
                                        raise ValueError("invalid operation payload")
                                with lock:
                                    if identifier in operations:
                                        close(4409)
                                        break
                                    operation = _Operation()
                                    operations[identifier] = operation
                                    context = contextvars.copy_context()
                                    future = executor.submit(context.run, run, identifier, operation, payload, deepcopy(parameters))
                                    futures.add(future)
                                    future.add_done_callback(operation_finished)
                            elif kind == ("stop" if legacy else "complete"):
                                identifier = message.get("id")
                                if not isinstance(identifier, str) or not identifier:
                                    raise ValueError("invalid operation ID")
                                with lock:
                                    operation = operations.pop(identifier, None)
                                    if operation:
                                        operation.cancel()
                            elif kind == "connection_terminate" and legacy:
                                close(1000)
                                break
                            else:
                                raise ValueError("unknown message type")
                        except (ValueError, TypeError):
                            close(4400)
                            break
                finally:
                    with lock:
                        closed = True
                        for operation in operations.values():
                            operation.cancel()
            if failures:
                raise failures[0]
        finally:
            timer.cancel()
            timer.join()
            connections.close_all()

    @staticmethod
    def format_result(result):
        payload = {"data": result.data}
        if result.errors:
            payload["errors"] = [error.formatted for error in result.errors]
        if result.extensions:
            payload["extensions"] = result.extensions
        return payload
