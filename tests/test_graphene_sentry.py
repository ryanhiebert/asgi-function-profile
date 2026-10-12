"""Offline SDK field spans, fallback capture and active-client isolation."""

import importlib.util
from types import SimpleNamespace
import unittest


AVAILABLE = all(importlib.util.find_spec(name) for name in ("sentry_sdk", "graphene"))


@unittest.skipUnless(AVAILABLE, "requires graphene-sentry extra")
class ResolverInstrumentationTests(unittest.TestCase):
    def client(self, enabled):
        import sentry_sdk
        from sentry_sdk.integrations import DidNotEnable
        from sentry_sdk.integrations.graphene import GrapheneIntegration
        from sentry_sdk.transport import Transport
        events = []
        class LocalTransport(Transport):
            def capture_envelope(self, envelope):
                events.extend(item.payload.json for item in envelope.items if item.type in ("event", "transaction"))
        options = dict(dsn="http://public@localhost/1", transport=LocalTransport(),
                       default_integrations=False, auto_enabling_integrations=False,
                       enable_backpressure_handling=False, traces_sample_rate=1)
        try:
            client = sentry_sdk.Client(integrations=[GrapheneIntegration()] if enabled else [], **options)
        except DidNotEnable:
            # An old Graphene version cannot enable SDK capture; fallback stays useful.
            client = sentry_sdk.Client(integrations=[], **options)
        self.addCleanup(client.close)
        return client, events

    def execute(self, middleware, client):
        import graphene
        import sentry_sdk
        failure = ValueError("resolver failed")
        class Query(graphene.ObjectType):
            broken = graphene.String()
            def resolve_broken(root, info):
                raise failure
        with sentry_sdk.isolation_scope() as isolation, sentry_sdk.new_scope() as current:
            isolation.set_client(client)
            current.set_client(client)
            sentry_sdk.set_user({"id": "test-user"})
            with sentry_sdk.start_transaction(name="resolver-test"):
                result = graphene.Schema(query=Query).execute("{ broken }", middleware=[middleware])
        self.assertIs(result.errors[0].original_error, failure)

    def test_one_error_with_sdk_capture_or_fallback_and_field_spans(self):
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                client, events = self.client(enabled)
                self.execute(SentryResolverMiddleware(), client)
                errors = [event for event in events if "exception" in event]
                self.assertEqual(len(errors), 1)
                self.assertEqual(errors[0]["user"]["id"], "test-user")
                spans = [span for event in events for span in event.get("spans", [])]
                self.assertTrue(any(span["op"] == "graphql.resolve" and span["description"] == "broken" for span in spans))

    def test_spans_only_disables_fallback_without_disabling_sdk_capture(self):
        from sentry_sdk.integrations.graphene import GrapheneIntegration
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                client, events = self.client(enabled)
                self.execute(SentryResolverMiddleware(error_capture="none"), client)
                errors = [event for event in events if "exception" in event]
                self.assertEqual(len(errors), int(client.get_integration(GrapheneIntegration) is not None))
                self.assertTrue(any(span["op"] == "graphql.resolve" for event in events for span in event.get("spans", [])))

    def test_same_middleware_uses_each_active_client_without_caching_capture_policy(self):
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        middleware = SentryResolverMiddleware()
        disabled, fallback = self.client(False)
        enabled, sdk = self.client(True)
        for client in (disabled, enabled, disabled):
            self.execute(middleware, client)
        self.assertEqual(sum("exception" in event for event in fallback), 2)
        self.assertEqual(sum("exception" in event for event in sdk), 1)

    def test_explicit_capture_handles_direct_execution_that_bypasses_sdk_boundary(self):
        import graphene
        import sentry_sdk
        from graphql import graphql_sync
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        client, events = self.client(True)
        class Query(graphene.ObjectType):
            broken = graphene.String()
            invalid = graphene.Int()
            def resolve_broken(root, info):
                raise ValueError("direct execution failed")
            def resolve_invalid(root, info):
                return "invalid"
        schema = graphene.Schema(query=Query)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            result = graphql_sync(schema.graphql_schema, "{ broken }",
                                  middleware=[SentryResolverMiddleware(error_capture="always")])
        self.assertEqual(result.errors[0].message, "direct execution failed")
        self.assertEqual(sum("exception" in event for event in events), 1)
        def direct(query, **kwargs):
            return graphql_sync(schema.graphql_schema, query, **kwargs)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            result = instrument_execution(direct, error_capture="always")(
                "{ broken invalid }", middleware=[SentryResolverMiddleware()])
        self.assertEqual(len(result.errors), 2)
        self.assertEqual(sum("exception" in event for event in events), 3)

    def test_invalid_capture_policy_is_rejected(self):
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        with self.assertRaisesRegex(ValueError, "error_capture"):
            SentryResolverMiddleware(error_capture="sometimes")

    def test_direct_resolver_failure_preserves_identity_and_traceback(self):
        import sentry_sdk
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        client, events = self.client(False)
        failure = OSError("resolver backend failed")
        def broken(root, info):
            raise failure
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            try:
                SentryResolverMiddleware().resolve(broken, None, SimpleNamespace(field_name="broken"))
            except OSError as raised:
                self.assertIs(raised, failure)
                import traceback
                self.assertIn("broken", [frame.name for frame in traceback.extract_tb(raised.__traceback__)])
            else:
                self.fail("resolver failure was swallowed")
        self.assertEqual(sum("exception" in event for event in events), 1)

    def test_capture_failure_does_not_replace_the_resolver_error(self):
        from unittest.mock import patch
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        client, events = self.client(False)
        import sentry_sdk
        failure = ValueError("resolver failure")
        def broken(root, info):
            raise failure
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            with patch("sentry_sdk.capture_exception", side_effect=OSError("transport unavailable")):
                with self.assertRaises(ValueError) as raised:
                    SentryResolverMiddleware().resolve(broken, None, SimpleNamespace(field_name="broken"))
        self.assertIs(raised.exception, failure)

    def test_async_resolver_is_rejected_and_closed(self):
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        async def asynchronous():
            pass
        result = asynchronous()
        with self.assertRaisesRegex(TypeError, "ordinary functions"):
            SentryResolverMiddleware(error_capture="none").resolve(
                lambda root, info: result, None, SimpleNamespace(field_name="asynchronous"))
        self.assertIsNone(result.cr_frame)
    def test_execution_boundary_captures_resolver_and_serialization_errors_once(self):
        import graphene
        import sentry_sdk
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        class Query(graphene.ObjectType):
            broken = graphene.Int()
            invalid = graphene.Int()
            def resolve_broken(root, info):
                raise ValueError("resolver failed")
            def resolve_invalid(root, info):
                return "not an integer"
        schema = graphene.Schema(query=Query)
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                client, events = self.client(enabled)
                results = []
                def execute(*args, **kwargs):
                    result = schema.execute(*args, **kwargs)
                    results.append(result)
                    return result
                with sentry_sdk.isolation_scope() as scope:
                    scope.set_client(client)
                    sentry_sdk.set_user({"id": "execution-user"})
                    result = instrument_execution(execute)("{ broken invalid }", middleware=[SentryResolverMiddleware()])
                self.assertIs(result, results[0])
                self.assertEqual(result.data, {"broken": None, "invalid": None})
                self.assertEqual(len(result.errors), 2)
                errors = [event for event in events if "exception" in event]
                self.assertEqual(len(errors), 2)
                self.assertTrue(all(event["user"]["id"] == "execution-user" for event in errors))

    def test_expected_error_predicate_receives_the_actual_graphql_error(self):
        import graphene
        import sentry_sdk
        from graphql import GraphQLError
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        expected = GraphQLError("expected denial", extensions={"code": "EXPECTED"})
        class Query(graphene.ObjectType):
            denied = graphene.String()
            invalid = graphene.Int()
            def resolve_denied(root, info):
                raise expected
            def resolve_invalid(root, info):
                return "not an integer"
        client, events = self.client(False)
        seen = []
        def capture_error(error):
            seen.append(error)
            return error.extensions.get("code") != "EXPECTED"
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            execute = instrument_execution(graphene.Schema(query=Query).execute, capture_error=capture_error)
            result = execute("{ denied invalid }", middleware=[SentryResolverMiddleware()])
        self.assertEqual([id(error) for error in seen], [id(error) for error in result.errors])
        self.assertIs(seen[0].original_error, expected)
        self.assertEqual(seen[0].path, ["denied"])
        self.assertEqual(sum("exception" in event for event in events), 1)

    def test_validation_errors_can_be_reported_or_excluded_by_policy(self):
        import graphene
        import sentry_sdk
        from asgi_function_profile.graphene_sentry import instrument_execution
        class Query(graphene.ObjectType):
            value = graphene.Int()
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            schema = graphene.Schema(query=Query)
            result = instrument_execution(schema.execute)("{ missing }")
            self.assertIsNone(result.errors[0].path)
            self.assertIsNone(result.errors[0].original_error)
            self.assertEqual(sum("exception" in event for event in events), 1)
            result = instrument_execution(schema.execute, capture_error=lambda error: error.path is not None)("{ missing }")
        self.assertEqual(len(result.errors), 1)
        self.assertEqual(sum("exception" in event for event in events), 1)

    def test_nested_execution_wrappers_report_once_per_call(self):
        import graphene
        import sentry_sdk
        from asgi_function_profile.graphene_sentry import instrument_execution
        class Query(graphene.ObjectType):
            invalid = graphene.Int()
            def resolve_invalid(root, info):
                return "invalid"
        client, events = self.client(False)
        execute = instrument_execution(instrument_execution(graphene.Schema(query=Query).execute))
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            for _ in range(2):
                self.assertEqual(len(execute("{ invalid }").errors), 1)
        self.assertEqual(sum("exception" in event for event in events), 2)

    def test_nested_predicate_exclusion_is_not_overridden_by_outer_capture(self):
        import sentry_sdk
        from graphql import ExecutionResult, GraphQLError
        from asgi_function_profile.graphene_sentry import instrument_execution
        result = ExecutionResult(errors=[GraphQLError("expected", extensions={"code": "EXPECTED"})])
        inner = instrument_execution(lambda: result, capture_error=lambda error: False)
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            self.assertIs(instrument_execution(inner)(), result)
        self.assertEqual(events, [])

    def test_copied_context_does_not_retain_capture_ownership_after_execution(self):
        from contextvars import copy_context
        import sentry_sdk
        from graphql import ExecutionResult
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        copied = []
        def execute():
            copied.append(copy_context())
            return ExecutionResult(data={"ok": True})
        client, events = self.client(False)
        def broken(root, info):
            raise ValueError("deferred resolver failed")
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            instrument_execution(execute)()
            with self.assertRaises(ValueError):
                copied[0].run(SentryResolverMiddleware().resolve, broken, None, SimpleNamespace(field_name="broken"))
        self.assertEqual(sum("exception" in event for event in events), 1)

    def test_copied_context_starts_fresh_capture_scopes_for_repeated_execution(self):
        from contextvars import copy_context
        import sentry_sdk
        from graphql import ExecutionResult, GraphQLError
        from asgi_function_profile.graphene_sentry import instrument_execution
        result = ExecutionResult(errors=[GraphQLError("same error object")])
        copied = []
        def execute():
            copied.append(copy_context())
            return result
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            self.assertIs(instrument_execution(execute)(), result)
            deferred = instrument_execution(lambda: result)
            for _ in range(2):
                self.assertIs(copied[0].run(deferred), result)
        self.assertEqual(sum("exception" in event for event in events), 3)

    def test_none_boundary_delegates_resolvers_and_restores_fallback_after_return(self):
        import graphene
        import sentry_sdk
        from sentry_sdk.integrations.graphene import GrapheneIntegration
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        class Query(graphene.ObjectType):
            broken = graphene.Int()
            def resolve_broken(root, info):
                raise ValueError("resolver failed")
        for enabled in (False, True):
            with self.subTest(enabled=enabled):
                client, events = self.client(enabled)
                with sentry_sdk.isolation_scope() as scope:
                    scope.set_client(client)
                    execute = instrument_execution(graphene.Schema(query=Query).execute, error_capture="none")
                    execute("{ broken }", middleware=[SentryResolverMiddleware()])
                count = int(client.get_integration(GrapheneIntegration) is not None)
                self.assertEqual(sum("exception" in event for event in events), count)
                self.execute(SentryResolverMiddleware(), client)
                self.assertEqual(sum("exception" in event for event in events), count + 1)

    def test_escaping_execution_error_is_uncaptured_and_restores_owner_scope(self):
        import sentry_sdk
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        failure = OSError("execution failed")
        def broken():
            raise failure
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            with self.assertRaises(OSError) as raised:
                instrument_execution(broken)()
        self.assertIs(raised.exception, failure)
        self.assertEqual(events, [])
        self.execute(SentryResolverMiddleware(), client)
        self.assertEqual(sum("exception" in event for event in events), 1)

    def test_result_reporting_failure_preserves_data_and_continues(self):
        import sentry_sdk
        from graphql import ExecutionResult, GraphQLError
        from unittest.mock import patch
        from asgi_function_profile.graphene_sentry import instrument_execution
        result = ExecutionResult(data={"safe": 1}, errors=[GraphQLError("one"), GraphQLError("two")])
        errors, data = result.errors, result.data
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            with patch("sentry_sdk.capture_exception", side_effect=[OSError("offline"), None]) as capture:
                self.assertIs(instrument_execution(lambda: result)(), result)
        self.assertEqual(capture.call_count, 2)
        self.assertIs(result.errors, errors)
        self.assertIs(result.data, data)

    def test_async_execution_is_rejected_closed_and_does_not_retain_ownership(self):
        import sentry_sdk
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        async def asynchronous():
            pass
        result = asynchronous()
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            with self.assertRaisesRegex(TypeError, "ordinary functions"):
                instrument_execution(lambda: result)()
        self.assertIsNone(result.cr_frame)
        self.assertEqual(events, [])
        self.execute(SentryResolverMiddleware(), client)
        self.assertEqual(sum("exception" in event for event in events), 1)

    def test_concurrent_execution_scopes_keep_clients_and_reports_independent(self):
        import graphene
        import sentry_sdk
        from concurrent.futures import ThreadPoolExecutor
        from threading import Barrier
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        barrier = Barrier(2)
        class Query(graphene.ObjectType):
            invalid = graphene.Int()
            def resolve_invalid(root, info):
                barrier.wait(5)
                return "invalid"
        execute = instrument_execution(graphene.Schema(query=Query).execute)
        left, left_events = self.client(False)
        right, right_events = self.client(True)
        def run(client, name):
            with sentry_sdk.isolation_scope() as scope, sentry_sdk.new_scope() as current:
                scope.set_client(client)
                current.set_client(client)
                sentry_sdk.set_user({"id": name})
                return execute("{ invalid }", middleware=[SentryResolverMiddleware()])
        with ThreadPoolExecutor(max_workers=2) as threads:
            results = [threads.submit(run, left, "left"), threads.submit(run, right, "right")]
            completed = [result.result(timeout=10) for result in results]
            self.assertTrue(all(len(result.errors) == 1 for result in completed))
            self.assertTrue(all(result.errors[0].message.startswith("Int cannot represent") for result in completed))
        for name, events in (("left", left_events), ("right", right_events)):
            errors = [event for event in events if "exception" in event]
            self.assertEqual(len(errors), 1)
            self.assertEqual(errors[0]["user"]["id"], name)

    @unittest.skipUnless(importlib.util.find_spec("graphene_django"), "requires graphene extra")
    def test_transport_execution_injection_preserves_source_context_and_middleware(self):
        import os
        import django
        import graphene
        import sentry_sdk
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
        django.setup()
        from asgi_function_profile.graphene import GrapheneDjangoWebSocket
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware, instrument_execution
        class Query(graphene.ObjectType):
            value = graphene.Int()
        class Subscription(graphene.ObjectType):
            values = graphene.Int()
            def subscribe_values(root, info):
                return iter([7])
            def resolve_values(root, info):
                return root
        schema = graphene.Schema(query=Query, subscription=Subscription)
        original = schema.execute.__func__
        transport = GrapheneDjangoWebSocket(schema, allowed_origins={"https://example.test"},
            middleware=[SentryResolverMiddleware()], execution=instrument_execution(schema.execute))
        client, events = self.client(False)
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            sources = []
            result = transport.execute("subscription { values }", source=sources)
            self.assertFalse(result.errors)
            self.assertEqual(next(sources[0]), 7)
            result = transport.execute("subscription { values }", root_value="invalid")
        self.assertEqual(result.errors[0].path, ["values"])
        self.assertEqual(sum("exception" in event for event in events), 1)
        self.assertIs(schema.execute.__func__, original)

    @unittest.skipUnless(importlib.util.find_spec("graphene_django"), "requires graphene extra")
    def test_transport_rejects_async_execution_without_instrumentation(self):
        import os
        import django
        import graphene
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
        django.setup()
        from asgi_function_profile.graphene import GrapheneDjangoWebSocket
        class Query(graphene.ObjectType):
            value = graphene.Int()
        async def asynchronous():
            pass
        result = asynchronous()
        transport = GrapheneDjangoWebSocket(graphene.Schema(query=Query), allowed_origins={"https://example.test"},
                                            middleware=[], execution=lambda *args, **kwargs: result)
        with self.assertRaisesRegex(TypeError, "ordinary function"):
            transport.execute("{ value }")
        self.assertIsNone(result.cr_frame)

SERVER = AVAILABLE and all(importlib.util.find_spec(name) for name in
                          ("graphene_django", "uvicorn_worker", "websockets", "strawberry"))


@unittest.skipUnless(SERVER, "requires GraphQL/server extras and test group")
class ExecutionInstrumentationServerTests(unittest.TestCase):
    def test_real_socket_fallback_captures_resolver_and_serialization_once(self):
        import json
        from deployment_support import NativeServer
        from test_django_auth_socket import Browser, ORIGIN
        from websockets.sync.client import connect
        server = NativeServer(target="graphql_server:application", env={"ASGI_FUNCTION_GRAPHENE_SENTRY": "disabled"})
        self.addCleanup(server.close)
        browser = Browser(server.port)
        browser.login("alice")
        with connect(f"ws://127.0.0.1:{server.port}/graphene", origin=ORIGIN,
                     additional_headers=[("Cookie", browser.socket_cookie())],
                     subprotocols=["graphql-transport-ws"], proxy=None, open_timeout=10) as socket:
            socket.send(json.dumps({"type": "connection_init", "payload": {"label": "fallback"}}))
            self.assertEqual(json.loads(socket.recv(timeout=5)), {"type": "connection_ack"})
            socket.send(json.dumps({"type": "subscribe", "id": "query", "payload": {"query": "{ broken invalid }"}}))
            result = json.loads(socket.recv(timeout=5))
            self.assertEqual((result["id"], result["type"]), ("query", "next"))
            self.assertEqual(result["payload"]["data"], {"broken": None, "invalid": None})
            self.assertEqual(len(result["payload"]["errors"]), 2)
            self.assertEqual(json.loads(socket.recv(timeout=5)), {"id": "query", "type": "complete"})
        server.stop()
        events = [json.loads(line) for line in (server.path / "sentry-events").read_text().splitlines()]
        errors = [event for event in events if "exception" in event]
        self.assertEqual(len(errors), 2)
        self.assertTrue(all(event["user"]["username"] == "alice" for event in errors))
        self.assertTrue(all(event["tags"]["client-label"] == "fallback" for event in errors))
