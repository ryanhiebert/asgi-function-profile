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
        from asgi_function_profile.graphene_sentry import SentryResolverMiddleware
        client, events = self.client(True)
        class Query(graphene.ObjectType):
            broken = graphene.String()
            def resolve_broken(root, info):
                raise ValueError("direct execution failed")
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            result = graphql_sync(graphene.Schema(query=Query).graphql_schema, "{ broken }",
                                  middleware=[SentryResolverMiddleware(error_capture="always")])
        self.assertEqual(result.errors[0].message, "direct execution failed")
        self.assertEqual(sum("exception" in event for event in events), 1)

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
