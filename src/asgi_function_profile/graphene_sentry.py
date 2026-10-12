"""Optional ordinary Graphene resolver spans with SDK-aware error fallback."""

from inspect import isasyncgen, isawaitable

import sentry_sdk
from sentry_sdk.integrations.graphene import GrapheneIntegration


class SentryResolverMiddleware:
    """Add field spans with an explicit error-capture policy.

    error_capture="none" provides spans only; "always" handles execution that
    bypasses SDK result capture. Configure user attribution and application
    policy through the surrounding context/middleware. This class
    neither initializes Sentry nor changes its integration configuration.
    """

    def __init__(self, *, error_capture="fallback"):
        if error_capture not in ("fallback", "always", "none"):
            raise ValueError("error_capture must be 'fallback', 'always' or 'none'")
        self.error_capture = error_capture

    def resolve(self, next_resolver, root, info, **kwargs):
        with sentry_sdk.start_span(op="graphql.resolve", name=info.field_name):
            try:
                result = next_resolver(root, info, **kwargs)
                if isawaitable(result) or isasyncgen(result):
                    if hasattr(result, "close"):
                        result.close()
                    raise TypeError("function-profile resolvers must be ordinary functions")
                return result
            except Exception:
                try:
                    if self.error_capture == "always" or (
                        self.error_capture == "fallback"
                        and sentry_sdk.get_client().get_integration(GrapheneIntegration) is None
                    ):
                        sentry_sdk.capture_exception()
                except Exception:
                    pass  # Instrumentation must not replace the resolver failure.
                raise
