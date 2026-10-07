"""Optional instrumentation at the coroutine server boundary."""

import sys


def instrument_django(application):
    """Restore Sentry's outer ASGI hooks when Django integration is enabled.

    The function Django handler inherits Sentry's request/view/database hooks,
    but overrides the coroutine handler that Sentry normally wraps. Keep the
    SDK's own middleware outside the adapter so its context enters each worker.
    Do not initialize Sentry or import it for applications that do not use it.
    """
    sdk = sys.modules.get("sentry_sdk")
    if sdk is None:
        return application

    django_integration = sys.modules.get("sentry_sdk.integrations.django")
    if django_integration is None:
        return application

    DjangoIntegration = django_integration.DjangoIntegration

    integration = sdk.get_client().get_integration(DjangoIntegration)
    if integration is None:
        return application

    from sentry_sdk.integrations.asgi import SentryAsgiMiddleware

    return SentryAsgiMiddleware(
        application,
        asgi_version=3,
        span_origin=DjangoIntegration.origin,
        http_methods_to_capture=integration.http_methods_to_capture,
    )
