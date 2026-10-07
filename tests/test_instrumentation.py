"""Optional SDK discovery must not introduce a core dependency."""

import unittest
from unittest.mock import Mock, patch

from asgi_function_profile.instrumentation import instrument_django


class InstrumentationDiscoveryTests(unittest.TestCase):
    def test_application_without_sdk_is_unchanged(self):
        application = Mock()
        with patch.dict("sys.modules", {"sentry_sdk": None}):
            self.assertIs(instrument_django(application), application)

    def test_sdk_without_django_does_not_import_django_integration(self):
        application = Mock()
        sdk = Mock()
        with patch.dict("sys.modules", {
            "sentry_sdk": sdk, "sentry_sdk.integrations.django": None,
        }):
            self.assertIs(instrument_django(application), application)
        sdk.get_client.assert_not_called()

    def test_disabled_django_integration_is_unchanged(self):
        application = Mock()
        sdk = Mock()
        sdk.get_client.return_value.get_integration.return_value = None
        with patch.dict("sys.modules", {
            "sentry_sdk": sdk, "sentry_sdk.integrations.django": Mock(),
        }):
            self.assertIs(instrument_django(application), application)
