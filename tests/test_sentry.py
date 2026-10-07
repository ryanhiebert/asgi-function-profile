"""Run SDK monkey patches in a separate process from other Django tests."""

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import unittest


@unittest.skipUnless(
    importlib.util.find_spec("django") and importlib.util.find_spec("sentry_sdk"),
    "requires Django and the Sentry test dependency",
)
class SentryTests(unittest.TestCase):
    def test_native_thread_instrumentation(self):
        for defaults in ("0", "1"):
            with self.subTest(default_integrations=defaults):
                result = subprocess.run(
                    [sys.executable, str(Path(__file__).parent / "fixtures" / "sentry_checks.py")],
                    env={**os.environ, "SENTRY_CHECK_DEFAULTS": defaults},
                    capture_output=True, text=True, timeout=45,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
