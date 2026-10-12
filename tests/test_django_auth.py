"""Authentication refresh using real cache and signed-cookie session stores."""

import asyncio
import importlib.util
import os
import unittest
from unittest.mock import patch


users = {}


class SessionBackend:
    def get_user(self, identifier):
        from django.contrib.auth import get_user_model
        data = users.get(identifier)
        return get_user_model()(**data) if data and data["is_active"] else None


@unittest.skipUnless(importlib.util.find_spec("django"), "requires django extra")
class AuthenticationRefreshTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import django
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
        django.setup()

    def setUp(self):
        from django.test import override_settings
        configuration = override_settings(AUTHENTICATION_BACKENDS=[__name__ + ".SessionBackend"],
            CACHES={"default": {"BACKEND": "django.core.cache.backends.locmem.LocMemCache",
                                "LOCATION": self.id()}}, SECRET_KEY="refresh-secret", SECRET_KEY_FALLBACKS=[])
        configuration.enable()
        self.addCleanup(configuration.disable)
        users.clear()

    def request(self, backend="cache"):
        from importlib import import_module
        from django.contrib.auth import BACKEND_SESSION_KEY, HASH_SESSION_KEY, SESSION_KEY, get_user_model
        from django.contrib.auth.middleware import AuthenticationMiddleware
        from django.http import HttpRequest
        user = get_user_model()(pk=1, username="original", password="password-hash", is_active=True)
        users[1] = {"pk": 1, "username": user.username, "password": user.password, "is_active": True}
        store = import_module("django.contrib.sessions.backends." + backend).SessionStore
        session = store()
        session.update({SESSION_KEY: str(user.pk), BACKEND_SESSION_KEY: __name__ + ".SessionBackend",
                        HASH_SESSION_KEY: user.get_session_auth_hash()})
        session.save()
        request = HttpRequest()
        request.session = store(session_key=session.session_key)
        AuthenticationMiddleware(lambda request: None).process_request(request)
        self.assertTrue(request.user.is_authenticated)
        request._acached_user = request._cached_user
        return request

    def test_refresh_updates_user_and_both_django_authentication_caches(self):
        from asgi_function_profile.django_auth import refresh_authentication
        for backend in ("cache", "signed_cookies"):
            with self.subTest(backend=backend):
                request = self.request(backend)
                previous = request.session
                users[1]["username"] = "updated"
                result = refresh_authentication(request)
                self.assertEqual(result.user.username, "updated")
                self.assertIs(request.user, result.user)
                self.assertIs(request._cached_user, result.user)
                self.assertIs(asyncio.run(request.auser()), result.user)
                self.assertIsNot(request.session, previous)
                self.assertIsInstance(request.session, previous.__class__)
                self.assertFalse(result.session_changed)
                self.assertFalse(result.http_login_required)

    def test_pending_session_changes_are_refused_without_changing_request(self):
        from asgi_function_profile.django_auth import PendingSessionChanges, refresh_authentication
        request = self.request()
        request.session["pending"] = "preserve"
        session, user = request.session, request.user
        with patch("asgi_function_profile.django_auth.get_user") as authenticate:
            with self.assertRaises(PendingSessionChanges):
                refresh_authentication(request)
        authenticate.assert_not_called()
        self.assertIs(request.session, session)
        self.assertIs(request.user, user)
        self.assertEqual(request.session["pending"], "preserve")

    def test_backend_failure_preserves_request_references_and_exception(self):
        from asgi_function_profile.django_auth import refresh_authentication
        for operation in ("load", "get_user"):
            with self.subTest(operation=operation):
                request = self.request()
                session, user, cached = request.session, request.user, request._cached_user
                failure = ConnectionError("authentication backend unavailable")
                target = session.__class__ if operation == "load" else SessionBackend
                with patch.object(target, operation, side_effect=failure):
                    with self.assertRaises(ConnectionError) as raised:
                        refresh_authentication(request)
                self.assertIs(raised.exception, failure)
                self.assertIs(request.session, session)
                self.assertIs(request.user, user)
                self.assertIs(request._cached_user, cached)

    def test_expired_signed_cookie_requires_http_login(self):
        from django.test import override_settings
        from asgi_function_profile.django_auth import refresh_authentication
        request = self.request("signed_cookies")
        with override_settings(SESSION_COOKIE_AGE=-1):
            result = refresh_authentication(request)
        self.assertFalse(result.user.is_authenticated)
        self.assertTrue(result.session_changed)
        self.assertTrue(result.http_login_required)

    def test_server_side_session_deletion_requires_http_login(self):
        from asgi_function_profile.django_auth import refresh_authentication
        request = self.request()
        request.session.__class__(session_key=request.session.session_key).delete()
        result = refresh_authentication(request)
        self.assertFalse(result.user.is_authenticated)
        self.assertTrue(result.session_changed)
        self.assertTrue(result.http_login_required)

    def test_signed_cookie_deletion_does_not_revoke_a_previously_issued_cookie(self):
        from asgi_function_profile.django_auth import refresh_authentication
        request = self.request("signed_cookies")
        request.session.__class__(session_key=request.session.session_key).delete()
        result = refresh_authentication(request)
        self.assertTrue(result.user.is_authenticated)
        self.assertFalse(result.session_changed)
        self.assertFalse(result.http_login_required)

    def test_password_change_flushes_authentication_and_reports_session_change(self):
        from asgi_function_profile.django_auth import refresh_authentication
        for backend in ("cache", "signed_cookies"):
            with self.subTest(backend=backend):
                request = self.request(backend)
                users[1]["password"] = "changed-password-hash"
                result = refresh_authentication(request)
                self.assertFalse(result.user.is_authenticated)
                self.assertIsNone(request.session.session_key)
                self.assertTrue(result.session_changed)
                self.assertTrue(result.http_login_required)

    def test_secret_fallback_can_require_http_even_while_user_is_authenticated(self):
        from django.test import override_settings
        from asgi_function_profile.django_auth import refresh_authentication
        for backend in ("cache", "signed_cookies"):
            with self.subTest(backend=backend):
                request = self.request(backend)
                key = request.session.session_key
                with override_settings(SECRET_KEY="rotated-secret", SECRET_KEY_FALLBACKS=["refresh-secret"]):
                    result = refresh_authentication(request)
                self.assertTrue(result.user.is_authenticated)
                self.assertTrue(result.session_changed)
                self.assertTrue(result.http_login_required)
                if backend == "cache":
                    self.assertNotEqual(request.session.session_key, key)
