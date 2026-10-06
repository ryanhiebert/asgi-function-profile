"""Real HTTP login, Django session cookies, and synchronous WebSocket work."""

import http.cookiejar
import importlib.util
import json
import unittest
import urllib.error
import urllib.parse
import urllib.request

import test_server


DEPENDENCIES_AVAILABLE = all(importlib.util.find_spec(name) is not None
                             for name in ("django", "uvicorn", "websockets"))
ORIGIN = "http://127.0.0.1:8000"


class Browser:
    """Exercise login over HTTP with normal cookie and redirect handling."""

    def __init__(self, port):
        self.base_url = f"http://127.0.0.1:{port}"
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPCookieProcessor(self.cookies),
        )

    def get(self, path):
        return self.opener.open(self.base_url + path, timeout=5)

    def post(self, path, fields=None, csrf=True):
        fields = dict(fields or {})
        if csrf:
            fields["csrfmiddlewaretoken"] = next(
                cookie.value for cookie in self.cookies if cookie.name == "csrftoken"
            )
        return self.opener.open(
            self.base_url + path, urllib.parse.urlencode(fields).encode(), timeout=5
        )

    def login(self, username="alice"):
        with self.get("/login/") as response:
            response.read()
        with self.post("/login/", {"username": username, "password": "test-password"}) as response:
            if not response.url.endswith("/account/"):
                raise AssertionError("login did not reach the protected account page")
            response.read()

    def socket_cookie(self):
        request = urllib.request.Request(self.base_url + "/ws/notes/")
        self.cookies.add_cookie_header(request)
        return request.get_header("Cookie", "")

    def me(self):
        with self.get("/me/") as response:
            return json.load(response)


@unittest.skipUnless(DEPENDENCIES_AVAILABLE, "requires django/server extras and test group")
class AuthenticatedSocketTests(test_server.UvicornTestCase):
    application_target = "django_server:application"

    def websocket(self, browser=None, *, origin=ORIGIN, extra_headers=None):
        from websockets.sync.client import connect

        headers = list(extra_headers or [])
        if browser is not None:
            headers.append(("Cookie", browser.socket_cookie()))
        return connect(
            f"ws://127.0.0.1:{self.port}/ws/notes/", origin=origin,
            additional_headers=headers, proxy=None, open_timeout=5, close_timeout=2,
        )

    def wait_for_sockets(self, count):
        marker = self.marker("websocket-finished")
        self.wait_for(lambda: marker.exists() and marker.stat().st_size >= count)

    def test_real_login_identity_orm_and_user_isolation(self):
        alice, bob = Browser(self.port), Browser(self.port)
        alice.login("alice")
        bob.login("bob")
        self.assertEqual(alice.me(), {"username": "alice", "notes": []})
        with self.websocket(alice) as socket:
            for text in ("first note", "second note"):
                socket.send(text)
                reply = json.loads(socket.recv(timeout=5))
                self.assertEqual(reply["username"], "alice")
                self.assertEqual(reply["text"], text)
            # Read the committed changes via normal Django HTTP while WS stays open.
            self.assertEqual(alice.me()["notes"], ["first note", "second note"])
            self.assertEqual(bob.me(), {"username": "bob", "notes": []})
        self.wait_for_sockets(1)
        with self.websocket(bob) as socket:
            socket.send("Bob's note")
            self.assertEqual(json.loads(socket.recv(timeout=5))["username"], "bob")
        self.wait_for_sockets(2)
        self.assertEqual(bob.me()["notes"], ["Bob's note"])
        self.assertEqual(alice.me()["notes"], ["first note", "second note"])

    def test_anonymous_and_invalid_sessions_are_rejected_before_accept(self):
        from websockets.exceptions import InvalidStatus

        for count, cookie in enumerate((None, "sessionid=invalid", "sessionid=" + "a" * 32), 1):
            with self.subTest(cookie=cookie):
                with self.assertRaises(InvalidStatus) as error:
                    with self.websocket(extra_headers=[("Cookie", cookie)] if cookie else []):
                        self.fail("anonymous socket was accepted")
                self.assertEqual(error.exception.response.status_code, 403)
                self.wait_for_sockets(count)

    def test_origin_rejection_even_with_a_valid_session(self):
        from websockets.exceptions import InvalidStatus

        browser = Browser(self.port)
        browser.login()
        for count, origin in enumerate((None, "null", "https://evil.example", ORIGIN + ".evil"), 1):
            with self.subTest(origin=origin):
                with self.assertRaises(InvalidStatus) as error:
                    with self.websocket(browser, origin=origin):
                        self.fail("untrusted origin was accepted")
                self.assertEqual(error.exception.response.status_code, 403)
                self.wait_for_sockets(count)
        with self.assertRaises(InvalidStatus):
            with self.websocket(browser, extra_headers=[("Origin", ORIGIN)]):
                self.fail("duplicate origin was accepted")
        # Uvicorn may reject duplicate Origin headers before invoking the app.
        self.assertEqual(browser.me()["notes"], [])

    def test_logout_revokes_open_socket_on_next_message_and_reconnect(self):
        from websockets.exceptions import ConnectionClosed, InvalidStatus

        browser = Browser(self.port)
        browser.login()
        old_cookie = browser.socket_cookie()
        with self.websocket(browser) as socket:
            socket.send("before logout")
            socket.recv(timeout=5)
            with browser.post("/logout/") as response:
                response.read()
            socket.send("must not be saved")
            with self.assertRaises(ConnectionClosed) as error:
                socket.recv(timeout=5)
            self.assertEqual(error.exception.rcvd.code, 1008)
        self.wait_for_sockets(1)
        with self.assertRaises(InvalidStatus):
            with self.websocket(extra_headers=[("Cookie", old_cookie)]):
                self.fail("revoked cookie was accepted")
        self.wait_for_sockets(2)
        browser.login()
        self.assertEqual(browser.me()["notes"], ["before logout"])

    def test_csrf_protects_http_login_and_logout(self):
        browser = Browser(self.port)
        with browser.get("/login/") as response:
            response.read()
        with self.assertRaises(urllib.error.HTTPError) as error:
            browser.post("/login/", {"username": "alice", "password": "test-password"}, csrf=False)
        self.assertEqual(error.exception.code, 403)
        error.exception.close()
        browser.login()
        with self.assertRaises(urllib.error.HTTPError) as error:
            browser.post("/logout/", csrf=False)
        self.assertEqual(error.exception.code, 403)
        error.exception.close()
        self.assertEqual(browser.me()["username"], "alice")

    def test_invalid_messages_and_abrupt_disconnect_release_resources(self):
        from websockets.exceptions import ConnectionClosed

        browser = Browser(self.port)
        browser.login()
        for count, message in enumerate((b"binary", "", "x" * 201), 1):
            with self.subTest(message=message):
                with self.websocket(browser) as socket:
                    socket.send(message)
                    with self.assertRaises(ConnectionClosed) as error:
                        socket.recv(timeout=5)
                    self.assertEqual(error.exception.rcvd.code, 1008)
                self.wait_for_sockets(count)
        with self.websocket(browser) as socket:
            socket.send("survives disconnect")
            socket.recv(timeout=5)
            # Close TCP without a WebSocket closing handshake.
            import socket as socket_module
            socket.socket.shutdown(socket_module.SHUT_RDWR)
            socket.socket.close()
        self.wait_for_sockets(4)
        self.assertEqual(browser.me()["notes"], ["survives disconnect"])

    def test_database_failure_rolls_back_and_releases_resources(self):
        from websockets.exceptions import ConnectionClosed

        browser = Browser(self.port)
        browser.login()
        self.marker("fail-note-write").touch()
        with self.websocket(browser) as socket:
            socket.send("must roll back")
            with self.assertRaises(ConnectionClosed):
                socket.recv(timeout=5)
        self.wait_for_sockets(1)
        self.assertEqual(browser.me()["notes"], [])
