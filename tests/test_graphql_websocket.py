"""Both schema libraries over real native-worker WebSocket connections."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest

import test_django_auth_socket
import test_gunicorn
import test_server


AVAILABLE = test_gunicorn.GUNICORN_AVAILABLE and all(
    importlib.util.find_spec(name) is not None
    for name in ("graphene_django", "strawberry", "websockets", "sentry_sdk")
)


@unittest.skipUnless(AVAILABLE, "requires graphene/strawberry/gunicorn extras and test group")
class GraphQLWebSocketTests(test_gunicorn.GunicornCommand, test_server.UvicornTestCase):
    application_target = "graphql_server:application"

    def wait_for_file(self, name, *, timeout=10):
        # Loading both schemas and the SDK costs more than the minimal fixture.
        return super().wait_for_file(name, timeout=timeout)

    def websocket(self, library, browser=None, *, protocol="graphql-transport-ws", origin=test_django_auth_socket.ORIGIN):
        from websockets.sync.client import connect
        headers = [("Cookie", browser.socket_cookie())] if browser else []
        return connect(f"ws://127.0.0.1:{self.port}/{library}", origin=origin,
                       additional_headers=headers, subprotocols=[protocol], proxy=None,
                       open_timeout=10, close_timeout=2)

    def login(self, user="alice"):
        browser = test_django_auth_socket.Browser(self.port)
        browser.login(user)
        return browser

    def init(self, socket, label="test"):
        socket.send(json.dumps({"type": "connection_init", "payload": {"label": label}}))
        self.assertEqual(json.loads(socket.recv(timeout=5)), {"type": "connection_ack"})

    def start(self, socket, identifier, query, *, protocol="graphql-transport-ws", **payload):
        socket.send(json.dumps({"type": "start" if protocol == "graphql-ws" else "subscribe",
                                "id": identifier, "payload": {"query": query, **payload}}))

    def result(self, socket, identifier, kind="next"):
        message = json.loads(socket.recv(timeout=5))
        self.assertEqual(message["id"], identifier)
        self.assertEqual(message["type"], kind)
        return message.get("payload")

    def command(self, socket, query):
        self.start(socket, "command", query)
        result = self.result(socket, "command")
        self.result(socket, "command", "complete")
        return result

    def test_broadcast_sources_cancel_and_recheck_authorization(self):
        for library, user in (("graphene", "alice"), ("strawberry", "bob")):
            browser = self.login(user)
            with self.subTest(library=library), self.websocket(library, browser) as socket, self.websocket(library, browser) as control:
                self.init(socket)
                self.init(control)
                self.start(socket, "live", 'subscription { updates(topic: "private") }')
                self.wait_for(lambda: self.command(control, "{ subscribers }")["data"]["subscribers"] == 1)
                self.command(control, 'mutation { publish(topic: "private", value: 7) }')
                self.assertEqual(self.result(socket, "live"), {"data": {"updates": 7}})
                socket.send(json.dumps({"type": "complete", "id": "live"}))
                self.wait_for(lambda: self.command(control, "{ subscribers }")["data"]["subscribers"] == 0)
                self.start(socket, "live", 'subscription { updates(topic: "private") }')
                self.wait_for(lambda: self.command(control, "{ subscribers }")["data"]["subscribers"] == 1)
                self.command(control, "mutation { revoke }")
                self.command(control, 'mutation { publish(topic: "private", value: 8) }')
                error = self.result(socket, "live", "error")
                self.assertEqual(error[0]["message"], "subscription access revoked")
                self.assertEqual(self.command(socket, "{ subscribers }")["data"]["subscribers"], 0)
                self.assertEqual(self.command(control, "{ subscribers }")["data"]["subscribers"], 0)

    def test_broadcast_drain_completes_waiting_operations(self):
        browser = self.login()
        with self.websocket("graphene", browser) as first, self.websocket("strawberry", browser) as second, self.websocket("graphene", browser) as control:
            for socket in (first, second, control):
                self.init(socket)
            for socket in (first, second):
                self.start(socket, "live", 'subscription { updates(topic: "one") }')
            self.wait_for(lambda: self.command(control, "{ subscribers }")["data"]["subscribers"] == 2)
            self.command(control, "mutation { drain }")
            for socket in (first, second):
                self.result(socket, "live", "complete")
            self.assertEqual(self.command(control, "{ subscribers }")["data"]["subscribers"], 0)

    def test_shutdown_wakes_idle_broadcast_sources(self):
        browser = self.login()
        with self.websocket("graphene", browser) as first, self.websocket("strawberry", browser) as second, self.websocket("graphene", browser) as control:
            for socket in (first, second, control):
                self.init(socket)
            for socket in (first, second):
                self.start(socket, "live", 'subscription { updates(topic: "one") }')
            self.wait_for(lambda: self.command(control, "{ subscribers }")["data"]["subscribers"] == 2)
            self.stop_server()

    def test_queries_mutations_variables_and_ordinary_subscription_iterators(self):
        browser = self.login()
        for library in ("graphene", "strawberry"):
            with self.subTest(library=library), self.websocket(library, browser) as socket:
                self.init(socket)
                self.start(socket, "who", "query { whoami }")
                self.assertEqual(self.result(socket, "who"), {"data": {"whoami": "alice"}})
                self.result(socket, "who", "complete")
                mutation = 'mutation { addNote(text: "hello") { count } }' if library == "graphene" else 'mutation { addNote(text: "hello") }'
                self.start(socket, "write", mutation)
                self.assertNotIn("errors", self.result(socket, "write"))
                self.result(socket, "write", "complete")
                self.start(socket, "ticks", "subscription T($n: Int!) { ticks(steps: $n) }",
                           variables={"n": 3}, operationName="T")
                for value in range(3):
                    self.assertEqual(self.result(socket, "ticks"), {"data": {"ticks": value}})
                self.result(socket, "ticks", "complete")
        self.assertEqual(browser.me()["notes"], ["hello", "hello"])

    def test_multiplexing_cancellation_and_operation_id_reuse(self):
        browser = self.login()
        for library in ("graphene", "strawberry"):
            with self.subTest(library=library), self.websocket(library, browser) as socket:
                self.init(socket)
                self.start(socket, "live", "subscription { ticks(steps: 100000) }")
                self.result(socket, "live")
                self.start(socket, "who", "{ whoami }")
                received = []
                while len(received) < 2:
                    message = json.loads(socket.recv(timeout=5))
                    if message["id"] == "who":
                        received.append(message)
                self.assertEqual([m["type"] for m in received], ["next", "complete"])
                socket.send(json.dumps({"type": "complete", "id": "live"}))
                self.start(socket, "live", "{ whoami }")
                # A source item already in flight may precede cancellation.
                while True:
                    message = json.loads(socket.recv(timeout=5))
                    if message.get("payload", {}).get("data", {}).get("whoami"):
                        break
                self.result(socket, "live", "complete")
        self.wait_for(lambda: self.marker("graphql-source-closed").exists()
                      and self.marker("graphql-source-closed").stat().st_size >= 2)

    def test_validation_errors_source_failures_and_ping(self):
        browser = self.login()
        for library in ("graphene", "strawberry"):
            with self.subTest(library=library), self.websocket(library, browser) as socket:
                self.init(socket)
                self.start(socket, "bad", "subscription { nonexistent }")
                errors = self.result(socket, "bad", "error")
                self.assertTrue(errors)
                self.start(socket, "failed", "subscription { ticks(fail: true) }")
                self.result(socket, "failed")
                errors = self.result(socket, "failed", "error")
                self.assertIn("subscription source failed", errors[0]["message"])
                socket.send(json.dumps({"type": "ping", "payload": {"x": 1}}))
                self.assertEqual(json.loads(socket.recv(timeout=5)), {"type": "pong", "payload": {"x": 1}})

    def test_resolver_errors_are_results_and_sentry_keeps_operation_user(self):
        alice, bob = self.login("alice"), self.login("bob")
        for library in ("graphene", "strawberry"):
            with self.websocket(library, alice) as first, self.websocket(library, bob) as second:
                self.init(first, "alice")
                self.init(second, "bob")
                self.start(first, "a", "{ broken }")
                self.start(second, "b", "subscription { ticks(fail: true) }")
                result = self.result(first, "a")
                self.assertEqual(result["data"], {"broken": None})
                self.assertEqual(result["errors"][0]["message"], "resolver failed")
                self.result(first, "a", "complete")
                self.result(second, "b")
                self.result(second, "b", "error")
        self.stop_server()
        events = [json.loads(line) for line in self.marker("sentry-events").read_text().splitlines()]
        for message, user in (("resolver failed", "alice"), ("subscription source failed", "bob")):
            errors = [e for e in events if e.get("exception", {}).get("values", [{}])[-1].get("value", "").startswith(message)]
            self.assertEqual(len(errors), 2)
            for error in errors:
                self.assertEqual(error["user"]["username"], user)
                self.assertEqual(error["tags"]["client-label"], user)

    def test_duplicate_operation_and_duplicate_initialization_close_connection(self):
        from websockets.exceptions import ConnectionClosed
        browser = self.login()
        for library in ("graphene", "strawberry"):
            with self.websocket(library, browser) as socket:
                self.init(socket)
                self.start(socket, "same", "subscription { ticks(steps: 100000) }")
                self.result(socket, "same")
                self.start(socket, "same", "{ whoami }")
                with self.assertRaises(ConnectionClosed) as error:
                    while True:
                        socket.recv(timeout=5)
                self.assertEqual(error.exception.rcvd.code, 4409)
            with self.websocket(library, browser) as socket:
                self.init(socket)
                socket.send(json.dumps({"type": "connection_init"}))
                with self.assertRaises(ConnectionClosed) as error:
                    socket.recv(timeout=5)
                self.assertEqual(error.exception.rcvd.code, 4429)

    def test_legacy_protocol(self):
        browser = self.login()
        for library in ("graphene", "strawberry"):
            with self.subTest(library=library), self.websocket(library, browser, protocol="graphql-ws") as socket:
                self.init(socket)
                self.start(socket, "old", "subscription { ticks(steps: 1) }", protocol="graphql-ws")
                self.assertEqual(self.result(socket, "old", "data"), {"data": {"ticks": 0}})
                self.result(socket, "old", "complete")

    def test_source_setup_errors_are_terminal_and_async_sources_are_rejected(self):
        browser = self.login()
        for library in ("graphene", "strawberry"):
            with self.subTest(library=library), self.websocket(library, browser) as socket:
                self.init(socket)
                self.start(socket, "denied", "subscription { ticks(steps: -1) }")
                errors = self.result(socket, "denied", "error")
                self.assertEqual(errors[0]["message"], "source denied")
                self.assertEqual(errors[0]["path"], ["ticks"])
                self.start(socket, "async", "subscription { asynchronous }")
                errors = self.result(socket, "async", "error")
                self.assertIn("ordinary iterator", errors[0]["message"])
                self.start(socket, "valid", "{ whoami }")
                self.assertEqual(self.result(socket, "valid")["data"], {"whoami": "alice"})
                self.result(socket, "valid", "complete")

    def test_session_users_are_isolated_and_http_runs_alongside_sockets(self):
        alice, bob = self.login("alice"), self.login("bob")
        with self.websocket("graphene", alice) as first, self.websocket("strawberry", bob) as second:
            self.init(first, "alice")
            self.init(second, "bob")
            self.start(first, "a", "{ whoami }")
            self.start(second, "b", "{ whoami }")
            self.assertEqual(self.result(first, "a")["data"]["whoami"], "alice")
            self.assertEqual(self.result(second, "b")["data"]["whoami"], "bob")
            self.result(first, "a", "complete")
            self.result(second, "b", "complete")
            self.assertEqual(alice.me()["username"], "alice")
        self.stop_server()
        events = [json.loads(line) for line in self.marker("sentry-events").read_text().splitlines()]
        transactions = [e for e in events if e.get("type") == "transaction"]
        self.assertTrue(any(any("graphql" in span["op"] for span in e["spans"]) for e in transactions))

    def test_protocol_errors_authentication_and_init_timeout(self):
        from websockets.exceptions import ConnectionClosed, InvalidStatus
        browser = self.login()
        for library in ("graphene", "strawberry"):
            for message, expected in (({"type": "subscribe", "id": "x", "payload": {"query": "{ whoami }"}}, 4401),
                                      ({"type": "garbage"}, 4400)):
                with self.subTest(library=library, code=expected), self.websocket(library, browser) as socket:
                    socket.send(json.dumps(message))
                    with self.assertRaises(ConnectionClosed) as error:
                        socket.recv(timeout=5)
                    self.assertEqual(error.exception.rcvd.code, expected)
            with self.websocket(library, browser) as socket:
                with self.assertRaises(ConnectionClosed) as error:
                    socket.recv(timeout=5)
                self.assertEqual(error.exception.rcvd.code, 4408)
            with self.websocket(library) as socket:
                socket.send(json.dumps({"type": "connection_init"}))
                with self.assertRaises(ConnectionClosed) as error:
                    socket.recv(timeout=5)
                self.assertEqual(error.exception.rcvd.code, 4403)
            with self.websocket(library, browser) as socket:
                socket.send(json.dumps({"type": "connection_init", "payload": {"fail_connect": True}}))
                with self.assertRaises(ConnectionClosed) as error:
                    socket.recv(timeout=5)
                self.assertEqual(error.exception.rcvd.code, 4403)
            with self.assertRaises(InvalidStatus):
                self.websocket(library, browser, origin="http://untrusted.test")
        self.stop_server()
        events = [json.loads(line) for line in self.marker("sentry-events").read_text().splitlines()]
        self.assertEqual(len([e for e in events if e.get("exception", {}).get("values", [{}])[-1].get("value") == "initialization failed"]), 2)

    def test_shutdown_cancels_sources_and_joins_operation_threads(self):
        browser = self.login()
        with self.websocket("graphene", browser) as first, self.websocket("strawberry", browser) as second:
            for socket in (first, second):
                self.init(socket)
                self.start(socket, "long", "subscription { ticks(steps: 100000) }")
                self.result(socket, "long")
            self.stop_server()
        self.assertEqual(self.marker("graphql-source-closed").read_text(), "xx")


@unittest.skipUnless(AVAILABLE, "requires graphene/strawberry extras and test group")
class GraphQLBridgeTests(unittest.TestCase):
    def test_backpressure_and_send_failure(self):
        result = subprocess.run(
            [sys.executable, str(Path(__file__).parent / "fixtures" / "graphql_semantics.py")],
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
