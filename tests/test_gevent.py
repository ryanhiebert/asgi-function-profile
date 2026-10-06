"""Run unchanged function-profile applications on the gevent worker."""

import importlib.util
import json
import subprocess
import sys
import unittest
from concurrent.futures import ThreadPoolExecutor

import test_django_auth_socket
import test_django_server
import test_gunicorn
import test_server


GEVENT_AVAILABLE = (test_gunicorn.GUNICORN_AVAILABLE
                    and importlib.util.find_spec("gevent") is not None)
DJANGO_AVAILABLE = GEVENT_AVAILABLE and test_django_auth_socket.DEPENDENCIES_AVAILABLE


class GeventCommand(test_gunicorn.GunicornCommand):
    worker_class = "asgi_function_profile.gevent.GeventUvicornWorker"
    application_threads = None

    def server_command(self):
        return [*super().server_command(), "--log-level", "warning"]

    def stop_server(self):
        output = super().stop_server()
        self.assertNotIn("Exception in 'lifespan' protocol", output)
        self.assertNotIn("protocol appears unsupported", output)
        return output


@unittest.skipUnless(DJANGO_AVAILABLE, "requires gevent/django extras, test group, and POSIX")
class GeventDjangoTests(GeventCommand, test_django_server.DjangoServerTests):
    pass


@unittest.skipUnless(DJANGO_AVAILABLE, "requires gevent/django extras, test group, and POSIX")
class GeventAuthTests(GeventCommand, test_gunicorn.GunicornAuthTests):
    pass


@unittest.skipUnless(GEVENT_AVAILABLE, "requires gevent extra and POSIX")
class GeventProtocolTests(GeventCommand, test_server.UvicornIntegrationTests):
    pass


@unittest.skipUnless(GEVENT_AVAILABLE, "requires gevent extra and POSIX")
class GeventProcessTests(GeventCommand, test_gunicorn.GunicornProcessTests):
    preload = False


@unittest.skipUnless(
    DJANGO_AVAILABLE,
    "requires gevent/django extras and test group",
)
class GeventConcurrencyTests(GeventCommand, test_server.UvicornTestCase):
    application_target = "django_server:application"
    websocket = test_django_auth_socket.AuthenticatedSocketTests.websocket

    def server_command(self):
        # This Gunicorn setting must not impose a greenlet/connection cap.
        return [*super().server_command(), "--worker-connections", "1"]

    def test_many_authenticated_sockets_do_not_consume_native_threads(self):
        import psutil
        from websockets.exceptions import ConnectionClosed

        browser = test_django_auth_socket.Browser(self.port)
        browser.login()
        markers = list(self.state_path.glob("worker-started-*"))
        self.assertEqual(len(markers), 1)
        process = psutil.Process(int(markers[0].name.removeprefix("worker-started-")))
        self.assertEqual(process.num_threads(), 2)
        sockets = [self.enterContext(self.websocket(browser)) for _ in range(64)]
        self.assertEqual(process.num_threads(), 2)
        for socket, text in ((sockets[0], "first"), (sockets[-1], "last")):
            socket.send(text)
            self.assertEqual(json.loads(socket.recv(timeout=5))["username"], "alice")
        self.assertEqual(browser.me()["notes"], ["first", "last"])
        self.assertEqual(process.num_threads(), 2)
        self.stop_server()
        for socket in sockets:
            with self.assertRaises(ConnectionClosed) as error:
                socket.recv(timeout=5)
            self.assertEqual(error.exception.rcvd.code, 1012)
        self.assertEqual(self.marker("websocket-finished").stat().st_size, 64)

    def test_overlapping_django_transactions_keep_local_state_isolated(self):
        alice, bob = (test_django_auth_socket.Browser(self.port) for _ in range(2))
        alice.login("alice")
        bob.login("bob")

        def request(browser, label):
            with browser.get("/isolation/?label=" + label) as response:
                return json.load(response)

        with ThreadPoolExecutor(max_workers=2) as clients:
            first = clients.submit(request, alice, "alice")
            second = clients.submit(request, bob, "bob")
            rows = [first.result(timeout=5), second.result(timeout=5)]
        self.assertEqual([row["username"] for row in rows], ["alice", "bob"])
        self.assertEqual(len({row["connection"] for row in rows}), 2)
        self.assertEqual(len({row["greenlet"] for row in rows}), 2)
        self.assertEqual(len({row["native_thread"] for row in rows}), 1)


@unittest.skipUnless(GEVENT_AVAILABLE, "requires gevent extra and POSIX")
class GeventAdapterTests(unittest.TestCase):
    def test_adapter_semantics_with_greenlets(self):
        result = subprocess.run(
            [sys.executable, str(test_server.FIXTURES / "gevent_adapter_suite.py")],
            capture_output=True, text=True, timeout=15,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Ran 8 tests", result.stderr)

    def test_preload_is_rejected_before_monkey_patching(self):
        # Reject this configuration without patching the test process itself.
        from types import SimpleNamespace
        from gevent import monkey
        from asgi_function_profile.gevent import GeventUvicornWorker

        worker = SimpleNamespace(cfg=SimpleNamespace(preload_app=True))
        self.assertFalse(monkey.is_module_patched("threading"))
        with self.assertRaisesRegex(RuntimeError, "omit --preload"):
            GeventUvicornWorker.init_process(worker)
        self.assertFalse(monkey.is_module_patched("threading"))
