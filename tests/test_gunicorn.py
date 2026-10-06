"""Run the existing Django coverage through the public Gunicorn worker entry."""

import importlib.util
import http.client
import json
import os
import signal
import sys
import unittest

import test_capacity
import test_django_auth_socket
import test_django_server
import test_server


GUNICORN_AVAILABLE = os.name == "posix" and all(
    importlib.util.find_spec(name) is not None
    for name in ("gunicorn", "uvicorn_worker", "psutil")
)


class GunicornCommand:
    shutdown_signal = signal.SIGTERM
    worker_processes = 1
    application_threads = 2
    worker_class = "asgi_function_profile.gunicorn.ThreadedUvicornWorker"
    preload = False

    def server_command(self):
        command = [
            sys.executable, "-m", "gunicorn", self.application_target,
            "--worker-class", self.worker_class,
            "--bind", f"127.0.0.1:{self.port}", "--workers", str(self.worker_processes),
            "--pythonpath", str(test_server.FIXTURES),
            "--config", str(test_server.FIXTURES / "gunicorn_config.py"),
            "--graceful-timeout", "3", "--log-level", "info",
        ]
        if self.application_threads is not None:
            command.extend(["--threads", str(self.application_threads)])
        if self.preload:
            command.append("--preload")
        return command

    def stop_server(self):
        output = super().stop_server()
        started = list(self.state_path.glob("worker-started-*"))
        self.assertTrue(started, output)
        for marker in started:
            pid = marker.name.removeprefix("worker-started-")
            stopped = self.marker("worker-stopped-" + pid)
            self.assertTrue(stopped.exists(), output)
            self.assertEqual(json.loads(stopped.read_text()), 1, output)
        self.assertNotIn("SIGKILL", output)
        self.assertNotIn("WORKER TIMEOUT", output)
        return output


@unittest.skipUnless(
    GUNICORN_AVAILABLE and test_django_server.DJANGO_AVAILABLE,
    "requires gunicorn/django extras, test group, and a POSIX host",
)
class GunicornDjangoTests(GunicornCommand, test_django_server.DjangoServerTests):
    pass


@unittest.skipUnless(
    GUNICORN_AVAILABLE and test_django_auth_socket.DEPENDENCIES_AVAILABLE,
    "requires gunicorn/django extras, test group, and a POSIX host",
)
class GunicornAuthTests(GunicornCommand, test_django_auth_socket.AuthenticatedSocketTests):
    def test_graceful_shutdown_with_authenticated_socket_open(self):
        from websockets.exceptions import ConnectionClosed

        browser = test_django_auth_socket.Browser(self.port)
        browser.login()
        with self.websocket(browser) as socket:
            socket.send("saved before shutdown")
            self.assertEqual(json.loads(socket.recv(timeout=5))["username"], "alice")
            self.assertEqual(browser.me()["notes"], ["saved before shutdown"])
            self.stop_server()
            with self.assertRaises(ConnectionClosed) as error:
                socket.recv(timeout=5)
            self.assertEqual(error.exception.rcvd.code, 1012)
        self.assertTrue(self.marker("websocket-finished").exists())


@unittest.skipUnless(GUNICORN_AVAILABLE, "requires gunicorn extra and a POSIX host")
class GunicornProcessTests(GunicornCommand, test_server.UvicornTestCase):
    worker_processes = 2
    preload = True

    def test_two_workers_start_and_exit_cleanly(self):
        self.wait_for(lambda: len(list(self.state_path.glob("worker-started-*"))) == 2)
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.request("GET", "/health")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertEqual(response.read(), b"ready")
        finally:
            connection.close()
        self.stop_server()
        self.assertEqual(len(list(self.state_path.glob("worker-stopped-*"))), 2)


@unittest.skipUnless(
    GUNICORN_AVAILABLE and test_capacity.DEPENDENCIES_AVAILABLE,
    "requires gunicorn/django extras, test group, and a POSIX host",
)
class GunicornDefaultCapacityTests(GunicornCommand, test_server.UvicornTestCase):
    application_target = "capacity_server:measured_application"
    application_threads = None  # Exercise Gunicorn's default: one thread.

    def stop_server(self):
        output = super().stop_server()
        events = [json.loads(line) for line in
                  self.marker("capacity-events").read_text().splitlines()]
        self.assertEqual(
            max(row["active"] for row in events),
            (self.application_threads or 1) + 1,
        )
        self.assertEqual(events[-1]["active"], 0)
        self.assertCountEqual(
            [row["scope"] for row in events if row["event"] == "started"],
            [row["scope"] for row in events if row["event"] == "finished"],
        )
        return output

    def websocket(self):
        from websockets.sync.client import connect

        return self.enterContext(connect(
            f"ws://127.0.0.1:{self.port}/ws/", proxy=None,
            open_timeout=5, close_timeout=2,
        ))

    def assert_http_works(self):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.request("GET", "/")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            self.assertIn(b"Hello, world!", response.read())
        finally:
            connection.close()

    def fill_pool(self):
        sockets = []
        for _ in range(self.application_threads or 1):
            # Every configured slot is usable despite the active lifespan.
            self.assert_http_works()
            sockets.append(self.websocket())
        pending = self.connect()
        pending.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        pending.settimeout(0.2)
        with self.assertRaises(TimeoutError):
            pending.recv(4096)
        pending.settimeout(5)
        for connection in sockets:
            connection.send("still responsive")
            self.assertEqual(connection.recv(timeout=5), "still responsive")
        return sockets, pending

    def test_threads_setting_bounds_requests_but_not_lifespan(self):
        sockets, pending = self.fill_pool()
        sockets[0].close()
        response = b""
        while data := pending.recv(4096):
            response += data
        self.assertIn(b"200 OK", response)
        self.assertIn(b"Hello, world!", response)

    def test_shutdown_joins_both_pools_when_request_pool_is_full(self):
        from websockets.exceptions import ConnectionClosed

        sockets, _ = self.fill_pool()
        self.stop_server()
        for connection in sockets:
            with self.assertRaises(ConnectionClosed) as error:
                connection.recv(timeout=5)
            self.assertEqual(error.exception.rcvd.code, 1012)


class GunicornConfiguredCapacityTests(GunicornDefaultCapacityTests):
    application_threads = 3
