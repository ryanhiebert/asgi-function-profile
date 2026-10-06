"""Characterize scheduling limits without changing the reference adapter."""

import importlib.util
import json
import socket
import sys
import unittest

import test_server


DEPENDENCIES_AVAILABLE = all(importlib.util.find_spec(name) is not None
                             for name in ("django", "uvicorn", "websockets"))


@unittest.skipUnless(DEPENDENCIES_AVAILABLE, "requires django/server extras and test group")
class CapacityTests(test_server.UvicornTestCase):
    def server_command(self):
        return [sys.executable, str(test_server.FIXTURES / "capacity_server.py"),
                str(self.port)]

    def events(self):
        path = self.marker("capacity-events")
        if not path.exists():
            return []
        # The child may still be appending its last line when it is read.
        return [json.loads(line) for line in path.read_text().splitlines(keepends=True)
                if line.endswith("\n")]

    def has_event(self, scope, event):
        return any(row["scope"] == scope and row["event"] == event
                   for row in self.events())

    def wait_for_event(self, scope, event):
        self.wait_for(lambda: self.has_event(scope, event))

    def websocket(self, label):
        from websockets.sync.client import connect

        return self.enterContext(connect(
            f"ws://127.0.0.1:{self.port}/ws/?{label}",
            proxy=None, open_timeout=5, close_timeout=2,
        ))

    def pending_http(self, label):
        connection = self.connect()
        connection.sendall(
            f"GET /?{label} HTTP/1.1\r\nHost: localhost\r\n"
            "Connection: close\r\n\r\n".encode()
        )
        self.wait_for_event(label, "arrived")
        return connection

    def assert_queued(self, connection, label):
        # Arrival is observed at the adapter boundary, so this isn't a request
        # merely delayed by TCP. All three workers are known to be occupied.
        self.assertFalse(self.has_event(label, "started"))
        connection.settimeout(0.2)
        with self.assertRaises(TimeoutError):
            connection.recv(4096)
        self.assertFalse(self.has_event(label, "started"))
        connection.settimeout(5)

    def read_response(self, connection):
        response = b""
        while data := connection.recv(4096):
            response += data
        return response

    def assert_workers_released(self):
        self.assertTrue(self.marker("executor-stopped").exists())
        events = self.events()
        started = [row["scope"] for row in events if row["event"] == "started"]
        finished = [row["scope"] for row in events if row["event"] == "finished"]
        arrived = [row["scope"] for row in events if row["event"] == "arrived"]
        returned = [row["scope"] for row in events if row["event"] == "returned"]
        self.assertCountEqual(started, finished)
        self.assertCountEqual(arrived, returned)
        self.assertEqual(events[-1]["active"], 0)
        self.assertEqual(max(row["active"] for row in events), 3)

    def test_open_sockets_starve_http_until_a_worker_is_released(self):
        first = self.websocket("socket-one")
        # Lifespan + one socket leaves one worker for ordinary Django HTTP.
        baseline = self.pending_http("baseline")
        self.assertIn(b"Hello, world!", self.read_response(baseline))
        self.wait_for_event("baseline", "returned")

        second = self.websocket("socket-two")
        pending = self.pending_http("queued-http")
        self.assert_queued(pending, "queued-http")
        self.assertEqual(self.events()[-1]["active"], 3)

        # The event loop and existing applications still make progress.
        second.send("still alive")
        self.assertEqual(second.recv(timeout=5), "still alive")
        self.assertFalse(self.has_event("queued-http", "started"))

        first.close()
        response = self.read_response(pending)
        self.assertIn(b"200 OK", response)
        self.assertIn(b"Hello, world!", response)
        self.wait_for_event("queued-http", "returned")
        events = self.events()
        release = next(i for i, row in enumerate(events)
                       if row["scope"] == "socket-one" and row["event"] == "finished")
        started = next(i for i, row in enumerate(events)
                       if row["scope"] == "queued-http" and row["event"] == "started")
        self.assertLess(release, started)
        second.close()
        self.stop_server()
        self.assert_workers_released()

    def test_shutdown_releases_open_sockets_and_queued_http(self):
        from websockets.exceptions import ConnectionClosed

        sockets = [self.websocket("socket-one"), self.websocket("socket-two")]
        pending = self.pending_http("queued-at-shutdown")
        self.assert_queued(pending, "queued-at-shutdown")
        output = self.stop_server()
        self.assertNotIn("timeout graceful shutdown exceeded", output)
        self.assertNotIn("Exception in ASGI application", output)
        for connection in sockets:
            with self.assertRaises(ConnectionClosed) as error:
                connection.recv(timeout=5)
            self.assertEqual(error.exception.rcvd.code, 1012)
        self.assert_workers_released()

    def test_disconnected_queued_http_does_not_leak_a_worker(self):
        first = self.websocket("socket-one")
        second = self.websocket("socket-two")
        abandoned = self.pending_http("abandoned")
        self.assert_queued(abandoned, "abandoned")
        abandoned.shutdown(socket.SHUT_RDWR)
        abandoned.close()
        # The adapter has no admission/disconnect monitor for a queued scope.
        self.assertFalse(self.has_event("abandoned", "started"))
        first.close()
        self.wait_for_event("abandoned", "returned")
        self.assertTrue(self.has_event("abandoned", "started"))
        recovered = self.pending_http("recovered")
        self.assertIn(b"Hello, world!", self.read_response(recovered))
        second.close()
        self.stop_server()
        self.assert_workers_released()
