from __future__ import annotations

import importlib.util
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest

from pathlib import Path


UVICORN_AVAILABLE = importlib.util.find_spec("uvicorn") is not None
FIXTURES = Path(__file__).parent / "fixtures"


def unused_tcp_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


class UvicornTestCase(unittest.TestCase):
    """Shared subprocess harness; subclasses supply an application target."""

    application_target = "server_app:application"
    server: subprocess.Popen[str]

    def setUp(self) -> None:
        self.state = tempfile.TemporaryDirectory()
        self.addCleanup(self.state.cleanup)
        self.state_path = Path(self.state.name)
        self.port = unused_tcp_port()
        environment = os.environ.copy()
        environment["ASGI_FUNCTION_PROFILE_TEST_STATE"] = self.state.name
        self.server = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "asgi_function_profile",
                self.application_target,
                "--app-dir",
                str(FIXTURES),
                "--port",
                str(self.port),
                "--log-level",
                "critical",
            ],
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        self.addCleanup(self.cleanup_server)
        self.wait_for_file("lifespan-started")
        self.wait_for_server()

    def cleanup_server(self) -> None:
        # Release a gated producer even if its assertion failed.
        self.marker("stream-release").touch()
        if self.server.poll() is None:
            self.stop_server()
        else:
            self.server.communicate()

    def marker(self, name: str) -> Path:
        return self.state_path / name

    def wait_for(self, condition, *, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if condition():
                return
            if self.server.poll() is not None:
                output = self.server.communicate()[0]
                self.fail(f"server exited early ({self.server.returncode}):\n{output}")
            time.sleep(0.01)
        self.fail("timed out waiting for the server")

    def wait_for_file(self, name: str, *, timeout: float = 5.0) -> None:
        self.wait_for(lambda: self.marker(name).exists(), timeout=timeout)

    def wait_for_server(self) -> None:
        def accepts_connections() -> bool:
            try:
                with socket.create_connection(("127.0.0.1", self.port), 0.1):
                    return True
            except OSError:
                return False

        self.wait_for(accepts_connections)

    def connect(self, *, receive_buffer: int | None = None) -> socket.socket:
        connection = socket.socket()
        self.addCleanup(connection.close)
        if receive_buffer is not None:
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, receive_buffer)
        connection.settimeout(5)
        connection.connect(("127.0.0.1", self.port))
        return connection

    def stop_server(self) -> str:
        self.server.send_signal(signal.SIGINT)
        try:
            output = self.server.communicate(timeout=5)[0]
        except subprocess.TimeoutExpired:
            self.server.kill()
            output = self.server.communicate()[0]
            self.fail(f"server did not shut down cleanly:\n{output}")

        self.assertEqual(self.server.returncode, 0, output)
        self.assertTrue(
            self.marker("lifespan-stopped").exists(),
            f"lifespan shutdown did not run:\n{output}",
        )
        return output


@unittest.skipUnless(UVICORN_AVAILABLE, "requires the server extra")
class UvicornIntegrationTests(UvicornTestCase):
    def test_streaming_reaches_the_client_before_the_application_returns(self) -> None:
        with self.connect() as connection:
            connection.sendall(
                b"POST /stream HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Content-Length: 5\r\n"
                b"Connection: close\r\n\r\n"
                b"hello"
            )
            self.wait_for_file("stream-first-sent")

            response = b""
            while b"3\r\nhel\r\n" not in response:
                data = connection.recv(4096)
                self.assertTrue(data, "connection closed before the first chunk")
                response += data

            self.assertIn(b"x-lifespan-state: ready", response.lower())
            self.assertFalse(self.marker("stream-finished").exists())
            self.marker("stream-release").touch()

            while True:
                data = connection.recv(4096)
                if not data:
                    break
                response += data

        self.wait_for_file("stream-finished")
        self.assertIn(b"2\r\nlo\r\n0\r\n\r\n", response)

    def test_client_disconnect_releases_receive(self) -> None:
        connection = self.connect()
        connection.sendall(
            b"POST /disconnect HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Content-Length: 1000000\r\n\r\n"
            b"partial"
        )
        self.wait_for_file("disconnect-request-received")
        connection.close()

        self.wait_for_file("disconnect-observed")

    def test_slow_client_applies_send_backpressure(self) -> None:
        connection = self.connect(receive_buffer=4096)
        connection.sendall(
            b"GET /backpressure HTTP/1.1\r\n"
            b"Host: localhost\r\n"
            b"Connection: close\r\n\r\n"
        )
        self.wait_for_file("backpressure-started")
        progress = self.marker("backpressure-progress")
        self.wait_for(lambda: progress.exists() and progress.stat().st_size > 0)

        stable_samples = 0
        previous_size = -1
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline and stable_samples < 3:
            size = progress.stat().st_size
            if size == previous_size:
                stable_samples += 1
            else:
                stable_samples = 0
                previous_size = size
            time.sleep(0.1)

        self.assertEqual(stable_samples, 3, "response producer never stalled")
        self.assertLess(previous_size, 1024)
        self.assertFalse(self.marker("backpressure-completed").exists())

        connection.close()
        self.wait_for_file("backpressure-released")

    def test_clean_shutdown_runs_lifespan_shutdown(self) -> None:
        with self.connect() as connection:
            connection.sendall(
                b"GET /health HTTP/1.1\r\n"
                b"Host: localhost\r\n"
                b"Connection: close\r\n\r\n"
            )
            response = b""
            while True:
                data = connection.recv(4096)
                if not data:
                    break
                response += data

        self.assertTrue(response.endswith(b"ready"))
        self.stop_server()


if __name__ == "__main__":
    unittest.main()
