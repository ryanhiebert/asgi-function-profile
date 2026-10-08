"""Process ownership for deployment experiments, kept outside the library."""

import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

from test_server import FIXTURES, unused_tcp_port


class NativeServer:
    def __init__(self, *, worker="deployment_server.PingWorker", target="deployment_server:application", env=None):
        self.state = tempfile.TemporaryDirectory()
        self.path = Path(self.state.name)
        self.port = unused_tcp_port()
        environment = {**os.environ, **(env or {}), "ASGI_FUNCTION_PROFILE_TEST_STATE": self.state.name}
        self.process = subprocess.Popen([
            sys.executable, "-m", "gunicorn", target, "--worker-class", worker,
            "--workers", "1", "--bind", f"127.0.0.1:{self.port}",
            "--pythonpath", str(FIXTURES), "--config", str(FIXTURES / "gunicorn_config.py"),
            "--graceful-timeout", "3", "--log-level", "warning",
        ], env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
           text=True, start_new_session=True)
        try:
            wait(lambda: (self.path / "lifespan-started").exists(), alive=self.process)
            wait(lambda: accepts(self.port), alive=self.process)
        except BaseException:
            self.close()
            raise

    def stop(self):
        if self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)
        output = self.process.communicate(timeout=5)[0]
        if self.process.returncode != 0 or not (self.path / "lifespan-stopped").exists():
            raise AssertionError("server did not stop cleanly:\n" + output)
        for started in self.path.glob("worker-started-*"):
            stopped = self.path / started.name.replace("started", "stopped")
            if not stopped.exists() or json.loads(stopped.read_text()) != 1:
                raise AssertionError("application threads did not join:\n" + output)
        return output

    def close(self):
        try:
            if self.process.poll() is None:
                try:
                    self.stop()
                except BaseException:
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.communicate()
                    raise
            else:
                self.process.communicate()
        finally:
            self.state.cleanup()


def accepts(port):
    try:
        with socket.create_connection(("127.0.0.1", port), 0.1):
            return True
    except OSError:
        return False


def wait(predicate, *, alive=None, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        if alive is not None and alive.poll() is not None:
            raise AssertionError(alive.communicate()[0])
        time.sleep(0.02)
    raise AssertionError("timed out waiting for deployment experiment")
