"""Record process and thread cleanup using Gunicorn's normal worker hooks."""

import json
import os
from pathlib import Path

import psutil

control_socket = str(Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"]) / "control.sock")


def post_worker_init(worker):
    state = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
    (state / f"worker-started-{worker.pid}").touch()


def worker_exit(server, worker):
    state = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
    # Patched threading.enumerate() also reports greenlets and stale DummyThread
    # entries. Measure actual native threads for both worker implementations.
    remaining_threads = psutil.Process().num_threads()
    (state / f"worker-stopped-{worker.pid}").write_text(json.dumps(remaining_threads))
