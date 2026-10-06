"""Record process and thread cleanup using Gunicorn's normal worker hooks."""

import json
import os
import threading
from pathlib import Path

control_socket = str(Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"]) / "control.sock")


def post_worker_init(worker):
    state = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
    (state / f"worker-started-{worker.pid}").touch()


def worker_exit(server, worker):
    state = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
    remaining_threads = [thread.name for thread in threading.enumerate()
                         if thread is not threading.main_thread()]
    (state / f"worker-stopped-{worker.pid}").write_text(json.dumps(remaining_threads))
