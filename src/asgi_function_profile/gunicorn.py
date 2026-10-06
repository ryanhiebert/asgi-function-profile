"""Gunicorn integration for ordinary function-profile applications."""

import signal

from uvicorn_worker import UvicornWorker

from .adapter import FunctionProfileAdapter
from .threads import GrowingThreadExecutor


class ThreadedUvicornWorker(UvicornWorker):
    """Run function applications in native threads behind Uvicorn.

    Threads are reused and grow on demand, with a high fail-fast ceiling.
    Gunicorn's ``--threads`` and ``--worker-connections`` are unused.
    """

    CONFIG_KWARGS = {**UvicornWorker.CONFIG_KWARGS, "interface": "asgi3"}

    def run(self) -> None:
        # Create the executor after forking and join it before worker exit.
        with GrowingThreadExecutor() as executor:
            self.wsgi = FunctionProfileAdapter(self.wsgi, executor=executor)
            super().run()

    def init_signals(self) -> None:
        super().init_signals()
        # Uvicorn replays SIGTERM after serving. Retain Gunicorn's handler so
        # that replay doesn't kill the process before executor/worker cleanup.
        signal.signal(signal.SIGTERM, self.handle_exit)
