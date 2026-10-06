"""Gunicorn integration for ordinary function-profile applications."""

import signal
from concurrent.futures import ThreadPoolExecutor

from uvicorn_worker import UvicornWorker

from .adapter import FunctionProfileAdapter


class ThreadWorker(UvicornWorker):
    """Run function applications in native threads behind Uvicorn.

    Gunicorn's ``--threads`` sizes the application pool in each worker process.
    Lifespan runs in a separate thread, outside that pool.
    """

    CONFIG_KWARGS = {**UvicornWorker.CONFIG_KWARGS, "interface": "asgi3"}

    def run(self) -> None:
        # Create pools after forking, and join them before the worker exits.
        with (
            ThreadPoolExecutor(max_workers=self.cfg.threads) as requests,
            ThreadPoolExecutor(max_workers=1) as lifespan,
        ):
            application = FunctionProfileAdapter(self.wsgi, executor=requests)
            lifecycle = FunctionProfileAdapter(self.wsgi, executor=lifespan)

            async def dispatch(scope, receive, send):
                adapter = lifecycle if scope["type"] == "lifespan" else application
                await adapter(scope, receive, send)

            self.wsgi = dispatch
            super().run()

    def init_signals(self) -> None:
        super().init_signals()
        # Uvicorn replays SIGTERM after serving. Retain Gunicorn's handler so
        # that replay doesn't kill the process before executor/worker cleanup.
        signal.signal(signal.SIGTERM, self.handle_exit)
