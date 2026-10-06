"""Experimental gevent execution with Uvicorn on a separate native thread."""

import asyncio
import signal
from concurrent.futures import Executor, Future

import gevent
from gevent import monkey
from gevent.pool import Group
from gevent.threadpool import ThreadPoolExecutor
from gunicorn.arbiter import Arbiter
from gunicorn.workers.base import Worker
from uvicorn import Server
from uvicorn_worker import UvicornWorker

from .adapter import FunctionProfileAdapter


class _GreenletExecutor(Executor):
    """Submit from asyncio's thread; execute on the owning gevent hub."""

    def __init__(self):
        self.hub = gevent.get_hub()
        self.group = Group()
        self.closed = False

    def submit(self, fn, /, *args, **kwargs):
        if self.closed:
            raise RuntimeError("cannot submit after executor shutdown")
        future = Future()

        def run():
            if not future.set_running_or_notify_cancel():
                return
            try:
                result = fn(*args, **kwargs)
            except BaseException as error:
                future.set_exception(error)
            else:
                future.set_result(result)

        self.hub.loop.run_callback_threadsafe(self.group.spawn, run)
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
        self.closed = True
        if wait:
            self.group.join()


class GeventUvicornWorker(UvicornWorker):
    """Run ordinary applications in greenlets, never on asyncio's thread.

    Like Uvicorn's coroutine worker, this adds no application concurrency cap.
    Gunicorn's ``--threads`` and ``--worker-connections`` are unused.
    """

    CONFIG_KWARGS = {"loop": "asyncio", "http": "h11", "interface": "asgi3"}

    def init_process(self):
        if self.cfg.preload_app:
            raise RuntimeError(
                "GeventUvicornWorker requires application loading after patching; omit --preload"
            )
        monkey.patch_all()
        gevent.reinit()
        super().init_process()

    def init_signals(self):
        Worker.init_signals(self)
        signal.signal(signal.SIGINT, self.handle_exit)
        signal.signal(signal.SIGQUIT, self.handle_exit)

    def handle_exit(self, sig, frame):
        self.alive = False
        if hasattr(self, "server"):
            self.server.should_exit = True

    async def _serve(self):
        adapter = FunctionProfileAdapter(self.wsgi, executor=self.applications)
        lifespan_task = None

        async def application(scope, receive, send):
            nonlocal lifespan_task
            if scope["type"] == "lifespan":
                lifespan_task = asyncio.current_task()
            await adapter(scope, receive, send)

        self.config.app = application
        self.server = Server(self.config)
        self.server.should_exit = not self.alive
        await self.server.serve(sockets=self.sockets)
        # Uvicorn waits for shutdown.complete, not for the lifespan callable
        # to return. Let its greenlet finish before closing the asyncio loop.
        if lifespan_task is not None:
            await lifespan_task
        if not self.server.started:
            raise SystemExit(Arbiter.WORKER_BOOT_ERROR)

    def run(self):
        with _GreenletExecutor() as self.applications:
            # This executor deliberately retains real threads after patch_all.
            with ThreadPoolExecutor(max_workers=1) as server_thread:
                server_thread.submit(super().run).result()
