"""Reusable native threads with explicit failure at the capacity ceiling."""

from concurrent.futures import Executor, ThreadPoolExecutor
from threading import BoundedSemaphore, Event


DEFAULT_MAX_WORKERS = 65_536


class ThreadCapacityError(RuntimeError):
    """The worker cannot admit another invocation at its thread ceiling."""


class GrowingThreadExecutor(Executor):
    """Reuse idle threads and grow on demand, rejecting rather than queuing.

    The high ceiling is a last-resort guard, not a deployment capacity target.
    Lifespan and other invocations share it. Idle threads remain until shutdown.
    """

    def __init__(self, max_workers=DEFAULT_MAX_WORKERS):
        self._pool = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="asgi-function"
        )
        self._slots = BoundedSemaphore(max_workers)
        self._max_workers = max_workers

    def submit(self, fn, /, *args, **kwargs):
        if not self._slots.acquire(blocking=False):
            raise ThreadCapacityError(
                f"ASGI function worker reached its native-thread capacity ceiling "
                f"({self._max_workers:,} outstanding invocations, including lifespan). "
                "The invocation was rejected instead of queued."
            )

        admitted = Event()
        aborted = False

        def run():
            # ThreadPoolExecutor queues before attempting thread creation. If
            # creation fails, another thread could otherwise run this work even
            # though submit() raised and the ASGI invocation has already ended.
            admitted.wait()
            if not aborted:
                try:
                    return fn(*args, **kwargs)
                finally:
                    self._slots.release()

        try:
            future = self._pool.submit(run)
        except BaseException:
            aborted = True
            self._slots.release()
            admitted.set()
            raise

        def release_cancelled(pending):
            if pending.cancelled():
                self._slots.release()

        future.add_done_callback(release_cancelled)
        admitted.set()
        return future

    def shutdown(self, wait=True, *, cancel_futures=False):
        self._pool.shutdown(wait=wait, cancel_futures=cancel_futures)
