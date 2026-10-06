"""Lifecycle and resource-failure behavior of invocation threads."""

from threading import Event, get_ident
from unittest import TestCase
from unittest.mock import patch

from asgi_function_profile.threads import GrowingThreadExecutor, ThreadCapacityError


class ThreadExecutorTests(TestCase):
    def test_waiting_invocation_does_not_hold_up_new_work(self):
        release = Event()
        executor = GrowingThreadExecutor()
        try:
            waiting = executor.submit(release.wait)
            self.assertEqual(executor.submit(lambda: 42).result(timeout=2), 42)
        finally:
            release.set()
            executor.shutdown()
        self.assertTrue(waiting.done())
        self.assertTrue(all(not thread.is_alive() for thread in executor._pool._threads))
        with self.assertRaises(RuntimeError):
            executor.submit(lambda: None)

    def test_exception_propagates_without_poisoning_executor(self):
        def fail():
            raise ValueError("application failed")

        with GrowingThreadExecutor() as executor:
            with self.assertRaisesRegex(ValueError, "application failed"):
                executor.submit(fail).result(timeout=2)
            self.assertEqual(executor.submit(lambda: 42).result(timeout=2), 42)

    def test_thread_creation_failure_propagates_and_cleanup_still_works(self):
        failed_invocation = Event()
        with GrowingThreadExecutor() as executor:
            with patch("threading.Thread.start",
                       side_effect=RuntimeError("can't start new thread")):
                with self.assertRaisesRegex(RuntimeError, "can't start new thread"):
                    executor.submit(failed_invocation.set)
            self.assertEqual(executor.submit(lambda: 42).result(timeout=2), 42)
        self.assertFalse(failed_invocation.is_set())

    def test_shutdown_can_wait_after_initial_nonwaiting_shutdown(self):
        release = Event()
        started = Event()

        def run():
            started.set()
            release.wait()

        executor = GrowingThreadExecutor()
        try:
            future = executor.submit(run)
            self.assertTrue(started.wait(timeout=2))
            executor.shutdown(wait=False, cancel_futures=True)
            self.assertFalse(future.cancelled())
        finally:
            release.set()
            executor.shutdown()
        self.assertTrue(future.done())

    def test_ceiling_rejects_without_queuing_and_capacity_is_released(self):
        release = Event()
        started = Event()
        rejected = Event()

        def hold():
            started.set()
            release.wait()

        with GrowingThreadExecutor(max_workers=1) as executor:
            try:
                waiting = executor.submit(hold)
                self.assertTrue(started.wait(timeout=2))
                with self.assertRaisesRegex(ThreadCapacityError, "1 outstanding.*rejected"):
                    executor.submit(rejected.set)
            finally:
                release.set()
            waiting.result(timeout=2)
            self.assertFalse(rejected.is_set())
            self.assertEqual(executor.submit(lambda: 42).result(timeout=2), 42)

    def test_idle_thread_is_reused(self):
        with GrowingThreadExecutor(max_workers=1) as executor:
            first = executor.submit(get_ident).result(timeout=2)
            self.assertEqual(executor.submit(get_ident).result(timeout=2), first)
