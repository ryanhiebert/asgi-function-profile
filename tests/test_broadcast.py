"""Policies and cooperative cleanup of the process-local broadcast example."""

from concurrent.futures import ThreadPoolExecutor
from threading import Event
import unittest

from examples.broadcast import Broadcast, SlowSubscriberError


class BroadcastTests(unittest.TestCase):
    def test_topics_and_independent_delivery(self):
        broker = Broadcast()
        first = broker.subscribe("one")
        second = broker.subscribe("one")
        other = broker.subscribe("two")
        broker.publish("one", 1)
        broker.publish("two", 2)
        self.assertEqual((next(first), next(second), next(other)), (1, 1, 2))
        broker.drain()
        self.assertFalse(broker.subscribers)

    def test_slow_subscriber_fails_without_stopping_fast_subscriber(self):
        broker = Broadcast(capacity=2)
        slow = broker.subscribe("one")
        fast = broker.subscribe("one")
        for value in range(3):
            broker.publish("one", value)
            self.assertEqual(next(fast), value)
        with self.assertRaisesRegex(SlowSubscriberError, "2-event limit"):
            next(slow)
        self.assertNotIn(slow, broker.subscribers)
        self.assertFalse(slow.pending)
        broker.drain()

    def test_cancel_and_drain_wake_waiting_sources(self):
        for drain in (False, True):
            with self.subTest(drain=drain):
                broker = Broadcast()
                source = broker.subscribe("one")
                entered = Event()

                def consume():
                    entered.set()
                    return next(source, "ended")

                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(consume)
                    self.assertTrue(entered.wait(1))
                    (broker.drain if drain else source.cancel)()
                    self.assertEqual(future.result(timeout=1), "ended")
                source.close()
                self.assertFalse(broker.subscribers)
                if drain:
                    with self.assertRaisesRegex(RuntimeError, "draining"):
                        broker.subscribe("one")

    def test_authorization_at_creation_and_each_event(self):
        broker = Broadcast()
        permitted = False
        with self.assertRaisesRegex(PermissionError, "denied"):
            broker.subscribe("private", authorized=lambda: permitted)
        permitted = True
        source = broker.subscribe("private", authorized=lambda: permitted)
        broker.publish("private", 1)
        self.assertEqual(next(source), 1)
        permitted = False
        broker.publish("private", 2)
        with self.assertRaisesRegex(PermissionError, "revoked"):
            next(source)
        self.assertFalse(broker.subscribers)
