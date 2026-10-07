"""Process-local broadcast example, not a distributed broker or profile API.

Publish immutable values. Each subscriber has a bounded queue; overflow fails
that subscriber rather than blocking publishers or silently losing events.
"""

from collections import deque
from threading import Condition


class SlowSubscriberError(RuntimeError):
    pass


class Broadcast:
    def __init__(self, capacity=64):
        if capacity < 1:
            raise ValueError("capacity must be positive")
        self.capacity = capacity
        self.condition = Condition()
        self.subscribers = set()
        self.stopped = False

    def subscribe(self, topic, *, authorized=lambda: True):
        # Application checks may access a database; never hold the broker lock.
        if not authorized():
            raise PermissionError("subscription access denied")
        subscriber = Subscription(self, topic, authorized)
        with self.condition:
            if self.stopped:
                raise RuntimeError("broadcast is draining")
            self.subscribers.add(subscriber)
        return subscriber

    def publish(self, topic, value):
        with self.condition:
            if self.stopped:
                raise RuntimeError("broadcast is draining")
            for subscriber in tuple(self.subscribers):
                if subscriber.topic != topic:
                    continue
                if len(subscriber.pending) == self.capacity:
                    subscriber.error = SlowSubscriberError(
                        f"subscription queue reached its {self.capacity}-event limit"
                    )
                    subscriber.pending.clear()
                    self.subscribers.remove(subscriber)
                else:
                    subscriber.pending.append(value)
            self.condition.notify_all()

    def drain(self):
        with self.condition:
            self.stopped = True
            for subscriber in tuple(self.subscribers):
                subscriber.cancel()


class Subscription:
    def __init__(self, broker, topic, authorized):
        self.broker = broker
        self.topic = topic
        self.authorized = authorized
        self.pending = deque()
        self.cancelled = False
        self.error = None

    def __iter__(self):
        return self

    def __next__(self):
        with self.broker.condition:
            self.broker.condition.wait_for(
                lambda: self.cancelled or self.error is not None or self.pending
            )
            if self.cancelled:
                raise StopIteration
            if self.error is not None:
                raise self.error
            value = self.pending.popleft()
        try:
            if not self.authorized():
                raise PermissionError("subscription access revoked")
        except BaseException:
            self.close()
            raise
        return value

    def cancel(self):
        # Safe from the connection's receive thread while next() is waiting.
        with self.broker.condition:
            self.cancelled = True
            self.pending.clear()
            self.broker.subscribers.discard(self)
            self.broker.condition.notify_all()

    close = cancel
