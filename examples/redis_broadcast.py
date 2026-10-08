"""Native Redis Pub/Sub source example; delivery is live and at most once.

Every source owns its PubSub connection on its operation thread. cancel()
only signals an Event, so redis-py's PubSub object never crosses threads.
Configure Redis's pubsub output-buffer limits separately: this example has
no local event queue, and a bounded payload is not a bound on server buffers.
"""

import json
from threading import Event, Lock
from time import monotonic


class RedisBroadcast:
    def __init__(self, client, *, prefix, max_event_bytes=65536):
        if not prefix or max_event_bytes < 1:
            raise ValueError("configure a namespace and positive event size limit")
        self.client = client
        self.prefix = prefix
        self.max_event_bytes = max_event_bytes
        self.lock = Lock()
        self.subscribers = set()
        self.stopped = False

    def channel(self, topic):
        return self.prefix + ":" + topic

    def publish(self, topic, value):
        encoded = json.dumps(value, allow_nan=False).encode()
        if len(encoded) > self.max_event_bytes:
            raise ValueError("broadcast event exceeds its byte limit")
        with self.lock:
            if self.stopped:
                raise RuntimeError("broadcast is draining")
        # A normal Redis client can be shared by publisher threads.
        return self.client.publish(self.channel(topic), encoded)

    def subscribe(self, topic, *, authorized=lambda: True):
        if not authorized():
            raise PermissionError("subscription access denied")
        with self.lock:
            if self.stopped:
                raise RuntimeError("broadcast is draining")
        source = RedisSubscription(self, topic, authorized)
        with self.lock:
            stopped = self.stopped
            if not stopped:
                self.subscribers.add(source)
        if stopped:
            source.close()
            raise RuntimeError("broadcast is draining")
        return source

    def drain(self):
        with self.lock:
            self.stopped = True
            for source in self.subscribers:
                source.cancel()


class RedisSubscription:
    def __init__(self, broker, topic, authorized):
        self.broker = broker
        self.authorized = authorized
        self.cancelled = Event()
        self.pubsub = broker.client.pubsub()
        try:
            self.pubsub.subscribe(broker.channel(topic))
            # Do not report readiness until Redis acknowledges membership.
            deadline = monotonic() + 2
            while monotonic() < deadline:
                message = self.pubsub.get_message(timeout=0.1)
                if message and message["type"] == "subscribe":
                    break
            else:
                raise TimeoutError("Redis did not acknowledge the subscription")
        except BaseException:
            self.pubsub.close()
            raise

    def __iter__(self):
        return self

    def __next__(self):
        try:
            while not self.cancelled.is_set():
                message = self.pubsub.get_message(timeout=0.1)
                if not message or message["type"] != "message":
                    continue
                if self.cancelled.is_set():
                    break
                if len(message["data"]) > self.broker.max_event_bytes:
                    raise ValueError("broadcast event exceeds its byte limit")
                if not self.authorized():
                    raise PermissionError("subscription access revoked")
                return json.loads(message["data"])
        except BaseException:
            self.close()
            raise
        self.close()
        raise StopIteration

    def cancel(self):
        self.cancelled.set()

    def close(self):
        # Only the source-owning operation thread calls close().
        self.cancel()
        self.pubsub.close()
        with self.broker.lock:
            self.broker.subscribers.discard(self)
