"""Requires an explicit disposable Redis endpoint; never uses production Redis."""

from concurrent.futures import ThreadPoolExecutor
import importlib.util
import os
import subprocess
import sys
from threading import Event
import unittest
from uuid import uuid4

import test_graphql_websocket


URL = os.environ.get("ASGI_FUNCTION_REDIS_URL")
AVAILABLE = bool(URL) and importlib.util.find_spec("redis") is not None


@unittest.skipUnless(AVAILABLE, "set ASGI_FUNCTION_REDIS_URL to a disposable Redis server")
class RedisBroadcastTests(unittest.TestCase):
    def setUp(self):
        import redis
        from redis.backoff import NoBackoff
        from redis.retry import Retry
        from examples.redis_broadcast import RedisBroadcast

        self.client_name = "function-test:" + uuid4().hex
        self.client = redis.Redis.from_url(URL, client_name=self.client_name, socket_connect_timeout=1,
                                          socket_timeout=1, retry=Retry(NoBackoff(), 0))
        self.addCleanup(self.client.close)
        self.broker = RedisBroadcast(self.client, prefix="function-test:" + uuid4().hex)

    def test_other_process_publishes_and_namespaces_isolate(self):
        source = self.broker.subscribe("one")
        self.addCleanup(source.close)
        script = """
import json, sys, redis
client = redis.Redis.from_url(sys.argv[1])
assert client.publish(sys.argv[2], json.dumps({"value": 7})) == 1
client.close()
"""
        self.assertEqual(self.client.publish("other:" + self.broker.channel("one"), "8"), 0)
        result = subprocess.run([sys.executable, "-c", script, URL, self.broker.channel("one")],
                                capture_output=True, text=True, timeout=5)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(next(source), {"value": 7})
        source.close()
        self.assertEqual(self.client.pubsub_numsub(self.broker.channel("one"))[0][1], 0)

    def test_two_independent_processes_receive_the_same_publication(self):
        script = """
import json, sys, redis
from examples.redis_broadcast import RedisBroadcast
client = redis.Redis.from_url(sys.argv[1], socket_connect_timeout=1, socket_timeout=1)
broker = RedisBroadcast(client, prefix=sys.argv[2])
source = broker.subscribe("one")
print("ready", flush=True)
try:
    print(json.dumps(next(source)), flush=True)
finally:
    source.close()
    client.close()
"""
        children = []
        try:
            for _ in range(2):
                child = subprocess.Popen([sys.executable, "-c", script, URL, self.broker.prefix],
                                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                children.append(child)
            from deployment_support import wait
            wait(lambda: self.client.pubsub_numsub(self.broker.channel("one"))[0][1] == 2)
            self.assertEqual(self.broker.publish("one", {"value": 9}), 2)
            for child in children:
                output, errors = child.communicate(timeout=5)
                self.assertEqual(child.returncode, 0, errors)
                self.assertEqual(output.splitlines(), ["ready", '{"value": 9}'])
            self.assertEqual(self.client.pubsub_numsub(self.broker.channel("one"))[0][1], 0)
        finally:
            for child in children:
                if child.poll() is None:
                    child.kill()
                child.communicate()

    def test_cancel_and_drain_keep_pubsub_on_its_owner_thread(self):
        for drain in (False, True):
            # Each broker has independent drain state.
            from examples.redis_broadcast import RedisBroadcast
            broker = RedisBroadcast(self.client, prefix=self.broker.prefix + str(drain))
            ready = Event()
            holder = []

            def consume():
                source = broker.subscribe("one")
                holder.append(source)
                ready.set()
                try:
                    return next(source, "ended")
                finally:
                    source.close()

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(consume)
                self.assertTrue(ready.wait(3))
                (broker.drain if drain else holder[0].cancel)()
                self.assertEqual(future.result(timeout=1), "ended")
            self.assertFalse(broker.subscribers)
            self.assertEqual(self.client.pubsub_numsub(broker.channel("one"))[0][1], 0)

    def test_event_size_limits_and_malformed_events_close_source(self):
        for value in (b"{invalid", b"x" * 65537):
            with self.subTest(value_size=len(value)):
                source = self.broker.subscribe("one")
                self.client.publish(self.broker.channel("one"), value)
                with self.assertRaises(ValueError):
                    next(source)
                self.assertFalse(self.broker.subscribers)
        with self.assertRaisesRegex(ValueError, "byte limit"):
            self.broker.publish("one", "x" * 65537)

    def test_server_disconnect_is_reported_and_cleans_up(self):
        import redis
        source = self.broker.subscribe("one")
        self.addCleanup(source.close)
        # Redis terminates the actual subscribed TCP connection. No fake PubSub.
        client_id = next(row["id"] for row in self.client.client_list(_type="pubsub")
                         if row["name"] == self.client_name)
        self.client.client_kill_filter(_id=client_id)
        with self.assertRaises(redis.ConnectionError):
            next(source)
        self.assertFalse(self.broker.subscribers)


@unittest.skipUnless(AVAILABLE and test_graphql_websocket.AVAILABLE,
                     "requires disposable Redis, GraphQL extras, and test group")
class RedisGraphQLTests(test_graphql_websocket.GraphQLWebSocketTests):
    def server_command(self):
        return super().server_command() + ["--env", "ASGI_FUNCTION_BROADCAST=redis"]
