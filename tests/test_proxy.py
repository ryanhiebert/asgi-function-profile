"""Actual nginx proxy checks, opt-in to a locally available Docker image."""

import http.client
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from uuid import uuid4

from deployment_support import NativeServer, accepts, wait
from test_server import unused_tcp_port


IMAGE = os.environ.get("ASGI_FUNCTION_NGINX_IMAGE")
AVAILABLE = bool(IMAGE) and os.name == "posix" and all(
    importlib.util.find_spec(name) for name in ("gunicorn", "websockets", "psutil")
)


class ProxyHarness(unittest.TestCase):
    server_options = {}
    http_path = "/identity"

    def setUp(self):
        self.backend = NativeServer(**self.server_options)
        self.addCleanup(self.backend.close)
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.config = Path(self.directory.name) / "nginx.conf"
        self.port = unused_tcp_port()
        self.container = "function-proxy-test-" + uuid4().hex
        self.write_config(self.backend.port)
        subprocess.run(["docker", "run", "--detach", "--name", self.container,
                        "--publish", f"127.0.0.1:{self.port}:8080",
                        "--mount", f"type=bind,src={self.directory.name},dst=/experiment,readonly",
                        IMAGE, "nginx", "-c", "/experiment/nginx.conf", "-g", "daemon off;"],
                       capture_output=True, text=True, check=True, timeout=15)
        self.addCleanup(lambda: subprocess.run(["docker", "rm", "--force", self.container],
                                               capture_output=True, check=True, timeout=10))
        wait(lambda: accepts(self.port))
        # A port can open before nginx has reached its upstream.
        wait(lambda: self.http()[0] == 200)

    def write_config(self, backend_port):
        self.config.write_text(f"""
pid /tmp/experiment-nginx.pid;
error_log /dev/stderr warn;
events {{ worker_connections 128; }}
http {{
    access_log off;
    server {{
        listen 8080;
        location / {{
            proxy_pass http://host.docker.internal:{backend_port};
            proxy_http_version 1.1;
            proxy_set_header Upgrade $http_upgrade;
            proxy_set_header Connection "upgrade";
            proxy_set_header Host $host;
            proxy_set_header X-Forwarded-Proto $scheme;
            proxy_set_header X-Forwarded-For $remote_addr;
            proxy_read_timeout 1s;
            proxy_buffering off;
        }}
    }}
}}
""")

    def http(self):
        client = http.client.HTTPConnection("127.0.0.1", self.port, timeout=2)
        try:
            client.request("GET", self.http_path)
            response = client.getresponse()
            body = response.read()
            return response.status, body
        except (OSError, http.client.HTTPException):
            return 503, b""
        finally:
            client.close()

    def socket(self):
        from websockets.sync.client import connect
        return connect(f"ws://127.0.0.1:{self.port}/socket", proxy=None,
                       origin="https://app.example.test", additional_headers={"Cookie": "sessionid=test-token"},
                       open_timeout=3, close_timeout=2)


@unittest.skipUnless(AVAILABLE, "set ASGI_FUNCTION_NGINX_IMAGE to a local nginx Docker image")
class ProxyTests(ProxyHarness):
    def test_protocol_pings_keep_idle_socket_alive_and_http_progresses(self):
        with self.socket() as connection:
            connection.send("before idle")
            first = json.loads(connection.recv(timeout=3))
            self.assertEqual(first["origin"], "https://app.example.test")
            self.assertEqual(first["cookie"], "sessionid=test-token")
            time.sleep(1.5)  # Longer than nginx's one-second upstream read timeout.
            self.assertEqual(self.http()[0], 200)
            connection.send("after idle")
            second = json.loads(connection.recv(timeout=3))
            self.assertEqual(second["pid"], first["pid"])
            self.assertEqual(second["echo"], "after idle")
        self.backend.stop()

    def test_worker_replacement_preserves_close_code_and_allows_reconnect(self):
        from websockets.exceptions import ConnectionClosed

        replacement = NativeServer()
        self.addCleanup(replacement.close)
        with self.socket() as connection:
            connection.send("old")
            old = json.loads(connection.recv(timeout=3))
            self.write_config(replacement.port)
            subprocess.run(["docker", "exec", self.container, "nginx", "-c",
                            "/experiment/nginx.conf", "-s", "reload"],
                           capture_output=True, text=True, check=True, timeout=5)
            wait(lambda: self.http()[0] == 200 and json.loads(self.http()[1])["pid"] != old["pid"])
            self.backend.stop()
            with self.assertRaises(ConnectionClosed) as error:
                connection.recv(timeout=3)
            self.assertEqual(error.exception.rcvd.code, 1012)
        with self.socket() as connection:
            connection.send("reconnected")
            result = json.loads(connection.recv(timeout=3))
            self.assertNotEqual(result["pid"], old["pid"])
            self.assertEqual(result["echo"], "reconnected")
        replacement.stop()


@unittest.skipUnless(AVAILABLE and bool(os.environ.get("ASGI_FUNCTION_REDIS_URL")) and all(
    importlib.util.find_spec(name) for name in ("redis", "graphene_django", "strawberry", "sentry_sdk")
), "requires nginx image, disposable Redis, GraphQL extras, and test group")
class RedisGraphQLProxyTests(ProxyHarness):
    server_options = {"target": "graphql_server:application", "env": {"ASGI_FUNCTION_BROADCAST": "redis"}}
    http_path = "/"

    def test_authenticated_broadcasts_and_shutdown_through_proxy(self):
        import redis
        from contextlib import ExitStack
        from websockets.sync.client import connect
        from websockets.exceptions import ConnectionClosed
        from test_django_auth_socket import Browser, ORIGIN

        browser = Browser(self.port)
        browser.login()
        client = redis.Redis.from_url(os.environ["ASGI_FUNCTION_REDIS_URL"])
        self.addCleanup(client.close)
        channel = "function-test:" + self.backend.path.name + ":proxy"
        with ExitStack() as stack:
            sockets = []
            for library in ("graphene", "strawberry"):
                connection = stack.enter_context(connect(
                    f"ws://127.0.0.1:{self.port}/{library}", proxy=None, origin=ORIGIN,
                    additional_headers={"Cookie": browser.socket_cookie()},
                    subprotocols=["graphql-transport-ws"], open_timeout=3, close_timeout=2))
                connection.send(json.dumps({"type": "connection_init"}))
                self.assertEqual(json.loads(connection.recv(timeout=3)), {"type": "connection_ack"})
                connection.send(json.dumps({"type": "subscribe", "id": "live",
                    "payload": {"query": 'subscription { updates(topic: "proxy") }'}}))
                sockets.append(connection)
            wait(lambda: client.pubsub_numsub(channel)[0][1] == 2)
            self.assertEqual(client.publish(channel, "11"), 2)
            for connection in sockets:
                result = json.loads(connection.recv(timeout=3))
                self.assertEqual(result, {"id": "live", "type": "next", "payload": {"data": {"updates": 11}}})
            self.backend.stop()
            for connection in sockets:
                with self.assertRaises(ConnectionClosed) as error:
                    connection.recv(timeout=3)
                self.assertEqual(error.exception.rcvd.code, 1012)
        self.assertEqual(client.pubsub_numsub(channel)[0][1], 0)
