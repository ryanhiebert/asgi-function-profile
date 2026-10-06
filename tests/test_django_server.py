from __future__ import annotations

import http.client
import importlib.util
import json
import unittest

import test_server


DJANGO_AVAILABLE = importlib.util.find_spec("django") is not None
WEBSOCKETS_AVAILABLE = importlib.util.find_spec("websockets") is not None


@unittest.skipUnless(
    DJANGO_AVAILABLE and test_server.UVICORN_AVAILABLE,
    "requires the django and server extras",
)
class DjangoServerTests(test_server.UvicornTestCase):
    application_target = "django_server:application"

    def request(self, method, path, body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        try:
            connection.request(method, path, body, headers or {})
            response = connection.getresponse()
            headers = {key.lower(): value for key, value in response.getheaders()}
            return response.status, headers, response.read()
        finally:
            connection.close()

    def test_existing_views_middleware_cookies_forms_and_uploads(self):
        status, headers, body = self.request("GET", "/?name=Django")
        self.assertEqual((status, body), (200, b"Hello, Django!"))
        self.assertEqual(headers["x-same-thread"], "yes")
        self.assertIn("demo=working", headers["set-cookie"])
        self.assertEqual(headers["content-length"], str(len(body)))
        self.wait_for_file("finished-index")

        status, _, body = self.request(
            "POST", "/echo/", b"text=existing+view",
            {"Content-Type": "application/x-www-form-urlencoded"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"text": "existing view"})

        status, _, body = self.request("POST", "/echo/", b"raw request body")
        self.assertEqual((status, body), (200, b"raw request body"))

        upload = (
            b'--demo\r\nContent-Disposition: form-data; name="file"; '
            b'filename="test.txt"\r\nContent-Type: text/plain\r\n\r\n'
            b'uploaded file\r\n--demo--\r\n'
        )
        status, _, body = self.request(
            "POST", "/echo/", upload,
            {"Content-Type": "multipart/form-data; boundary=demo"},
        )
        self.assertEqual((status, body), (200, b"uploaded file"))
        # Real Django CSRF middleware remains effective on non-exempt views.
        self.assertEqual(self.request("POST", "/notes/")[0], 403)

    def test_orm_transactions_errors_and_connection_cleanup(self):
        status, headers, body = self.request("GET", "/notes/?text=unchanged")
        self.assertEqual(status, 200)
        self.assertEqual(json.loads(body), {"text": "unchanged"})
        self.assertEqual(headers["x-same-thread"], "yes")
        self.wait_for_file("finished-notes")
        self.assertEqual(self.request("GET", "/rollback/")[0], 500)
        self.wait_for_file("finished-rollback")
        self.assertEqual(self.request("GET", "/count/")[2], b"1")
        self.assertEqual(self.request("GET", "/does-not-exist/")[0], 404)

    def test_file_and_head_responses(self):
        status, headers, body = self.request("GET", "/download/")
        self.assertEqual((status, body), (200, b"a file from Django\n"))
        self.assertIn('filename="demo.txt"', headers["content-disposition"])
        self.wait_for_file("finished-download")
        status, headers, body = self.request("HEAD", "/")
        self.assertEqual((status, body), (200, b""))
        self.assertEqual(headers["content-length"], str(len(b"Hello, world!")))

    def test_sync_iterator_streams_without_buffering_and_closes(self):
        with self.connect() as connection:
            connection.sendall(
                b"GET /gated-stream/ HTTP/1.1\r\nHost: localhost\r\n"
                b"Connection: close\r\n\r\n"
            )
            response = b""
            while b"5\r\nfirst\r\n" not in response:
                data = connection.recv(4096)
                self.assertTrue(data)
                response += data
            self.wait_for_file("stream-first-sent")
            self.assertFalse(self.marker("stream-closed").exists())
            self.marker("stream-release").touch()
            while data := connection.recv(4096):
                response += data
        self.assertIn(b"4\r\nlast\r\n0\r\n\r\n", response)
        self.wait_for_file("stream-closed")
        self.wait_for_file("finished-gated-stream")

    @unittest.skipUnless(WEBSOCKETS_AVAILABLE, "requires the test dependency group")
    def test_sync_websocket_and_django_share_the_server(self):
        from websockets.sync.client import connect

        with connect(f"ws://127.0.0.1:{self.port}/ws/", proxy=None) as websocket:
            websocket.send("hello")
            self.assertEqual(websocket.recv(timeout=5), "hello")
            # The socket remains open while an unchanged Django view runs.
            self.assertEqual(self.request("GET", "/")[0], 200)
            websocket.send(b"binary")
            self.assertEqual(websocket.recv(timeout=5), b"binary")
        self.wait_for_file("websocket-finished")
