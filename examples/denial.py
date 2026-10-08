"""A regular-function use of ASGI's WebSocket denial-response extension."""


def application(scope, receive, send):
    if scope["type"] != "websocket":
        raise ValueError("this example only handles websocket scopes")
    if receive()["type"] != "websocket.connect":
        return
    if "websocket.http.response" not in scope.get("extensions", {}):
        # Core ASGI supplies the default HTTP denial response in this case.
        send({"type": "websocket.close", "code": 1008})
        return
    send({"type": "websocket.http.response.start", "status": 429,
          "headers": [(b"content-type", b"text/plain"), (b"retry-after", b"5")]})
    send({"type": "websocket.http.response.body", "body": b"try again ", "more_body": True})
    send({"type": "websocket.http.response.body", "body": b"later", "more_body": False})
