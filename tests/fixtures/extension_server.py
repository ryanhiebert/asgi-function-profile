from examples.denial import application as denial
import server_app


def application(scope, receive, send):
    if scope["type"] == "websocket":
        return denial(scope, receive, send)
    return server_app.application(scope, receive, send)
