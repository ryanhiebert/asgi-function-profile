"""Ordinary consumer fixture using the existing channel-layer backend."""
import os
from pathlib import Path

from channels.layers import InMemoryChannelLayer
from asgi_function_profile.channels import FunctionChannelLayer, JsonWebsocketConsumer

STATE = Path(os.environ["ASGI_FUNCTION_PROFILE_TEST_STATE"])
layer = None


class Room(JsonWebsocketConsumer):
    def connect(self):
        self.join_group(os.environ.get("ASGI_FUNCTION_CHANNEL_GROUP", "room"))
        self.accept()
        self.send_json({"channel": self.channel_name})

    def receive_json(self, content):
        self.channel_layer.group_send(os.environ.get("ASGI_FUNCTION_CHANNEL_GROUP", "room"),
                                     {"type": "room.message", "value": content})

    def room_message(self, event):
        self.send_json(event["value"])

    def room_fail(self, event):
        raise ValueError("layer handler failure")

    def ws_drain(self, event):
        self.close(code=1001)

    def disconnect(self, code):
        (STATE / "consumer-disconnected").touch()


def application(scope, receive, send):
    global layer
    if scope["type"] == "lifespan":
        while True:
            event = receive()
            if event["type"] == "lifespan.startup":
                def factory():
                    url = os.environ.get("ASGI_FUNCTION_REDIS_URL")
                    if url:
                        from channels_redis.core import RedisChannelLayer
                        return RedisChannelLayer(hosts=[url], prefix=os.environ["ASGI_FUNCTION_CHANNEL_PREFIX"])
                    return InMemoryChannelLayer()
                layer = FunctionChannelLayer(factory)
                Room.channel_layer = layer
                (STATE / "lifespan-started").touch()
                send({"type": "lifespan.startup.complete"})
            elif event["type"] == "lifespan.shutdown":
                layer.close()
                (STATE / "lifespan-stopped").touch()
                send({"type": "lifespan.shutdown.complete"})
                return
    elif scope["type"] == "websocket":
        Room.as_asgi()(scope, receive, send)
    else:
        receive()
        send({"type": "http.response.start", "status": 200, "headers": []})
        send({"type": "http.response.body", "body": b"ok"})
