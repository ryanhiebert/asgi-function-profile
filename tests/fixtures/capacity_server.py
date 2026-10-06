"""Run the existing Django fixture with a measured, three-thread adapter pool."""

import json
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

# Match the normal runner's app directory behavior when this file is executed.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import uvicorn
from asgi_function_profile import FunctionProfileAdapter
from django_server import STATE, application as django_application

lock = threading.Lock()
active = 0


def record(event, scope):
    global active
    with lock:
        if event == "started":
            active += 1
        elif event == "finished":
            active -= 1
        label = scope.get("query_string", b"").decode("ascii") or scope["type"]
        entry = {"event": event, "scope": label, "active": active}
        with (STATE / "capacity-events").open("a") as output:
            output.write(json.dumps(entry) + "\n")


def measured_application(scope, receive, send):
    record("started", scope)
    try:
        return django_application(scope, receive, send)
    finally:
        record("finished", scope)


def main():
    # Explicit injection is already supported; no production launcher change.
    with ThreadPoolExecutor(max_workers=3) as executor:
        adapter = FunctionProfileAdapter(measured_application, executor=executor)

        async def application(scope, receive, send):
            record("arrived", scope)
            try:
                await adapter(scope, receive, send)
            finally:
                record("returned", scope)

        uvicorn.run(
            application, host="127.0.0.1", port=int(sys.argv[1]),
            interface="asgi3", lifespan="on", log_level="error",
            timeout_graceful_shutdown=2,
        )
    # The executor context has joined every worker before this is written.
    (STATE / "executor-stopped").touch()


if __name__ == "__main__":
    main()
