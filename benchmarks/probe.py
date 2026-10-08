"""Local closed-loop WSGI/function-profile comparison; no production targets.

uv run --all-extras --group test python -m benchmarks.probe > /tmp/probe.json
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timezone
import http.client
import hashlib
from importlib.metadata import version
import json
import math
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import sys
import tempfile
from threading import Barrier
import time

import psutil
from websockets.sync.client import connect


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def resources(server):
    workers = psutil.Process(server.pid).children(recursive=True)
    if not workers:
        raise RuntimeError("no worker processes found")
    return {"threads": sum(worker.num_threads() for worker in workers),
            "rss_bytes": sum(worker.memory_info().rss for worker in workers),
            "virtual_bytes": sum(worker.memory_info().vms for worker in workers),
            "cpu_seconds": sum(worker.cpu_times().user + worker.cpu_times().system for worker in workers)}


def requests(port, path, count, concurrency):
    barrier = Barrier(concurrency)

    def client(index):
        connection = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
        latencies = []
        try:
            barrier.wait(timeout=5)
            for _ in range(index, count, concurrency):
                started = time.perf_counter()
                connection.request("GET", path)
                response = connection.getresponse()
                if response.status != 200 or response.read() != b"hello from Django\n":
                    raise AssertionError("unexpected benchmark response")
                latencies.append(time.perf_counter() - started)
            return latencies
        finally:
            connection.close()

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        timings = sorted(value for batch in executor.map(client, range(concurrency)) for value in batch)
    duration = time.perf_counter() - started
    return {"requests": count, "duration_seconds": duration,
            "requests_per_second": count / duration,
            "p50_ms": 1000 * timings[math.ceil(len(timings) * 0.50) - 1],
            "p95_ms": 1000 * timings[math.ceil(len(timings) * 0.95) - 1],
            "p99_ms": 1000 * timings[math.ceil(len(timings) * 0.99) - 1]}


def sample(mode, sockets, args):
    with tempfile.TemporaryDirectory() as directory, ExitStack() as stack:
        selected_port = port()
        worker, target = {
            "wsgi": ("gthread", "wsgi_application"),
            "coroutine": ("uvicorn_worker.UvicornWorker", "coroutine_application"),
            "function": ("asgi_function_profile.gunicorn.ThreadedUvicornWorker", "function_application"),
        }[mode]
        command = [sys.executable, "-m", "gunicorn", "benchmarks.application:" + target,
                   "--worker-class", worker, "--workers", str(args.workers),
                   "--threads", str(args.concurrency), "--bind", f"127.0.0.1:{selected_port}",
                   "--graceful-timeout", "5", "--log-level", "warning"]
        with (Path(directory) / "server.log").open("w+") as output:
            server = subprocess.Popen(command, stdout=output, stderr=subprocess.STDOUT,
                env={**os.environ, "FUNCTION_PROFILE_DEMO_DATABASE": str(Path(directory) / "db.sqlite3")},
                start_new_session=True)
            try:
                deadline = time.monotonic() + 10
                while True:
                    if server.poll() is not None:
                        output.seek(0)
                        raise RuntimeError(output.read())
                    try:
                        with socket.create_connection(("127.0.0.1", selected_port), 0.1):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("benchmark server did not start")
                        time.sleep(0.02)
                requests(selected_port, args.path, args.concurrency * 4, args.concurrency)
                baseline = resources(server)
                for _ in range(sockets):
                    connection = stack.enter_context(connect(
                        f"ws://127.0.0.1:{selected_port}/socket", proxy=None,
                        open_timeout=5, close_timeout=2))
                    connection.send("ready")
                    if connection.recv(timeout=5) != "ready":
                        raise AssertionError("socket did not echo")
                before = resources(server)
                measured = requests(selected_port, args.path, args.requests, args.concurrency)
                after = resources(server)
                stack.close()
                closed = resources(server)
                return {"mode": mode, "idle_sockets": sockets, "baseline": baseline,
                        "with_sockets": before, "after_requests": after,
                        "after_socket_close": closed,
                        "worker_cpu_seconds": after["cpu_seconds"] - before["cpu_seconds"], **measured}
            finally:
                stack.close()
                if server.poll() is None:
                    server.send_signal(signal.SIGTERM)
                try:
                    server.wait(timeout=7)
                except subprocess.TimeoutExpired:
                    os.killpg(server.pid, signal.SIGKILL)
                    server.wait()
                    raise RuntimeError("benchmark worker did not shut down gracefully")
                if server.returncode != 0:
                    output.seek(0)
                    raise RuntimeError(output.read())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--requests", type=int, default=512)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument("--sockets", type=int, nargs="+", default=[0, 32, 128])
    parser.add_argument("--path", choices=["/", "/wait/"], default="/wait/")
    args = parser.parse_args()
    if min(args.workers, args.repeat, args.concurrency) < 1 or args.requests < args.concurrency or min(args.sockets) < 0:
        parser.error("positive workers/repeats/concurrency and requests >= concurrency are required")
    result = {"time_utc": datetime.now(timezone.utc).isoformat(),
              "python": platform.python_version(), "os": platform.system(),
              "implementation": platform.python_implementation(),
              "gil_enabled": getattr(sys, "_is_gil_enabled", lambda: True)(),
              "machine": platform.machine(), "cpu_count": os.cpu_count(),
              "versions": {name: version(name) for name in ("Django", "gunicorn", "uvicorn", "uvicorn-worker")},
              "git_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
              "working_tree_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], text=True)),
              "source_sha256": {str(path): hashlib.sha256(path.read_bytes()).hexdigest()
                                for path in sorted([*Path("benchmarks").glob("*.py"),
                                                   *Path("src/asgi_function_profile").glob("*.py"),
                                                   Path("examples/django_demo/settings.py")])},
              "parameters": vars(args), "samples": []}
    for mode, sockets in [("wsgi", 0), ("coroutine", 0)] + [("function", n) for n in args.sockets]:
        for repetition in range(args.repeat):
            result["samples"].append({"repetition": repetition, **sample(mode, sockets, args)})
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
