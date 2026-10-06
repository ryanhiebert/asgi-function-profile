"""Bridge function-profile applications to coroutine-profile ASGI servers."""

from __future__ import annotations

import asyncio
import contextvars

from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from concurrent.futures import CancelledError as FutureCancelledError
from concurrent.futures import Executor
from concurrent.futures import Future as ConcurrentFuture
from functools import partial
from threading import Lock
from typing import Any
from typing import TypeVar

from .types import Application
from .types import Message
from .types import Scope


Result = TypeVar("Result")
AsyncReceive = Callable[[], Awaitable[Message]]
AsyncSend = Callable[[Message], Awaitable[None]]


class BridgeClosedError(RuntimeError):
    """Raised when a server operation is attempted after its bridge closes."""


class _Bridge:
    """Expose ASGI awaitables as blocking calls from worker threads."""

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        receive: AsyncReceive,
        send: AsyncSend,
    ) -> None:
        self._loop = loop
        self._async_receive = receive
        self._async_send = send
        self._lock = Lock()
        self._closed = False
        self._pending: set[ConcurrentFuture[Any]] = set()

    def close(self) -> None:
        """Close the bridge and release calls waiting on ASGI operations."""

        with self._lock:
            if self._closed:
                return
            self._closed = True
            pending = tuple(self._pending)

        for future in pending:
            future.cancel()

    def receive(self) -> Message:
        async def operation() -> Message:
            return await self._async_receive()

        return self._submit(operation)

    def send(self, message: Message) -> None:
        async def operation() -> None:
            await self._async_send(message)

        self._submit(operation)

    def _submit(
        self,
        operation: Callable[[], Coroutine[Any, Any, Result]],
    ) -> Result:
        with self._lock:
            if self._closed:
                raise BridgeClosedError("the ASGI invocation has ended")

            coroutine = operation()
            try:
                future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
            except BaseException:
                coroutine.close()
                raise
            self._pending.add(future)

        try:
            return future.result()
        except FutureCancelledError as error:
            raise BridgeClosedError("the ASGI invocation ended while waiting") from error
        finally:
            with self._lock:
                self._pending.discard(future)


class FunctionProfileAdapter:
    """Present a function-profile application as an ASGI 3 application.

    Each scope invocation runs in an executor worker. Calls to the supplied
    ordinary ``receive`` and ``send`` functions are executed by the ASGI event
    loop, and the worker waits for their actual completion.
    """

    def __init__(
        self,
        application: Application,
        *,
        executor: Executor | None = None,
    ) -> None:
        self.application = application
        self.executor = executor

    async def __call__(
        self,
        scope: Scope,
        receive: AsyncReceive,
        send: AsyncSend,
    ) -> None:
        loop = asyncio.get_running_loop()
        bridge = _Bridge(loop, receive, send)
        context = contextvars.copy_context()
        invoke = partial(
            context.run,
            self.application,
            scope,
            bridge.receive,
            bridge.send,
        )

        try:
            worker = loop.run_in_executor(self.executor, invoke)
            await worker
        finally:
            bridge.close()


def adapt(
    application: Application,
    *,
    executor: Executor | None = None,
) -> FunctionProfileAdapter:
    """Create a coroutine-profile adapter for deployment infrastructure."""

    return FunctionProfileAdapter(application, executor=executor)
