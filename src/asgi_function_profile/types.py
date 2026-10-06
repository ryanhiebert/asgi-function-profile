"""Types for the ASGI function calling convention."""

from collections.abc import Callable
from typing import Any
from typing import TypeAlias

Scope: TypeAlias = dict[str, Any]
Message: TypeAlias = dict[str, Any]
Receive: TypeAlias = Callable[[], Message]
Send: TypeAlias = Callable[[Message], None]
Application: TypeAlias = Callable[[Scope, Receive, Send], None]
