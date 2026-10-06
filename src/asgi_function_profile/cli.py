"""Development runner for function-profile applications."""

from __future__ import annotations

import argparse
import importlib
import sys

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from .adapter import FunctionProfileAdapter
from .types import Application


def load_application(target: str) -> Application:
    """Load ``module:attribute`` without inspecting its calling convention."""

    module_name, separator, attribute_path = target.partition(":")
    if not separator or not module_name or not attribute_path:
        raise ValueError("application must be specified as module:attribute")

    value: Any = importlib.import_module(module_name)
    for attribute in attribute_path.split("."):
        value = getattr(value, attribute)
    if not callable(value):
        raise TypeError(f"{target!r} does not resolve to a callable")
    return value


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="asgi-function",
        description="Run an ASGI function-profile application with Uvicorn.",
    )
    parser.add_argument("application", help="function application as module:attribute")
    parser.add_argument(
        "--app-dir",
        default=".",
        help="directory containing the application module (default: current directory)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info", "debug", "trace"),
        default="info",
    )
    arguments = parser.parse_args(argv)
    sys.path.insert(0, str(Path(arguments.app_dir).resolve()))

    try:
        import uvicorn
    except ImportError as error:
        parser.error(
            "Uvicorn is required by the development runner; "
            "install asgi-function-profile[server]"
        )
        raise AssertionError("unreachable") from error

    try:
        application = load_application(arguments.application)
    except (AttributeError, ImportError, TypeError, ValueError) as error:
        parser.error(str(error))
        raise AssertionError("unreachable") from error

    uvicorn.run(
        FunctionProfileAdapter(application),
        host=arguments.host,
        port=arguments.port,
        log_level=arguments.log_level,
    )
