"""Optional ordinary Graphene spans and execution-result error capture."""

from contextlib import contextmanager, suppress
from contextvars import ContextVar
from functools import wraps
from inspect import isasyncgen, isawaitable

import sentry_sdk
from sentry_sdk.integrations.graphene import GrapheneIntegration


_execution_reports = ContextVar("sentry_graphql_execution_reports", default=None)


class _ExecutionReports:
    def __init__(self):
        self.active = True
        self.decisions = {}


@contextmanager
def _execution_scope():
    reports = _execution_reports.get()
    if reports is not None and reports.active:
        yield reports
        return
    reports = _ExecutionReports()
    token = _execution_reports.set(reports)
    try:
        yield reports
    finally:
        # Copies of this context must also stop delegating after execution ends.
        reports.active = False
        reports.decisions.clear()
        _execution_reports.reset(token)


def _validate_policy(policy):
    if policy not in ("fallback", "always", "none"):
        raise ValueError("error_capture must be 'fallback', 'always' or 'none'")


def _ordinary(result):
    if isawaitable(result) or isasyncgen(result):
        if hasattr(result, "close"):
            result.close()
        raise TypeError("function-profile GraphQL hooks must be ordinary functions")
    return result


def _owns_capture(policy, client):
    return policy == "always" or (
        policy == "fallback" and client.get_integration(GrapheneIntegration) is None
    )


def instrument_execution(execute, *, error_capture="fallback", capture_error=None):
    """Wrap ordinary GraphQL execution; return its original ExecutionResult.

    Capture result errors, including serialization/validation, under the same
    policy as field middleware. capture_error(error) filters helper-owned errors;
    enabled SDK capture retains its own filtering. Nested wrappers share report
    decisions per client/error object, scoped to this execution. Unhandled
    exceptions escape to the caller's existing exception boundary.
    """
    _validate_policy(error_capture)

    @wraps(execute)
    def instrumented(*args, **kwargs):
        with _execution_scope() as reports:
            result = _ordinary(execute(*args, **kwargs))
            # Reporting failures never replace the execution result; one failed
            # report must not prevent attempts for the remaining errors.
            with suppress(Exception):
                client = sentry_sdk.get_client()
                if _owns_capture(error_capture, client):
                    for error in result.errors or ():
                        with suppress(Exception):
                            key = (id(client), id(error))
                            if key in reports.decisions:
                                continue
                            reports.decisions[key] = (client, error)  # Retain identities during nested calls.
                            if capture_error is None or _ordinary(capture_error(error)):
                                sentry_sdk.capture_exception(error)
            return result

    return instrumented


class SentryResolverMiddleware:
    """Add field spans with an explicit error-capture policy.

    error_capture="none" provides spans only; "always" handles execution that
    bypasses SDK result capture. Configure user attribution and application
    policy through the surrounding context/middleware. This class
    neither initializes Sentry nor changes its integration configuration.
    """

    def __init__(self, *, error_capture="fallback"):
        _validate_policy(error_capture)
        self.error_capture = error_capture

    def resolve(self, next_resolver, root, info, **kwargs):
        with sentry_sdk.start_span(op="graphql.resolve", name=info.field_name):
            try:
                return _ordinary(next_resolver(root, info, **kwargs))
            except Exception:
                with suppress(Exception):
                    reports = _execution_reports.get()
                    delegated = self.error_capture == "fallback" and reports is not None and reports.active
                    if not delegated and _owns_capture(self.error_capture, sentry_sdk.get_client()):
                        sentry_sdk.capture_exception()
                raise
