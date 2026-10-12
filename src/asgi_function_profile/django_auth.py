"""Explicit session authentication refresh for long-lived Django operations."""

from dataclasses import dataclass
from types import SimpleNamespace

from django.contrib.auth import get_user


class PendingSessionChanges(ValueError):
    """Refreshing would discard unsaved changes to the current session."""


@dataclass(frozen=True)
class AuthenticationRefresh:
    user: object
    session_changed: bool
    http_login_required: bool


def refresh_authentication(request):
    """Reload session authentication without saving cookies or deciding access.

    Refuse pre-existing session modifications. Authentication may itself flush
    or rotate the reloaded session; report that separately. Freshness follows
    the configured session and authentication backends, not a revocation policy.
    The caller owns database cleanup and any permission/context updates.
    """
    previous = request.session
    if previous.modified:
        raise PendingSessionChanges("resolve pending session changes before refreshing authentication")
    key = previous.session_key
    session = previous.__class__(session_key=key)
    # Authenticate the candidate before replacing the request's current state.
    user = get_user(SimpleNamespace(session=session))
    changed = session.modified or session.session_key != key
    request.session = session
    request.user = user
    # Keep Django 5.2's lazy sync/async authentication accessors consistent.
    request._cached_user = request._acached_user = user
    return AuthenticationRefresh(user, changed, changed or not user.is_authenticated)
