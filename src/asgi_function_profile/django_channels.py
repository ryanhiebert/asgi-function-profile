"""Explicit ownership of native channel layers from Django configuration."""

import os
from contextlib import contextmanager
from contextvars import ContextVar
from threading import Lock
from weakref import WeakSet

from channels.exceptions import InvalidChannelLayerError
from channels.layers import channel_layers

from .channels import FunctionChannelLayer


_providers = WeakSet()


def _after_fork():
    for provider in tuple(_providers):
        provider._reset()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_after_fork)


class DjangoChannelLayerProvider:
    """Own shared layers; scope standalone publishers with ``publishing()``.

    Construction starts no threads. get() creates a layer lazily from the named
    CHANNEL_LAYERS entry; the owner must close() after its consumers finish.
    publishing() reuses that layer, or closes a temporary layer on context exit.
    Settings changes retain separate layers until close(). Fork resets ownership
    and locks without touching the parent's backends. No exit hook is installed.
    """

    def __init__(self, alias="default"):
        self.alias = alias
        self._temporary = ContextVar("function_channel_publisher", default=None)
        self._reset()
        _providers.add(self)

    def _reset(self):
        self._layers = {}
        self._lock = Lock()
        self._temporary.set(None)

    def _config(self):
        config = channel_layers.configs.get(self.alias)
        if config is None:
            raise InvalidChannelLayerError(f"no channel layer configured for alias {self.alias!r}")
        return config

    def _create(self):
        return FunctionChannelLayer(lambda: channel_layers.make_backend(self.alias))

    def get(self):
        config = self._config()
        with self._lock:
            key = id(config)
            cached = self._layers.get(key)
            if cached is None or cached[1].closed:
                cached = (config, self._create())
                self._layers[key] = cached  # Retain the config so its id cannot be reused.
            return cached[1]

    @contextmanager
    def publishing(self):
        temporary = self._temporary.get()
        if temporary is not None and not temporary.closed and temporary.pid == os.getpid():
            yield temporary
            return
        config = self._config()
        with self._lock:
            cached = self._layers.get(id(config))
            shared = cached[1] if cached is not None and not cached[1].closed else None
        if shared is not None:
            yield shared
            return
        with self._create() as layer:
            token = self._temporary.set(layer)
            try:
                yield layer
            finally:
                self._temporary.reset(token)

    def close(self):
        with self._lock:
            layers = tuple(self._layers.values())
            self._layers.clear()
        error = None
        for _, layer in layers:
            try:
                layer.close()
            except Exception as failure:
                if error is None:
                    error = failure
        if error is not None:
            raise error
