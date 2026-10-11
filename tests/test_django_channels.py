"""Configured layer ownership, scoped publishers and isolation after fork."""

import importlib.util
import os
from queue import Queue
import subprocess
import sys
from threading import Barrier, get_ident
from concurrent.futures import ThreadPoolExecutor
import unittest
from unittest.mock import patch


AVAILABLE = importlib.util.find_spec("channels") is not None
GRAPHENE = AVAILABLE and importlib.util.find_spec("channels_graphql_ws") is not None


@unittest.skipUnless(AVAILABLE, "requires channels extra")
class DjangoChannelProviderTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import django
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "examples.django_demo.settings")
        django.setup()

    def setUp(self):
        from django.test import override_settings
        from asgi_function_profile.django_channels import DjangoChannelLayerProvider
        configuration = {
            "default": {"BACKEND": "channels.layers.InMemoryChannelLayer", "CONFIG": {"capacity": 2}},
            "other": {"BACKEND": "channels.layers.InMemoryChannelLayer", "CONFIG": {"capacity": 3}},
        }
        settings = override_settings(CHANNEL_LAYERS=configuration)
        settings.enable()
        self.addCleanup(settings.disable)
        self.provider = DjangoChannelLayerProvider()
        self.addCleanup(self.provider.close)
        from asgi_function_profile import django_channels
        factory = django_channels.FunctionChannelLayer
        self.created = []
        def create(backend):
            layer = factory(backend)
            self.created.append(layer)
            return layer
        spy = patch.object(django_channels, "FunctionChannelLayer", side_effect=create)
        spy.start()
        self.addCleanup(spy.stop)

    def test_lazy_creation_aliases_shared_reuse_and_recreation_after_close(self):
        from asgi_function_profile.django_channels import DjangoChannelLayerProvider
        from channels.layers import channel_layers
        self.assertEqual(self.created, [])
        construction = []
        factory = channel_layers.make_backend
        def backend(alias):
            construction.append(get_ident())
            return factory(alias)
        with patch.object(channel_layers, "make_backend", side_effect=backend):
            layer = self.provider.get()
        self.assertEqual(construction, [layer.thread.ident])
        self.assertNotEqual(layer.thread.ident, get_ident())
        self.assertIs(self.provider.get(), layer)
        with self.provider.publishing() as borrowed:
            self.assertIs(borrowed, layer)
        self.assertFalse(layer.closed)
        other = DjangoChannelLayerProvider("other")
        self.addCleanup(other.close)
        self.assertEqual(other.get().backend.capacity, 3)
        self.assertEqual(layer.backend.capacity, 2)
        self.provider.close()
        self.assertFalse(layer.thread.is_alive())
        self.assertTrue(layer.loop.is_closed())
        self.assertIsNot(self.provider.get(), layer)

    def test_standalone_and_nested_publishers_close_without_caching(self):
        with self.provider.publishing() as layer:
            with self.provider.publishing() as nested:
                self.assertIs(nested, layer)
                nested.group_send("room", {"type": "notice"})
            self.assertFalse(layer.closed)
        self.assertTrue(layer.closed)
        self.assertFalse(layer.thread.is_alive())
        self.assertEqual(self.created, [layer])
        with self.provider.publishing() as next_layer:
            self.assertIsNot(next_layer, layer)
        self.assertTrue(next_layer.closed)
        self.assertEqual(self.provider._layers, {})

    def test_configuration_changes_retain_separate_layers_until_cleanup(self):
        from django.test import override_settings
        first = self.provider.get()
        with override_settings(CHANNEL_LAYERS={"default": {
                "BACKEND": "channels.layers.InMemoryChannelLayer", "CONFIG": {"capacity": 7}}}):
            second = self.provider.get()
            self.assertIsNot(second, first)
            self.assertEqual(second.backend.capacity, 7)
        self.assertIs(self.provider.get(), first)
        self.provider.close()
        self.assertTrue(first.closed and second.closed)

    def test_deferred_copied_context_does_not_reuse_closed_temporary_layer(self):
        from contextvars import copy_context
        with self.provider.publishing() as previous:
            deferred = copy_context()
        def publish():
            with self.provider.publishing() as fresh:
                self.assertIsNot(fresh, previous)
                fresh.group_send("room", {"type": "deferred"})
        deferred.run(publish)
        self.assertEqual(len(self.created), 2)
        self.assertTrue(all(layer.closed and not layer.thread.is_alive() for layer in self.created))

    def test_unconfigured_alias_starts_no_backend(self):
        from channels.exceptions import InvalidChannelLayerError
        from asgi_function_profile.django_channels import DjangoChannelLayerProvider
        missing = DjangoChannelLayerProvider("missing")
        with self.assertRaisesRegex(InvalidChannelLayerError, "missing"):
            missing.get()
        with self.assertRaises(InvalidChannelLayerError):
            with missing.publishing():
                self.fail("unconfigured publisher was started")
        self.assertEqual(self.created, [])

    def test_publisher_failure_survives_backend_cleanup_and_next_call_recovers(self):
        from channels.layers import InMemoryChannelLayer, channel_layers
        failure = TimeoutError("backend send failed")
        class Backend(InMemoryChannelLayer):
            async def group_send(self, *args):
                raise failure
            async def close_pools(self):
                raise OSError("cleanup failed")
        with patch.object(channel_layers, "make_backend", side_effect=lambda alias: Backend()):
            with self.assertRaises(TimeoutError) as raised:
                with self.provider.publishing() as layer:
                    layer.group_send("room", {"type": "notice"})
        self.assertIs(raised.exception, failure)
        self.assertTrue(layer.loop.is_closed())
        self.assertFalse(layer.thread.is_alive())
        with self.provider.publishing() as recovered:
            recovered.group_send("room", {"type": "recovered"})
        self.assertTrue(recovered.closed)

    def test_cleanup_attempts_all_cached_backends_and_reports_first_error(self):
        from channels.layers import InMemoryChannelLayer, channel_layers
        from django.test import override_settings
        failure = OSError("close failed")
        closed = []
        class Backend(InMemoryChannelLayer):
            async def close_pools(self):
                closed.append(self)
                raise failure
        with patch.object(channel_layers, "make_backend", side_effect=lambda alias: Backend()):
            first = self.provider.get()
            with override_settings(CHANNEL_LAYERS={"default": {"BACKEND": "unused"}}):
                second = self.provider.get()
            with self.assertRaises(OSError) as raised:
                self.provider.close()
        self.assertIs(raised.exception, failure)
        self.assertCountEqual(closed, [first.backend, second.backend])
        self.assertTrue(all(layer.loop.is_closed() and not layer.thread.is_alive() for layer in (first, second)))

    @unittest.skipUnless(GRAPHENE, "requires channels-graphene extra")
    def test_concurrent_graphene_publishers_have_independent_temporary_ownership(self):
        import graphene
        from channels.layers import InMemoryChannelLayer, channel_layers
        from channels_graphql_ws.serializer import Serializer
        from asgi_function_profile.channels_graphene import Subscription
        barrier, sent = Barrier(2), Queue()
        failure = ConnectionError("one publisher failed")
        provider = self.provider
        class Notice(Subscription):
            value = graphene.String()
            channel_layer_provider = provider
            def publish(payload, info):
                return Notice(value=payload)
        class Backend(InMemoryChannelLayer):
            async def group_send(self, group, message):
                payload = Serializer.deserialize(message["payload"])
                self_outer.assertIsNone(Notice.channel_layer)
                self_outer.assertIs(Notice.channel_layer_provider, provider)
                barrier.wait(5)
                sent.put((self, payload))
                if payload == "fail":
                    raise failure
        self_outer = self
        with patch.object(channel_layers, "make_backend", side_effect=lambda alias: Backend()):
            with ThreadPoolExecutor(max_workers=2) as threads:
                failed = threads.submit(Notice.broadcast_sync, group="room", payload="fail")
                succeeded = threads.submit(Notice.broadcast, group="room", payload="ok")
                self.assertIsNone(succeeded.result(timeout=10))
                with self.assertRaises(ConnectionError) as raised:
                    failed.result(timeout=10)
        self.assertIs(raised.exception, failure)
        events = [sent.get(timeout=1), sent.get(timeout=1)]
        self.assertIsNot(events[0][0], events[1][0])
        self.assertCountEqual([value for _, value in events], ["fail", "ok"])
        self.assertEqual(len(self.created), 2)
        self.assertTrue(all(layer.closed and not layer.thread.is_alive() for layer in self.created))
        self.assertIsNone(Notice.channel_layer)
        self.assertIs(Notice.channel_layer_provider, provider)

    @unittest.skipUnless(hasattr(os, "fork"), "requires POSIX fork")
    def test_fork_resets_cached_layer_and_inherited_lock(self):
        script = '''
import os, signal, warnings
from contextvars import copy_context
from django.conf import settings
settings.configure(CHANNEL_LAYERS={"default": {"BACKEND": "channels.layers.InMemoryChannelLayer"}})
from asgi_function_profile.django_channels import DjangoChannelLayerProvider
provider = DjangoChannelLayerProvider()
with provider.publishing() as inherited_temporary:
    copied = copy_context()
    parent = provider.get()
    with provider._lock, warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        child = os.fork()
    if child == 0:
        signal.alarm(5)
        try:
            def publish():
                with provider.publishing() as temporary:
                    assert temporary.pid == os.getpid()
                    assert temporary is not parent and temporary is not inherited_temporary
                    assert not provider._layers
                    temporary.group_send("room", {"type": "child"})
            copied.run(publish)
            shared = provider.get()
            assert shared.pid == os.getpid() and shared is not parent
            provider.close()
            assert shared.loop.is_closed() and not shared.thread.is_alive()
        except BaseException:
            import traceback
            traceback.print_exc()
            os._exit(1)
        os._exit(0)
    assert os.waitpid(child, 0)[1] == 0
    assert provider.get() is parent and parent.thread.is_alive()
    assert not inherited_temporary.closed
provider.close()
assert parent.loop.is_closed() and not parent.thread.is_alive()
'''
        result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
