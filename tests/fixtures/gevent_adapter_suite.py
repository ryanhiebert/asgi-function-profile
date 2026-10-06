"""Run the same bridge contract tests in an isolated monkey-patched process."""

import sys
import unittest
from functools import partial
from pathlib import Path

from asgi_function_profile.gevent import _GreenletExecutor
from gevent import monkey
from gevent.threadpool import ThreadPoolExecutor

monkey.patch_all()
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import test_adapter

with _GreenletExecutor() as executor:
    test_adapter.FunctionProfileAdapter = partial(
        test_adapter.FunctionProfileAdapter, executor=executor,
    )
    suite = unittest.defaultTestLoader.loadTestsFromModule(test_adapter)
    with ThreadPoolExecutor(max_workers=1) as native:
        result = native.submit(unittest.TextTestRunner(verbosity=2).run, suite).result()
assert not executor.group
raise SystemExit(not result.wasSuccessful())
