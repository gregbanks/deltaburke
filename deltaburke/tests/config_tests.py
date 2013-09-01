from copy import copy
from functools import partial
from unittest import TestCase

from bunch import Bunch

from deltaburke.config import Config, ConfigManager, FrozenError


class TestConfig(TestCase):
    def setUp(self):
        self.config = Config({'a': 1,
                              'b': 2,
                              'c': [1, 2, 3, {'z': 1}],
                              'd': {'e': {'f': 1}}})

    def test_walk(self):
        expected_callback_args = \
            [[[], Config(a=1, b=2, c=[1, 2, 3, {'z': 1}],
                         d=Bunch(e=Bunch(f=1))), False],
             [['a'], 1, True],
             [['b'], 2, True],
             [['c'], [1, 2, 3, {'z': 1}], True],
             [['d'], Bunch(e=Bunch(f=1)), False],
             [['d', 'e'], Bunch(f=1), False],
             [['d', 'e', 'f'], 1, True]]

        callback_args = []

        def callback(keys, val, parent, isleaf):
            callback_args.append([copy(keys), val, isleaf])
            return True

        Config._walk(self.config, callback)
        self.assertEqual(callback_args, expected_callback_args)

    def test_moon_walk(self):
        expected_callback_args = \
            [[['a'], 1, True],
             [['b'], 2, True],
             [['c'], [1, 2, 3, {'z': 1}], True],
             [['d', 'e', 'f'], 1, True],
             [['d', 'e'], Bunch(f=1), False],
             [['d'], Bunch(e=Bunch(f=1)), False],
             [[], Config(a=1, b=2, c=[1, 2, 3, {'z': 1}],
                         d=Bunch(e=Bunch(f=1))), False]]

        callback_args = []

        def callback(keys, val, parent, isleaf):
            callback_args.append([copy(keys), val, isleaf])
            return True

        Config._moon_walk(self.config, callback)
        self.assertEqual(callback_args, expected_callback_args)

    def test_freeze(self):
        self.config._freeze()
        error = None
        try:
            self.config.a = {'blah': 1}
        except Exception, e:
            error = e
        self.assertIsInstance(error, FrozenError)

        error = None
        try:
            self.config.d.e.f = {'blah': 1}
        except Exception, e:
            error = e
        self.assertIsInstance(error, FrozenError)

    def test_thaw(self):
        self.config._freeze()

        error = None
        try:
            self.config.d.e.f = {'blah': 1}
        except Exception, e:
            error = e
        self.assertIsInstance(error, FrozenError)

        self.config._thaw()

        error = None
        try:
            self.config.d.e.f = {'blah': 1}
        except Exception, e:
            error = e
        self.assertIsNone(error)

    def test_handle(self):
        self.config._freeze()

        error = None
        try:
            self.config.d.e.f = 2
        except Exception, e:
            error = e
        self.assertIsInstance(error, FrozenError)
        self.assertEqual(self.config.d.e.f, 1)

        with self.config._handle():
            self.config.d.e.f = 2

        self.assertEqual(self.config.d.e.f, 2)

        error = None
        try:
            self.config.d.e.f = 3
        except Exception, e:
            error = e
        self.assertIsInstance(error, FrozenError)
        self.assertEqual(self.config.d.e.f, 2)


class TestConfigManager(TestCase):
    def setUp(self):
        self.configs = [
            {'a': 1,
             'b': 2,
             'c': [3, 4, {'d': 'e'}],
             'f': {'g': {'h': 5}}},
            {'a': {'b': 'c'}},
            {'a': {'w': 'x', 'y': 'z', 'b': 10}},
            {'i': [6, 7, 8]},
            {'f': {'g': {'j': 9}}}
        ]

    def test_load_src(self):
        mgr = ConfigManager()
        mgr.load(self.configs[0])
        self.assertEqual(mgr.config, self.configs[0])

    def test_load_src_with_merges(self):
        mgr = ConfigManager()
        mgr.load(self.configs)
        mgr.config._thaw()
        self.assertEqual(mgr.config,
                         {'a': {'b': 10, 'w': 'x', 'y': 'z'},
                          'b': 2,
                          'c': [3, 4, {'d': 'e'}],
                          'i': [6, 7, 8],
                          'f': {'g': {'h': 5, 'j': 9}}})

    def test_register_unregister_callback(self):
        def callback():
            pass
        mgr = ConfigManager()
        mgr.register_update_callback(callback)
        self.assertTrue(
            bool(
                mgr._update_signals[ConfigManager.DEFAULT_NAMESPACE].receivers))
        mgr.unregister_update_callback(callback)
        self.assertFalse(
            bool(
                mgr._update_signals[ConfigManager.DEFAULT_NAMESPACE].receivers))

    def test_signal_change(self):
        result = {}
        def callback(_config):
            result['config'] = _config
        mgr = ConfigManager()
        mgr.register_update_callback(callback)
        mgr.load(self.configs[0])
        self.assertEqual(result['config'], self.configs[0])

    def test_namespaces(self):
        out = []
        def callback_foo(out, config):
            out.append('foo callback called')
        def callback_bar(out, config):
            out.append('bar callback called')
        mgr = ConfigManager()
        mgr.load(self.configs[0], namespace='foo')
        cb_foo_partial = partial(callback_foo, out)
        mgr.register_update_callback(cb_foo_partial, namespace='foo')
        mgr.load(self.configs[1], namespace='bar')
        cb_bar_partial = partial(callback_bar, out)
        mgr.register_update_callback(cb_bar_partial, namespace='bar')
        self.assertEqual(len(ConfigManager()._update_signals.keys()), 3)
        with mgr.namespace('foo'):
            mgr.load(self.configs[2])
        self.assertIn('foo callback called', out)
        self.assertNotIn('bar callback called', out)
        self.assertEqual(mgr.get_config('foo'), self.configs[2])
        self.assertEqual(mgr.get_config('bar'), self.configs[1])
        with mgr.namespace('bar'):
            mgr.unregister_update_callback(cb_bar_partial)
        self.assertEqual(len(ConfigManager()._update_signals.keys()), 2)


