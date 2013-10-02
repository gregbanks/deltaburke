import os
import threading

from copy import copy, deepcopy
from functools import partial
from json import dumps
from tempfile import mkstemp
from unittest import TestCase

from bunch import Bunch

from deltaburke.config import (
    Config, ConfigManager, CurrentConfigAttr, FrozenError
)


class TestConfig(TestCase):
    def setUp(self):
        self.config = Config({'a': 1,
                              'b': 2,
                              'c': [1, 2, 3, {'z': 1}],
                              'd': {'e': {'f': 1}}})
        ConfigManager().monitor_interval = .1

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

    def test_mutable_clone(self):
        self.config._freeze()
        self.assertRaises(FrozenError, setattr, self.config, 'a', 3)
        clone = self.config.mutable_clone()
        self.assertEqual(self.config, clone)
        clone.a = 3
        self.assertEqual(clone.a, 3)

    def test_deepcopy(self):
        self.config._freeze()
        clone = deepcopy(self.config)
        self.assertEqual(self.config, clone)


class TestCurrentConfigAttr(TestCase):
    def setUp(self):
        ConfigManager().delete()
        class CurrentConfig(object):
            config = CurrentConfigAttr()
        self._current_config = CurrentConfig()

    def test_current_config(self):
        self.assertIsNone(self._current_config.config)
        ConfigManager().load({'a': 'b', 'c': {'d': 'e'}})
        self.assertEqual(self._current_config.config,
                         {'a': 'b', 'c': {'d': 'e'}})
        ConfigManager().merge({'c': {'f': 'g'}})
        self.assertEqual(self._current_config.config,
                         {'a': 'b', 'c': {'d': 'e', 'f': 'g'}})


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

    def test_monitor(self):
        fd, path = mkstemp()
        try:
            config = open(path, 'w')
            config.write(dumps({'a': 'b', 'c': {'d': 'e'}}))
            config.close()

            mgr = ConfigManager()
            mgr.load('file://%s' % (path), False, None, True)

            call_args = []
            callback_event = threading.Event()
            def callback(*args, **kwargs):
                call_args.append((args, kwargs))
                callback_event.set()

            mgr.register_update_callback(callback)

            self.assertEqual(mgr.config, {'a': 'b', 'c': {'d': 'e'}})

            config = open(path, 'w')
            config.write(dumps({'a': 'b', 'c': {'d': 'f'}}))
            config.close()

            callback_event.wait(3)
            self.assertEqual(len(call_args), 1)
            self.assertEqual(mgr.config, {'a': 'b', 'c': {'d': 'f'}})
            callback_event.clear()

            config = open(path, 'w')
            config.write(dumps({'a': 'b', 'c': {'g': 'h'}}))
            config.close()

            callback_event.wait(3)
            self.assertEqual(len(call_args), 2)
            self.assertEqual(mgr.config, {'a': 'b', 'c': {'d': 'f', 'g': 'h'}})
            callback_event.clear()

            mgr.delete()

            self.assertEqual(len(mgr._monitors.keys()), 0)
        finally:
            os.unlink(path)

    def test_string_substitutions(self):
        source = {
            '_subs': {'foo': 'bar',
                      'alice': 'bob',
                      'dead': 'beef'},
            'a': {'b': '${alice} in wonderland'},
            'c': 'dive ${foo}',
            'd': ['where\'s', 'the', '${dead}']}

        mgr = ConfigManager()
        mgr.load(source, False, sub_key='_subs')

        self.assertEqual(mgr.config.a.b, 'bob in wonderland')
        self.assertEqual(mgr.config.c, 'dive bar')
        self.assertEqual(mgr.config.d,
                         ['where\'s', 'the', 'beef'])

        mgr.merge({'a': {'e': '${foo}'}})

        self.assertEqual(mgr.config.a, {'b': 'bob in wonderland', 'e': 'bar'})

