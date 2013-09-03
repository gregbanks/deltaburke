import threading

from contextlib import contextmanager
from functools import wraps

import blinker

from bunch import Bunch, bunchify

from loader import Loader


def synchronized(lock):
    def wrap(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _lock = getattr(args[0], lock) if isinstance(lock, basestring) \
                                           else lock
            with _lock:
                return func(*args, **kwargs)
        return wrapper
    return wrap


class FrozenError(Exception):
    pass


class Frozen(Bunch):
    def __setattr__(self, k, v):
        raise FrozenError()

    def __setitem__(self, k, v):
        raise FrozenError()


class Config(Bunch):
    def __init__(self, *args, **kwargs):
        super(Config, self).__init__(*args, **kwargs)
        self.__dict__['_frozen'] = False
        for item in [k for k in self.keys() if not k.startswith('_')]:
            if isinstance(self[item], dict):
                self[item] = bunchify(self[item])

    def __setattr__(self, k, v):
        if self._frozen:
            raise FrozenError()
        super(Config, self).__setattr__(k, v)

    def __setitem__(self, k, v):
        if self._frozen:
            raise FrozenError()
        super(Config, self).__setitem__(k, v)

    @classmethod
    def _traverse(cls, direction, config, callback, keys=None):
        def _handle_dict(keys, val, parent=None):
            if direction == 'down':
                callback(keys, val, parent, not isinstance(val, dict))
                if len(keys) > 0:
                    val = parent[keys[-1]]
                cls._traverse(direction, val, callback, keys)
            elif direction == 'up':
                cls._traverse(direction, val, callback, keys)
                callback(keys, val, parent, not isinstance(val, dict))
            else:
                raise ValueError('unknown direction %s' % (direction))
        if keys is None:
            keys = []
            _handle_dict(keys, config)
        elif isinstance(config, (dict, Frozen)):
            for key in sorted(config.keys()):
                keys.append(key)
                _handle_dict(keys, config[key], config)
                keys.pop()

    @classmethod
    def _walk(cls, config, callback, keys=None):
        try:
            cls._traverse('down', config, callback, keys)
        except StopIteration:
            pass

    @classmethod
    def _moon_walk(cls, config, callback, keys=None):
        try:
            cls._traverse('up', config, callback, keys)
        except StopIteration:
            pass

    def _merge(self, other):
        """ Merge another config into this one

        """
        def _merge_node(keys, val, parent, isleaf):
            if not isleaf:
                return
            node = self
            for i in xrange(len(keys) - 1):
                if keys[i] not in node or \
                   not isinstance(node[keys[i]], dict):
                    for key in keys[i:-1]:
                        node[key] = Bunch()
                        node = node[key]
                    break
                node = node[keys[i]]
            node[keys[-1]] = val

        with self._handle():
            Config._walk(other, _merge_node)

    def _freeze(self):
        """ Make Config immutable

        NOTE: embedded lists are still mutable

        """
        if self._frozen:
            return
        def _freeze_node(keys, val, parent, isleaf):
            if val is not self and isinstance(val, dict):
                parent[keys[-1]] = Frozen(val)
        Config._moon_walk(self, _freeze_node)
        self._frozen = True


    def _thaw(self):
        """ Make Config mutable

        """
        if not self._frozen:
            return
        def _thaw_node(keys, val, parent, isleaf):
            if val is not self and isinstance(val, Frozen):
                parent[keys[-1]] = Bunch(val)
        self.__dict__['_frozen'] = False
        Config._walk(self, _thaw_node)

    @contextmanager
    def _handle(self):
        """ Convenience context manager for updating a config

        """
        frozen = self._frozen
        if frozen:
            self._thaw()
        try:
            yield
        finally:
            if frozen:
                self._freeze()


class ConfigManager(object):
    DEFAULT_NAMESPACE = 'default'

    _lock = threading.RLock()

    def __new__(cls, *args, **kwargs):
        if '_instance' not in cls.__dict__:
            cls._instance = \
                super(ConfigManager, cls).__new__(cls, *args, **kwargs)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._configs = {}
            self._initialized = True
            self._namespace = self.__class__.DEFAULT_NAMESPACE
            self._signal_namespace = blinker.Namespace()
            self._update_signals = {}
            self._update_signals[self.__class__.DEFAULT_NAMESPACE] = \
                self._signal_namespace.signal(
                    self.__class__._update_signal_name(
                        self.__class__.DEFAULT_NAMESPACE))

    @staticmethod
    def _update_signal_name(namespace):
        return '__deltaburke__.%s' % (namespace)

    @property
    def lock(self):
        return self._lock

    @property
    @synchronized(_lock)
    def config(self):
        return self._configs.get(self._namespace, None)

    @synchronized(_lock)
    def get_config(self, namespace=None):
        if namespace is None:
            return self.config
        return self._configs.get(namespace, None)

    @synchronized(_lock)
    def load(self, config_src, signal_update=True, namespace=None):
        """ Load config from source(s)

        :param config_src:  URI(s) or dictionaries to load the config from. If
                            config_src is a list, then the first config is
                            loaded as the main config with subsequent configs
                            meged into it.
        :type config_src:   a string or dictionary or list of strings and/or
                            dictionaries

        """
        namespace = self._namespace if namespace is None else namespace
        merge_configs = []
        if isinstance(config_src, list):
            merge_configs = config_src[1:]
            config_src = config_src[0]
        if isinstance(config_src, basestring):
            config_src = Loader.load(config_src)
        self._configs[namespace] = Config(bunchify(config_src))
        self.merge(merge_configs, False, namespace)
        self._configs[namespace]._freeze()
        if signal_update:
            self.signal_update(namespace)

    @synchronized(_lock)
    def merge(self, config_src, signal_update=False, namespace=None):
        """ Merge configs

        :param config_src:  URI(s) or dictionaries to load config(s) from to
                            be merged into the main config
        :type config_src:   a string or dictionary or list of strings and/or
                            dictionaries

        """
        namespace = self._namespace if namespace is None else namespace
        if self._configs.get(namespace, None) is None:
            raise ValueError('no config to merge with!')
        if not isinstance(config_src, list):
            config_src = [config_src]
        for config in config_src:
            if isinstance(config, basestring):
                config = Loader.load(config)
            self._configs[namespace]._merge(bunchify(config))
        if signal_update:
            self.signal_update(namespace)
    
    @synchronized(_lock)
    def delete(self, namespace=None):
        namespace = self._namespace if namespace is None else namespace
        try:
            del self._configs[namespace]
        except KeyError:
            pass

    @synchronized(_lock)
    def start_src_monitor(self, interval=60, namespace=None):
        """ Monitor config sources for changes in a separate thread

        If a change occurs, then update the config and signal a change to those
        listening
        """
        raise NotImplementedError()

    @synchronized(_lock)
    def stop_src_monitor(self, namespace=None):
        """ Monitor config sources for changes in a separate thread

        If a change occurs, then update the config and signal a change to those
        listening
        """
        raise NotImplementedError()

    @synchronized(_lock)
    def register_update_callback(self, callback, namespace=None):
        """ Register callback for updates

        :param callback: a function or method to be called when the config is
                         updated
        :type callback:  a function or mehod

        """
        namespace = self._namespace if namespace is None else namespace
        signal_name = self._update_signal_name(namespace)
        if namespace not in self._update_signals:
            self._update_signals[namespace] = \
                self._signal_namespace.signal(signal_name)
        self._update_signals[namespace].connect(callback)

    @synchronized(_lock)
    def unregister_update_callback(self, callback, namespace=None):
        """ Unregister callback for updates

        :param callback: the function or method to unregister for config
                         updates
        :type callback: a function or method

        """
        namespace = self._namespace if namespace is None else namespace
        signal_name = self._update_signal_name(namespace)
        if namespace not in self._update_signals:
            return
        self._update_signals[namespace].disconnect(callback)
        if namespace != self.__class__.DEFAULT_NAMESPACE and \
           not bool(self._update_signals[namespace].receivers):
            del self._update_signals[namespace]

    @synchronized(_lock)
    def signal_update(self, namespace=None):
        namespace = self._namespace if namespace is None else namespace
        if namespace not in self._update_signals:
            return
        self._update_signals[namespace].send(self._configs[namespace])

    @contextmanager
    @synchronized(_lock)
    def namespace(self, namespace):
        previous_namespace = self._namespace
        self._namespace = namespace
        try:
            yield
        finally:
            self._namespace = previous_namespace


class CurrentConfigAttr(object):
    _lock = threading.RLock()

    def __init__(self, namespace=None):
        self._namespace = namespace
        self._config = ConfigManager().get_config(self._namespace)
        ConfigManager().register_update_callback(self._update_config,
                                                 self._namespace)

    @synchronized(_lock)
    def _update_config(self, config):
        self._config = config

    @synchronized(_lock)
    def __get__(self, obj, type=None):
        return self._config

