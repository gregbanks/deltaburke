import os
import threading

from json import dumps
from tempfile import mkstemp
from unittest import TestCase

from mock import MagicMock

from deltaburke.monitor import (
    SourceMonitor, FileSourceMonitor, MongoSourceMonitor
)


class ConfigManagerMock(MagicMock):
    def __init__(self, *args, **kwargs):
        super(ConfigManagerMock, self).__init__(*args, **kwargs)
        self.merge_event = threading.Event()

    def merge(self, *args, **kwargs):
        self.merge_event.set()


class TestSourceMonitor(TestCase):
    def setUp(self):
        self._data = {'a': 'b', 'c': {'d': 'e'}}
        self._fd, self._path = mkstemp()
        self._config_manager = ConfigManagerMock()

        os.fdopen(self._fd, 'w').write(dumps(self._data))

    def tearDown(self):
        os.unlink(self._path)


class TestThreadingSourceMonitor(TestSourceMonitor):
    def test_stop(self):
        monitor = FileSourceMonitor(self._config_manager,
                                    'file:///%s' % (self._path),
                                    SourceMonitor.hash(self._data),
                                    poll_interval=.1)
        self.assertFalse(monitor.is_alive())
        monitor.start()
        self.assertTrue(monitor.is_alive())
        monitor.stop()
        self.assertFalse(monitor.is_alive())


class TestFileSourceMonitor(TestSourceMonitor):
    def test_file_change(self):
        monitor = FileSourceMonitor(self._config_manager,
                                    'file:///%s' % (self._path),
                                    SourceMonitor.hash(self._data),
                                    poll_interval=.2)
        monitor.start()
        try:
            config = open(self._path, 'w')
            config.write(dumps({'a': 'b', 'c': {'d': 'f'}}))
            config.close()
            self._config_manager.merge_event.wait(1)
            self.assertTrue(self._config_manager.merge_event.is_set())
        finally:
            monitor.stop()

