import hashlib
import select
import threading
import urlparse

from abc import ABCMeta, abstractmethod
from functools import partial
from json import dumps

import inotify.watcher as file_watcher

from inotify import IN_CLOSE_WRITE
from robustify.robustify import retry_till_done

from loader import Loader

POLL_INTERVAL = 60


class SourceMonitor(object):
    __metaclass__ = ABCMeta

    def __init__(self, manager, source, hash_, namespace=None,
                       poll_interval=POLL_INTERVAL):
        self._manager = manager
        self._source = source
        self._hash = hash_
        self._namespace = namespace
        self._pol_interval = poll_interval
        self._stop = None
        self._monitor_thread = None

    def is_alive(self):
        if self._monitor_thread is not None:
            return self._monitor_thread.is_alive()
        return False

    @abstractmethod
    def monitor(self):
        pass

    @staticmethod
    def hash(data):
        return hashlib.md5(dumps(data)).hexdigest()

    def start(self, how='threading'):
        if not self.is_alive():
            if how == 'threading':
                self._stop = threading.Event()
                self._monitor_thread = threading.Thread(target=self.monitor)
                self._monitor_thread.daemon = True
                self._monitor_thread.start()
            else:
                raise ValueError(how)

    def stop(self):
        if self.is_alive():
            self._stop.set()
            self._monitor_thread.join()
        self._monitor_thread = None

    @staticmethod
    def monitor(manager, source, data, namespace=None,
                poll_interval=POLL_INTERVAL):
        scheme = urlparse.urlparse(source).scheme
        cls = None
        if scheme == 'file':
            cls = FileSourceMonitor
        elif scheme == 'mongodb':
            cls = MongoSourceMonitor
        else:
            raise ValueError(scheme)
        monitor = cls(manager, source, SourceMonitor.hash(data), namespace,
                      poll_interval)
        monitor.start()
        return monitor


class FileSourceMonitor(SourceMonitor):
    def __init__(self, manager, source, hash_, namespace=None,
                       poll_interval=POLL_INTERVAL):
        super(FileSourceMonitor, self).__init__(manager, source, hash_,
                                                namespace, poll_interval)
        assert(source.startswith('file://'))
        self._watcher = file_watcher.Watcher()
        self._source = source
        self._watcher.add(urlparse.urlparse(source).path,
                          IN_CLOSE_WRITE)

    def monitor(self):
        while not self._stop.is_set():
            rlist, _, _ = select.select([self._watcher.fileno()], [], [], 1)
            if self._watcher.fileno() in rlist:
                try:
                    data = retry_till_done(partial(Loader.load, self._source),
                                           max_wait_in_secs=2, 
                                           retry_interval=.2)
                    hash_ = self.hash(data)
                    if hash_ != self._hash:
                        self._hash = hash_
                        self._manager.merge(data, True, self._namespace)
                except ValueError:
                    raise
                except Exception:
                    import traceback
                    traceback.print_exc()
                finally:
                    self._watcher.read()
                    self._watcher.add(urlparse.urlparse(self._source).path,
                                      IN_CLOSE_WRITE)


class MongoSourceMonitor(SourceMonitor):
    def monitor(self):
        raise NotImplementedError()


