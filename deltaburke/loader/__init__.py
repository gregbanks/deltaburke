from abc import ABCMeta, abstractmethod
from urlparse import urlparse


class ConfigNotFoundError(Exception):
    def __init__(self, location):
        msg = 'no config found at %s' % (location)
        super(ConfigNotFoundError, self).__init__(msg)


class Loader(object):
    __metaclass__ = ABCMeta

    _schemes = {}

    @classmethod
    def register_scheme(cls, scheme, klass):
        cls._schemes[scheme] = klass

    @staticmethod
    @abstractmethod
    def _load(cls, parsed_url):
        pass

    @classmethod
    def load(cls, src):
        parts = urlparse(src)
        loader = cls._schemes.get(parts.scheme, None)
        if loader is None:
            raise ValueError('no loader registered for scheme "%s"' %
                             (parts.scheme))
        return loader._load(parts)

from .file import FileLoader
from .mongo import MongoLoader

