import os

from unittest import TestCase

from deltaburke.loader import Loader, ConfigNotFoundError

data_path = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                         '..',
                                         'data'))


class TestFileLoader(TestCase):
    def setUp(self):
        self.data = {'a': 1,
                     'b': 2,
                     'c': [3, 4, {'d': 'e'}],
                     'f': {'g': {'h': 5}}}

    def test_load_missing(self):
        self.assertRaises(ConfigNotFoundError,
                          Loader.load,
                          'file://%s' %
                          (os.path.join(data_path,
                           'missing.yml')))

    def test_load_yaml(self):
        data = Loader.load('file://%s' %
                           (os.path.join(data_path,
                            'loader_test.yml')))
        self.assertEqual(data, self.data)

    def test_load_json(self):
        data = Loader.load('file://%s' %
                           (os.path.join(data_path,
                            'loader_test.json')))
        self.assertEqual(data, self.data)

