from unittest import TestCase

from pymongo import MongoClient

from deltaburke.loader import Loader, ConfigNotFoundError


class TestMongoLoader(TestCase):
    TEST_DB = 'deltaburke_test'

    def setUp(self):
        client = MongoClient()
        client.drop_database(self.__class__.TEST_DB)
        self.db = client[self.__class__.TEST_DB]
        self.db.foo.insert([{'_id': 1,
                             'a': 1,
                             'b': 2,
                             'c': [3, 4, {'d': 'e'}],
                             'f': {'g': {'h': 5}}},
                            {'_id': 2,
                             'b': {'x': 2}},
                            {'_id': 3,
                             'f': {'g': 5}}])

    def test_invalid_uri(self):
        self.assertRaises(
            ValueError,
            Loader.load,
            'mongodb://localhost/%s/foo' % (self.__class__.TEST_DB))

    def test_load_missing_config(self):
        self.assertRaises(
            ConfigNotFoundError,
            Loader.load,
            'mongodb://localhost/%s/foo/4' % (self.__class__.TEST_DB))

    def test_load_config(self):
        config = Loader.load('mongodb://localhost/%s/foo/1' %
                             (self.__class__.TEST_DB))
        self.assertEqual(config, {'_id': 1,
                                  'a': 1,
                                  'b': 2,
                                  'c': [3, 4, {'d': 'e'}],
                                  'f': {'g': {'h': 5}}})
