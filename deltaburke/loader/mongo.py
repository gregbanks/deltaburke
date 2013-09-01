import re

import pymongo

from . import Loader, ConfigNotFoundError


class MongoLoader(Loader):
    """ Mongo config loader class

    NOTE: the URI expected here extends the format defined by 10gen. it adds
          (and requires) a collection and id field to the path element.

          mongodb://[username:password@]host1[:port1][...[,hostN[:portN]]]/database/collection/id[?options]
    """
    @staticmethod
    def _load(parts):
        path = ''
        query = ''
        try:
            path = parts.path.split('?')[0]
            query = parts.path.split('?')[1]
        except IndexError:
            pass
        try:
            database, collection, _id = filter(lambda x: len(x) > 0,
                                               path.split('/'))
        except ValueError:
            raise ValueError('invalid path %s. database, collection, and id '
                             'are required')
        uri = 'mongodb://%s/%s%s' % (parts.netloc,
                                     database,
                                     '' if query == '' else '?%s' % (query))
        client = pymongo.MongoClient(uri)
        config = client.get_default_database()[collection].find_one(
                                                            {'_id': _id})
        if config is None and re.match(r'\d*', _id):
            config = \
                client.get_default_database()[collection].find_one(
                                                            {'_id': int(_id)})
        if config is None:
            raise ConfigNotFoundError(uri)
        return config

Loader.register_scheme('mongodb', MongoLoader)

