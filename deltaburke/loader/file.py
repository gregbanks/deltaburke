import json
import yaml

from . import ConfigNotFoundError, Loader


class FileLoader(Loader):
    @staticmethod
    def _load(parts):
        try:
            config = open(parts.path).read()
        except IOError:
            raise ConfigNotFoundError('file://%s' % (parts.path))
        if parts.path.endswith(('.yml', '.yaml')):
            return yaml.load(config)
        return json.loads(config)

Loader.register_scheme('file', FileLoader)

