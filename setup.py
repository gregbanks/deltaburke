import os
import re
import sys

from functools import partial
from tempfile import mkstemp

from setuptools import setup, find_packages


# NOTE: http://bugs.python.org/issue15881#msg170215
try:
    import multiprocessing
except ImportError:
    pass


def get_version(path):
    version = None
    try:
        version =  re.search(r'__version__\s*=\s*[\'"]([\d.]+)[\'"]',
                             open(path).read()).group(1)
    except (IOError, AttributeError):
        pass
    return version


get_path = partial(os.path.join,  os.path.dirname(os.path.abspath(__file__)))


reqs_path = get_path('requirements.txt')
tmp_reqs_path = None

# HACK
if 'linux' in sys.platform:
    tmp_reqs_file, tmp_reqs_path = mkstemp()
    tmp_reqs_file = os.fdopen(tmp_reqs_file, 'w')
    for line in open(reqs_path):
        if 'inotify' in line:
            line = line.strip('#')
        tmp_reqs_file.write(line)
    tmp_reqs_file.close()
    reqs_path = tmp_reqs_path

try:
    setup(name='deltaburke',
          author='Greg Banks',
          author_email='quaid@kuatowares.com',
          description='stylish configs',
          setup_requires=['rexparse'],
          dependency_links=['https://github.com/gregbanks/rexparse/archive/master.zip#egg=rexparse'],
          rexparse={'requirements_path': reqs_path},
          version=get_version(get_path('deltaburke/_version.py')),
          test_suite='nose.collector',
          packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*",
                                          "tests"]))
finally:
    if tmp_reqs_path is not None:
        os.unlink(tmp_reqs_path)
