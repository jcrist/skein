# flake8: noqa
from __future__ import print_function, division, absolute_import

import sys

PY2 = sys.version_info.major == 2
PY3 = not PY2

if PY2:
    from urlparse import urlparse, urlsplit

    class ConnectionError(OSError):
        pass

    class FileExistsError(OSError):
        pass

    class FileNotFoundError(OSError):
        pass

else:
    from urllib.parse import urlparse, urlsplit
    ConnectionError = ConnectionError
    FileExistsError = FileExistsError
    FileNotFoundError = FileNotFoundError


def with_metaclass(meta):
    """Create a base class with a metaclass."""
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            return meta(name, (object,), d)
    return type.__new__(metaclass, 'temporary_class', (), {})
