from __future__ import print_function, division, absolute_import

import datetime
import sys

PY2 = sys.version_info.major == 2
PY3 = not PY2

if PY2:
    import os
    import types
    from urlparse import urlparse, urlsplit  # noqa
    unicode = unicode  # noqa
    string = basestring  # noqa
    integer = (int, long)  # noqa

    # UTC tzinfo singleton
    UTC = type('utc', (datetime.tzinfo,),
               {'__slots__': (),
                '_offset': datetime.timedelta(0),
                '__reduce__': lambda self: 'UTC',
                'utcoffset': lambda self, dt: self._offset,
                'fromutc': lambda self, dt: dt + self._offset})()

    def add_method(cls):
        def bind(func):
            setattr(cls, func.__name__, types.MethodType(func, None, cls))
            return func
        return bind

    def makedirs(name, mode=0o777, exist_ok=True):
        try:
            os.makedirs(name, mode=mode)
        except OSError:
            if not exist_ok or not os.path.isdir(name):
                raise
else:
    from urllib.parse import urlparse, urlsplit  # noqa
    from os import makedirs  # noqa
    unicode = str
    string = str
    integer = int

    UTC = datetime.timezone.utc

    def add_method(cls):
        def bind(func):
            setattr(cls, func.__name__, func)
            return func
        return bind


def with_metaclass(meta):
    """Create a base class with a metaclass."""
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            return meta(name, (object,), d)
    return type.__new__(metaclass, 'temporary_class', (), {})
