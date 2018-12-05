from __future__ import print_function, division, absolute_import

import datetime
import sys

PY2 = sys.version_info.major == 2
PY3 = not PY2

if PY2:
    import os
    import re
    import types
    from math import ceil as _ceil
    from urlparse import urlparse, urlsplit  # noqa
    from Queue import Queue  # noqa
    from collections import Mapping, MutableMapping  # noqa
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

    def makedirs(name, mode=0o777, exist_ok=True):
        try:
            os.makedirs(name, mode=mode)
        except OSError:
            if not exist_ok or not os.path.isdir(name):
                raise

    def read_stdin_bytes():
        return sys.stdin.read()

    def write_stdout_bytes(b):
        sys.stdout.write(b)

    def bind_method(cls, name, func):
        setattr(cls, name, types.MethodType(func, None, cls))

    _name_re = re.compile(r"[a-zA-Z_][a-zA-Z0-9_]*$")

    def isidentifier(s):
        return bool(_name_re.match(s))

    def math_ceil(x):
        return int(_ceil(x))
else:
    from math import ceil as math_ceil  # noqa
    from urllib.parse import urlparse, urlsplit  # noqa
    from os import makedirs  # noqa
    from queue import Queue  # noqa
    from collections.abc import Mapping, MutableMapping  # noqa
    unicode = str
    string = str
    integer = int

    UTC = datetime.timezone.utc

    def read_stdin_bytes():
        return sys.stdin.buffer.read()

    def write_stdout_bytes(b):
        sys.stdout.buffer.write(b)

    def bind_method(cls, name, func):
        setattr(cls, name, func)

    def isidentifier(s):
        return s.isidentifier()


def with_metaclass(meta):
    """Create a base class with a metaclass."""
    class metaclass(type):
        def __new__(cls, name, this_bases, d):
            return meta(name, (object,), d)
    return type.__new__(metaclass, 'temporary_class', (), {})
