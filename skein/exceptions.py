from __future__ import print_function, division, absolute_import

import warnings
import sys
from contextlib import contextmanager

from .compatibility import PY2

__all__ = ('FileExistsError',    # py2 compat
           'FileNotFoundError',  # py2 compat
           'SkeinError',
           'SkeinConfigurationError',
           'ConnectionError',
           'DaemonNotRunningError',
           'ApplicationNotRunningError',
           'DaemonError',
           'ApplicationError')


if PY2:
    class _ConnectionError(OSError):
        pass

    class FileExistsError(OSError):
        pass

    class FileNotFoundError(OSError):
        pass

else:
    _ConnectionError = ConnectionError
    FileExistsError = FileExistsError
    FileNotFoundError = FileNotFoundError


class SkeinError(Exception):
    """Base class for Skein specific exceptions"""


class SkeinConfigurationError(SkeinError, FileNotFoundError):
    """Skein configuration was not found"""


class ConnectionError(SkeinError, _ConnectionError):
    """Failed to connect to the daemon or application master"""


class DaemonNotRunningError(ConnectionError):
    """The daemon process is not currently running"""


class ApplicationNotRunningError(ConnectionError):
    """The application master is not currently running"""


class DaemonError(SkeinError):
    """Internal exceptions from the daemon"""


class ApplicationError(SkeinError):
    """Internal exceptions from the application master"""


class _Context(object):
    def __init__(self):
        self.is_cli = False

    def info(self, msg):
        print(msg)

    def warn(self, msg):
        if self.is_cli:
            print(msg + "\n", file=sys.stderr)
        else:
            warnings.warn(msg)

    @contextmanager
    def set_cli(self):
        old = self.is_cli
        self.is_cli = True
        yield
        self.is_cli = old

    @classmethod
    def register_wrapper(cls, typ):
        name = typ.__name__
        typ2 = type(name, (typ, SkeinError), {})

        def wrap(self, msg):
            return typ2(msg) if self.is_cli else typ(msg)

        if PY2:
            import types
            setattr(cls, name, types.MethodType(wrap, None, cls))
        else:
            setattr(cls, name, wrap)


for exc in [ValueError, TypeError, FileExistsError, FileNotFoundError]:
    _Context.register_wrapper(exc)


context = _Context()
