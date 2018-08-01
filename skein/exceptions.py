from __future__ import print_function, division, absolute_import

import warnings
import sys
from contextlib import contextmanager

from .compatibility import PY2, bind_method

__all__ = ('FileExistsError',    # py2 compat
           'FileNotFoundError',  # py2 compat
           'SkeinError',
           'ConnectionError',
           'TimeoutError',
           'DaemonNotRunningError',
           'ApplicationNotRunningError',
           'DaemonError',
           'ApplicationError')


if PY2:
    class _ConnectionError(OSError):
        pass

    class _TimeoutError(OSError):
        pass

    class FileExistsError(OSError):
        pass

    class FileNotFoundError(OSError):
        pass

else:
    _ConnectionError = ConnectionError  # noqa
    _TimeoutError = TimeoutError  # noqa
    FileExistsError = FileExistsError
    FileNotFoundError = FileNotFoundError


class SkeinError(Exception):
    """Base class for Skein specific exceptions"""


class ConnectionError(SkeinError, _ConnectionError):
    """Failed to connect to the daemon or application master"""


class TimeoutError(SkeinError, _TimeoutError):
    """Request to daemon or application master timed out"""


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

        bind_method(cls, name, wrap)


for exc in [ValueError, KeyError, TypeError, FileExistsError,
            FileNotFoundError]:
    _Context.register_wrapper(exc)


context = _Context()
