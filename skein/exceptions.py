import warnings
import sys
from contextlib import contextmanager

__all__ = ('FileExistsError',    # py2 compat
           'FileNotFoundError',  # py2 compat
           'SkeinError',
           'ConnectionError',
           'TimeoutError',
           'DriverNotRunningError',
           'ApplicationNotRunningError',
           'DriverError',
           'ApplicationError')


FileExistsError = FileExistsError
FileNotFoundError = FileNotFoundError


class SkeinError(Exception):
    """Base class for Skein specific exceptions"""


class ConnectionError(SkeinError, ConnectionError):
    """Failed to connect to the driver or application master"""


class TimeoutError(SkeinError, TimeoutError):
    """Request to driver or application master timed out"""


class DriverNotRunningError(ConnectionError):
    """The driver process is not currently running"""


class ApplicationNotRunningError(ConnectionError):
    """The application master is not currently running"""


class DriverError(SkeinError):
    """Internal exceptions from the driver"""


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

        setattr(cls, name, wrap)


for exc in [ValueError, KeyError, TypeError, FileExistsError,
            FileNotFoundError]:
    _Context.register_wrapper(exc)


context = _Context()
