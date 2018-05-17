from __future__ import print_function, division, absolute_import

import warnings
import sys
from contextlib import contextmanager

import grpc

from .compatibility import (PY2, ConnectionError, FileExistsError,
                            FileNotFoundError)


class SkeinError(Exception):
    """Base class for exceptions that are handled nicely by the CLI"""
    def with_cli_message(self, cli_message=None):
        if cli_message is not None:
            self._cli_message = cli_message
        return self

    def cli_message(self):
        return getattr(self, '_cli_message', None) or str(self)


class DaemonError(SkeinError):
    """Exceptions raised when communicating with the daemon"""
    pass


class ApplicationMasterError(SkeinError):
    """Exceptions raised when communicating with the application master"""
    pass


class NotInitializedError(FileNotFoundError, SkeinError):
    pass


NOT_INITIALIZED = (NotInitializedError(
    "Skein global configuration directory is not initialized. Please run "
    "either ``Security.from_new_key_pair()`` or ``skein init``.")
    .with_cli_message(
    "Skein global configuration directory is not initialized. Please run "
    "``skein init``."))


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

        def wrap(self, msg, cli=None):
            if not self.is_cli:
                return typ(msg)
            return typ2(msg).with_cli_message(cli)

        if PY2:
            import types
            setattr(cls, name, types.MethodType(wrap, None, cls))
        else:
            setattr(cls, name, wrap)


for exc in [ValueError, TypeError, ConnectionError, FileExistsError,
            FileNotFoundError]:
    _Context.register_wrapper(exc)


context = _Context()


@contextmanager
def convert_errors(daemon=True):
    exc = None
    try:
        yield
    except grpc.RpcError as _exc:
        exc = _exc

    if exc is not None:
        code = exc.code()
        if code == grpc.StatusCode.UNAVAILABLE:
            server_name = 'daemon' if daemon else 'application master'
            raise ConnectionError("Unable to connect to %s" % server_name)
        elif code in (grpc.StatusCode.INVALID_ARGUMENT,
                      grpc.StatusCode.NOT_FOUND,
                      grpc.StatusCode.ALREADY_EXISTS):
            raise context.ValueError(exc.details())
        else:
            cls = DaemonError if daemon else ApplicationMasterError
            raise cls(exc.details())
