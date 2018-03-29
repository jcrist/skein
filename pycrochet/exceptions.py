from __future__ import absolute_import, print_function, division


class RequestError(IOError):
    """Base class for all exceptions that result from communicating with the
    various servers"""


class UnauthorizedError(RequestError):
    """Authentication/Authorization failed for the request"""


class ApplicationMasterError(RequestError):
    """An exception occurred when communicating with the application master"""


class ResourceManagerError(RequestError):
    """An exception occurred when communicating with the resource manager"""
