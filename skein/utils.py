from __future__ import print_function, division, absolute_import

from .compatibility import urlparse


def ensure_bytes(x):
    if type(x) is not bytes:
        x = x.encode('utf-8')
    return x


def normalize_address(addr):
    p = urlparse(addr)
    if p.scheme:
        address = p.netloc
    else:
        address = p.path
    return 'http://%s' % address


class cached_property(object):

    def __init__(self, func):
        self.__doc__ = getattr(func, "__doc__")
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self

        res = obj.__dict__[self.func.__name__] = self.func(obj)
        return res
