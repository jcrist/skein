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
