from __future__ import print_function, division, absolute_import

import errno
import json
import os

from .compatibility import urlparse


SECRET_ENV_VAR = 'SKEIN_SECRET_ACCESS_KEY'
ADDRESS_ENV_VAR = 'SKEIN_APPMASTER_ADDRESS'

CONFIG_PATH = os.path.join(os.path.expanduser('~'), '.skein')
SECRET_PATH = os.path.join(CONFIG_PATH, 'secret')
DAEMON_PATH = os.path.join(CONFIG_PATH, 'daemon')


def ensure_bytes(x):
    if type(x) is not bytes:
        x = x.encode('utf-8')
    return x


def normalize_address(addr, scheme='http'):
    p = urlparse(addr)
    address = p.netloc if p.scheme else p.path
    return '%s://%s' % (scheme, address)


class cached_property(object):

    def __init__(self, func):
        self.__doc__ = getattr(func, "__doc__")
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self

        res = obj.__dict__[self.func.__name__] = self.func(obj)
        return res


def implements(f):
    def decorator(g):
        g.__doc__ = f.__doc__
        return g
    return decorator


def read_secret():
    try:
        with open(SECRET_PATH) as fil:
            secret = fil.read()
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise

        secret = os.environ.get(SECRET_ENV_VAR)
        if secret is None:
            raise ValueError("Secret key not found in config file or "
                             "%r envar" % SECRET_ENV_VAR)
    return secret


def read_daemon():
    try:
        with open(DAEMON_PATH, 'r') as fil:
            data = json.load(fil)
            address = data['address']
            pid = data['pid']
    except Exception:
        address = pid = None
    return address, pid


def write_daemon(address, pid):
    # Ensure the config dir exists
    os.makedirs(CONFIG_PATH, exist_ok=True)
    # Write to the daemon file
    with open(DAEMON_PATH, 'w') as fil:
        json.dump({'address': address, 'pid': pid}, fil)
