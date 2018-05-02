from __future__ import print_function, division, absolute_import

import os

import yaml

from .compatibility import urlparse
from .hadoop_config import HadoopConfiguration


_SECRET_ENV_VAR = 'SKEIN_SECRET_ACCESS_KEY'
_ADDRESS_ENV_VAR = 'SKEIN_APPMASTER_ADDRESS'


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


def _flatten(d):
    if type(d) is dict:
        out = {}
        for k, v in d.items():
            if type(v) is dict:
                out2 = _flatten({'.'.join((k, k2)): v2 for k2, v2 in v.items()})
                out.update(out2)
            else:
                out[k] = v
        return out
    else:
        return d


def load_config():
    path = os.sep.join([os.path.expanduser('~'), '.skein', 'config.yaml'])
    if os.path.exists(path):
        with open(path) as fil:
            out = _flatten(yaml.load(fil))
    else:
        out = {}

    secret_key = 'skein.secret'
    rest_key = 'resourcemanager.rest.address'
    keys = {secret_key, rest_key}

    extra = set(out).difference(keys)
    if False:
        raise ValueError("Unknown configuration keys:\n"
                         "%s" % '\n'.join("- %r" % k for k in extra))

    config = HadoopConfiguration()

    if rest_key not in out:
        if config.get('yarn.http.policy') == 'HTTP_ONLY':
            key = 'yarn.resourcemanager.webapp.address'
            scheme = 'http'
        else:
            key = 'yarn.resourcemanager.webapp.https.address'
            scheme = 'https'
        out[rest_key] = normalize_address(config.get(key), scheme)

    out['hadoop.security'] = config.get('hadoop.http.authentication.type')

    if secret_key not in out:
        secret = os.environ.get(_SECRET_ENV_VAR)
        if secret is None:
            raise ValueError("Secret key not found in config file or "
                             "%r envar" % _SECRET_ENV_VAR)
        out[secret_key] = secret

    return out
