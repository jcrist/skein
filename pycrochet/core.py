from __future__ import print_function, division, absolute_import

import hmac
import json
from base64 import b64encode
from hashlib import sha1, md5
from collections import MutableMapping

import requests
from requests.auth import AuthBase


def ensure_bytes(x):
    if type(x) is not bytes:
        x = x.encode('utf-8')
    return x


class ServerException(Exception):
    pass


class UnauthorizedException(ServerException):
    pass


class CrochetAuth(AuthBase):
    def __init__(self, secret):
        self.secret = secret

    def __call__(self, r):
        method = ensure_bytes(r.method)
        content_type = ensure_bytes(r.headers.get('content-type', b''))
        path = ensure_bytes(r.path_url)
        if r.body is not None:
            body = ensure_bytes(r.body)
            body_md5 = md5(body).digest()
        else:
            body_md5 = b''

        msg = b'\n'.join([method, body_md5, content_type, path])
        mac = hmac.new(self.secret, msg=msg, digestmod=sha1)
        signature = b64encode(mac.digest())

        r.headers['Authorization'] = b'CROCHET %s' % signature
        return r


class Configuration(MutableMapping):
    """Represents configuration state stored on the server"""
    def __init__(self, address, auth):
        self._address = address
        self._auth = auth

    def __repr__(self):
        return 'Configuration<address=%s>' % self._address

    def _handle_exceptions(self, resp):
        if resp.status_code == 401:
            raise UnauthorizedException("Provided token rejected by server")
        else:
            raise ServerException("Server responded with unhandled status "
                                  "code %d" % resp.status_code)

    def _check_key(self, key):
        if not len(key):
            raise ValueError("len(key) must be > 0")

    def _list_keys(self):
        url = 'http://%s/keys/' % self._address
        resp = requests.get(url, auth=self._auth)
        if resp.status_code != 200:
            self._handle_exceptions(resp)
        return json.loads(resp.content)['keys']

    def __getitem__(self, key):
        self._check_key(key)
        url = 'http://%s/keys/%s' % (self._address, key)
        resp = requests.get(url, auth=self._auth)
        if resp.status_code == 200:
            return resp.content.decode()
        elif resp.status_code == 404:
            raise KeyError(key)
        else:
            self._handle_exceptions(resp)

    def __setitem__(self, key, value):
        self._check_key(key)
        url = 'http://%s/keys/%s' % (self._address, key)
        resp = requests.put(url, auth=self._auth, data=value)
        if resp.status_code != 204:
            self._handle_exceptions(resp)

    def __delitem__(self, key):
        self._check_key(key)
        url = 'http://%s/keys/%s' % (self._address, key)
        resp = requests.delete(url, auth=self._auth)
        if resp.status_code == 404:
            raise KeyError(key)
        elif resp.status_code != 204:
            self._handle_exceptions(resp)

    def __iter__(self):
        return iter(self._list_keys())

    def __len__(self):
        return len(self._list_keys())


class Client(object):
    def __init__(self, address, secret=None):
        self.address = address
        self._auth = CrochetAuth(secret)
        self.configuration = Configuration(self.address, self._auth)

    def __repr__(self):
        return 'Client<address=%s>' % self.address
