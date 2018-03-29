from __future__ import absolute_import, print_function, division

import requests

from .exceptions import UnauthorizedError, ResourceManagerError
from .hadoop_config import HadoopConfiguration


def _get_or_raise(config, parameter, key):
    try:
        return config.get(key)
    except (KeyError, ValueError):
        raise ValueError("Failed to infer value of %r from configuration "
                         "files, please pass this parameter in "
                         "explicitly" % parameter)


class SimpleAuth(requests.auth.AuthBase):
    """Implement simple authentication for the yarn REST api"""
    def __init__(self, user):
        self.user = user

    def __call__(self, r):
        r.url += '?user.name={user}'.format(user=self.user)
        return r


class Client(object):
    def __init__(self, address=None, user=None, auth=None):

        config_params = [address, user, auth]
        if any(p is None for p in config_params):
            # Some parameters are missing, autoconfigure
            config = HadoopConfiguration()
            if address is None:
                address = _get_or_raise(config, 'address',
                                        'yarn.resourcemanager.webapp.address')
            if user is None:
                user = config.get('user.name')

            if auth is None:
                auth = _get_or_raise(config, 'auth',
                                     'hadoop.http.authentication.type')

        if auth == 'simple':
            auth = SimpleAuth(user)
        elif auth == 'kerberos':
            from requests_kerberos import HTTPKerberosAuth
            auth = HTTPKerberosAuth()

        self.address = address
        self.user = user
        self.auth = auth
        self._template = 'http://{address}/ws/v1/{path}'
        self._cookies = None

    def _handle_exceptions(self, resp):
        if resp.status_code == 401:
            raise UnauthorizedError("Failed to authenticate with "
                                    "ApplicationMaster")
        else:
            raise ResourceManagerError("ApplicationMaster responded with an "
                                       "unhandled status code: "
                                       "%d" % resp.status_code)

    def _get(self, path):
        url = self._template.format(address=self.address, path=path)
        resp = requests.get(url, cookies=self._cookies, auth=self.auth)
        if resp.cookies:
            self._cookies = resp.cookies
        return resp

    def info(self):
        resp = self._get('cluster/info')
        if resp.status_code == 200:
            return resp.json()['clusterInfo']
        self._handle_exceptions(resp)

    def metrics(self):
        resp = self._get('cluster/metrics')
        if resp.status_code == 200:
            return resp.json()['clusterMetrics']
        self._handle_exceptions(resp)
