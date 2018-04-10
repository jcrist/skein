from __future__ import absolute_import, print_function, division

import requests

from .compatibility import urlparse
from .exceptions import UnauthorizedError, ResourceManagerError
from .hadoop_config import HadoopConfiguration
from .utils import normalize_address


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
        sym = '&' if '?' in r.url else '?'
        r.url += '{sym}user.name={user}'.format(sym=sym, user=self.user)
        return r


class Client(object):
    def __init__(self, rm_address=None, webhdfs_address=None,
                 user=None, auth=None):

        config_params = [rm_address, webhdfs_address, user, auth]
        if any(p is None for p in config_params):
            # Some parameters are missing, autoconfigure
            config = HadoopConfiguration()
            if rm_address is None:
                rm_address = _get_or_raise(config, 'rm_address',
                                           'yarn.resourcemanager.webapp.address')
            if webhdfs_address is None:
                webhdfs = _get_or_raise(config, 'webhdfs_address',
                                        'dfs.namenode.http-address')
                port = urlparse(normalize_address(webhdfs)).port
                address = _get_or_raise(config, 'webhdfs_address',
                                        'fs.defaultFS')
                host = urlparse(normalize_address(address)).hostname
                webhdfs_address = '%s:%s' % (host, port)

            if user is None:
                user = config.get('user.name')

            if auth is None:
                auth = _get_or_raise(config, 'auth',
                                     'hadoop.http.authentication.type')

        self._is_kerberized = auth == 'kerberos'

        if auth == 'simple':
            auth = SimpleAuth(user)
        elif auth == 'kerberos':
            from requests_kerberos import HTTPKerberosAuth
            auth = HTTPKerberosAuth()

        self._rm_address = rm_address
        self._rm_template = 'http://%s/ws/v1/' % rm_address
        self._rm = requests.Session()
        self._rm.auth = auth

        self._webhdfs_address = webhdfs_address
        self._webhdfs_template = 'http://%s/webhdfs/v1/' % webhdfs_address
        self._webhdfs = requests.Session()
        self._webhdfs.auth = auth

    def close(self):
        self._webhdfs.close()
        self._rm.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def _handle_exceptions(self, resp):
        if resp.status_code == 401:
            raise UnauthorizedError("Failed to authenticate with Server")
        else:
            raise ResourceManagerError("Server responded with an unhandled "
                                       "status code: %d" % resp.status_code)

    def info(self):
        resp = self._rm.get(self._rm_template + 'cluster/info')
        if resp.status_code == 200:
            return resp.json()['clusterInfo']
        self._handle_exceptions(resp)

    def metrics(self):
        resp = self._rm.get(self._rm_template + 'cluster/metrics')
        if resp.status_code == 200:
            return resp.json()['clusterMetrics']
        self._handle_exceptions(resp)

    def _new_application(self):
        resp = self._rm.post(self._rm_template + 'cluster/apps/new-application')
        if resp.status_code == 200:
            blob = resp.json()
            appid = blob['application-id']
            limits = blob['maximum-resource-capability']
        else:
            self._handle_exceptions(resp)
        return appid, limits

    def _submit_application(self, spec, resource, queue='default',
                            appname='crochet', priority=0, max_attempts=2,
                            tags=None):
        appid, _ = self._new_application()

        msg = {'application-id': appid,
               'application-name': appname,
               'queue': queue,
               'priority': priority,
               'am-container-spec': spec,
               'unmanaged-AM': False,
               'max-app-attempts': max_attempts,
               'resource': resource,
               'application-type': 'crochet',
               'keep-containers-across-application-attempts': False}

        resp = self._rm.post(self._rm_template + 'cluster/apps', json=msg)
        if resp.status_code != 202:
            self._handle_exceptions(resp)
        return appid

    def _hdfs_status(self, path):
        resp = self._webhdfs.get(self._webhdfs_template + path.lstrip('/'),
                                 params={'op': 'GETFILESTATUS'})
        if resp.status_code == 200:
            return resp.json()['FileStatus']
        self._handle_exceptions(resp)

    def _hdfs_get_delegation_token(self):
        resp = self._webhdfs.get(self._webhdfs_template,
                                 params={'op': 'GETDELEGATIONTOKEN',
                                         'renewer': 'yarn/master.example.com'})
        if resp.status_code == 200:
            token = resp.json()['Token']
            return None if token is None else token['urlString']
        self._handle_exceptions(resp)

    def _yarn_get_delegation_token(self):
        resp = self._rm.post(self._rm_template + 'cluster/delegation-token',
                             json={'renewer': 'yarn'})
        if resp.status_code == 200:
            return resp.json()['token']
        self._handle_exceptions(resp)

    def _build_local_resources(self, paths):
        entries = []
        for target, source in paths.items():
            parsed = urlparse(source)
            assert parsed.scheme == 'hdfs'
            status = self._hdfs_status(parsed.path)
            entries.append({'key': target,
                            'value': {'resource': source,
                                      'type': 'FILE',
                                      'visibility': 'APPLICATION',
                                      'size': status['length'],
                                      'timestamp': status['modificationTime']}})

        return {'entry': entries}

    def start(self):
        command = ("yarn jar crochet.jar "
                   "1> <LOG_DIR>/appmaster.stdout "
                   "2> <LOG_DIR>/appmaster.stderr")
        commands = {'command': command}

        resource = {'memory': 1024, 'vCores': 1}

        am_path = ('hdfs://master.example.com:9000/user/testuser/'
                   'crochet-1.0-SNAPSHOT-jar-with-dependencies.jar')
        local_resources = self._build_local_resources({'crochet.jar': am_path})

        env = {'entry': [{'key': 'CROCHET_SECRET_ACCESS_KEY',
                          'value': 'foobar'}]}

        if self._is_kerberized:
            tokens = [{'key': 'hdfs',
                       'value': self._hdfs_get_delegation_token()}]
            credentials = {'tokens': tokens,
                           'secrets': tokens}
        else:
            credentials = None

        spec = {'local-resources': local_resources,
                'environment': env,
                'commands': commands,
                'credentials': credentials}

        return self._submit_application(spec, resource)
