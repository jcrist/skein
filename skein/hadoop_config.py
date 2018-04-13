from __future__ import absolute_import, print_function, division

import getpass
import os
import re
from xml.etree import ElementTree


_var_expansion = re.compile('\$\{(.*?)\}')
_envar_expansion = re.compile('env.([^\W-]*)(:-?|-?)([^\W-]*)')


_defaults = {
             # A few system properties that are defined in java's
             # System.properties. These can be used in hadoop's configuration
             # variable expansion.
             'user.name': getpass.getuser(),
             'user.home': os.path.expanduser('~'),
             'file.separator': os.sep,
             'path.separator': os.pathsep,
             'line.separator': os.linesep,

             # Relevant defaults from core-default.xml
             'hadoop.security.authentication': 'simple',
             'hadoop.http.authentication.type': 'simple',
             'hadoop.http.authentication.simple.anonymous.allowed': 'true',

             # Relevant defaults from hdfs-default.xml
             'dfs.http.policy': 'HTTP_ONLY',
             'dfs.webhdfs.enabled': 'true',
             'dfs.namenode.http-address': '0.0.0.0:50070',
             'dfs.namenode.https-address': '0.0.0.0:50470',

             # Relevant defaults from yarn-default.xml
             'yarn.resourcemanager.hostname': '0.0.0.0',
             'yarn.http.policy': 'HTTP_ONLY',
             'yarn.resourcemanager.webapp.address': '${yarn.resourcemanager.hostname}:8088',  # noqa
             'yarn.resourcemanager.webapp.https.address': '${yarn.resourcemanager.hostname}:8090',  # noqa
             'yarn.scheduler.minimum-allocation-mb': '1024',
             'yarn.scheduler.maximum-allocation-mb': '8192',
             'yarn.scheduler.minimum-allocation-vcores': '1',
             'yarn.scheduler.maximum-allocation-vcores': '4'
             }


class HadoopConfiguration(object):
    _default_files = ('core-site.xml', 'hdfs-site.xml', 'yarn-site.xml')

    def __init__(self):
        self._config = _defaults.copy()

        dirs = self._find_conf_dirs()
        for fil in self._default_files:
            paths = [p for p in (os.path.join(d, fil) for d in dirs)
                     if os.path.exists(p)]
            if not paths:
                locations = '\n'.join('- %s' % d for d in dirs)
                msg = ("Could not find {fil}. Looked in:\n"
                       "{locations}\n\n"
                       "Please verify that HADOOP_CONF_DIR or HADOOP_HOME is "
                       "properly set.").format(fil=fil, locations=locations)
                raise ValueError(msg)
            # Hadoop only uses the first location found
            self.add_config(paths[0])

    def get(self, key):
        value = self._config[key]
        parts = []
        start = 0
        for m in _var_expansion.finditer(value):
            (subkey,) = m.groups()
            left, right = m.span()

            if subkey in self._config:
                subval = self.get(subkey)
            elif subkey.startswith('env.'):
                # Handle environment variable substitution
                ematch = _envar_expansion.match(subkey)
                if ematch:
                    envar, separator, default = ematch.groups()
                    subval = os.environ.get(envar)
                    if subval is None:
                        if separator is not None:
                            subval = default
                        else:
                            emsg = ("Failed to find environment variable %r "
                                    "needed to load configuration key "
                                    "%r" % (envar, key))
                            raise ValueError(emsg)
                    elif not subval and separator == ':-':
                        subval = default
            else:
                emsg = ("Configuration key %r requires variable substitution "
                        "of %r, which wasn't found" % (key, subkey))
                raise ValueError(emsg)

            parts.extend((value[start:left], subval))
            start = right
        else:
            if parts:
                parts.append(value[start:])
                value = ''.join(parts)
                # Cache substitution
                self._config[key] = value
        return value

    def add_config(self, path):
        exc = None
        try:
            tree = ElementTree.parse(path)
            root = tree.getroot()

            config = {}
            for prop in root.findall('property'):
                name = prop.find('name').text
                value = prop.find('value').text
                config[name] = value
        except Exception as e:
            exc = e

        if exc:
            raise ValueError(("Unable to load configuration file at '{path}', "
                              "original error is below:\n\n"
                              "{error}").format(path=path, error=repr(exc)))

        self._config.update(config)

    def _find_conf_dirs(self):
        search_dirs = []

        HADOOP_CONF_DIR = os.environ.get('HADOOP_CONF_DIR')
        HADOOP_HOME = os.environ.get('HADOOP_HOME')

        if HADOOP_CONF_DIR:
            search_dirs.append(HADOOP_CONF_DIR)
        if HADOOP_HOME:
            search_dirs.append(os.path.join(HADOOP_HOME, 'conf'))
            search_dirs.append(os.path.join(HADOOP_HOME, 'etc', 'hadoop'))

        search_dirs.extend(['/etc/hadoop/conf/',
                            '/usr/local/etc/hadoop/conf/',
                            '/usr/local/hadoop/conf/'])
        return search_dirs
