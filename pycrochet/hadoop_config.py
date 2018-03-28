from __future__ import absolute_import, print_function, division

import os
from xml.etree import ElementTree


class HadoopConfiguration(object):
    _default_files = ('core-site.xml', 'hdfs-site.xml', 'yarn-site.xml')

    def __init__(self):
        self._config = {}

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

        search_dirs.extend(['/etc/hadoop/conf/',
                            '/usr/local/etc/hadoop/conf/',
                            '/usr/local/hadoop/conf/'])
        return search_dirs
