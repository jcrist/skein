import os
import re
import subprocess
import sys
import pkg_resources
from distutils.command.build import build as _build
from distutils.command.clean import clean as _clean
from distutils.dir_util import remove_tree
from glob import glob

from setuptools import setup, Command
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install

import versioneer

ROOT_DIR = os.path.abspath(os.path.dirname(os.path.relpath(__file__)))
JAVA_DIR = os.path.join(ROOT_DIR, 'java')
JAVA_TARGET_DIR = os.path.join(JAVA_DIR, 'target')
JAVA_PROTO_DIR = os.path.join(ROOT_DIR, "java", "src", "main", "proto")
SKEIN_JAVA_DIR = os.path.join(ROOT_DIR, 'skein', 'java')
SKEIN_JAR = os.path.join(SKEIN_JAVA_DIR, 'skein.jar')
SKEIN_PROTO_DIR = os.path.join(ROOT_DIR, 'skein', 'proto')


class build_proto(Command):
    description = "build protobuf artifacts"

    user_options = []

    def initialize_options(self):
        pass

    finalize_options = initialize_options

    def _fix_imports(self, path):
        new = ['from __future__ import absolute_import']
        with open(path) as fil:
            for line in fil:
                if re.match("^import [^ ]*_pb2 as [^ ]*$", line):
                    line = 'from . ' + line
                new.append(line)

        with open(path, 'w') as fil:
            fil.write(''.join(new))

    def run(self):
        from grpc_tools import protoc
        include = pkg_resources.resource_filename('grpc_tools', '_proto')
        for src in glob(os.path.join(JAVA_PROTO_DIR, "*.proto")):
            command = ['grpc_tools.protoc',
                       '--proto_path=%s' % JAVA_PROTO_DIR,
                       '--proto_path=%s' % include,
                       '--python_out=%s' % SKEIN_PROTO_DIR,
                       '--grpc_python_out=%s' % SKEIN_PROTO_DIR,
                       src]
            if protoc.main(command) != 0:
                self.warn('Command: `%s` failed'.format(command))
                sys.exit(1)

        for path in _compiled_protos():
            self._fix_imports(path)


class build_java(Command):
    description = "build java artifacts"

    user_options = []

    def initialize_options(self):
        pass

    finalize_options = initialize_options

    def run(self):
        # Compile the java code and copy the jar to skein/java/
        # This will be picked up as package_data later
        self.mkpath(SKEIN_JAVA_DIR)
        code = subprocess.call(['mvn', '-f', os.path.join(JAVA_DIR, 'pom.xml'),
                                '--batch-mode', 'package'])
        if code:
            sys.exit(code)

        jar_files = glob(os.path.join(JAVA_TARGET_DIR, 'skein-*.jar'))
        if not jar_files:
            self.warn('Maven compilation produced no jar files')
            sys.exit(1)
        elif len(jar_files) > 1:
            self.warn('Maven produced multiple jar files')
            sys.exit(1)

        jar = jar_files[0]

        self.copy_file(jar, SKEIN_JAR)


def _ensure_java(command):
    if not getattr(command, 'no_java', False) and not os.path.exists(SKEIN_JAR):
        command.run_command('build_java')


def _compiled_protos():
    return glob(os.path.join(SKEIN_PROTO_DIR, '*_pb2*.py'))


def _ensure_proto(command):
    if not _compiled_protos():
        command.run_command('build_proto')


class build(_build):
    def run(self):
        _ensure_java(self)
        _ensure_proto(self)
        _build.run(self)


class install(_install):
    def run(self):
        _ensure_java(self)
        _ensure_proto(self)
        _install.run(self)


class develop(_develop):
    user_options = list(_develop.user_options)
    user_options.append(('no-java', None, "Don't build the java source"))

    def initialize_options(self):
        self.no_java = False
        _develop.initialize_options(self)

    def run(self):
        if not self.uninstall:
            _ensure_java(self)
            _ensure_proto(self)
        _develop.run(self)


class clean(_clean):
    def run(self):
        if self.all:
            for d in [SKEIN_JAVA_DIR, JAVA_TARGET_DIR]:
                if os.path.exists(d):
                    remove_tree(d, dry_run=self.dry_run)
            for fil in _compiled_protos():
                if not self.dry_run:
                    os.unlink(fil)
        _clean.run(self)


is_build_step = bool({'build', 'install', 'develop',
                      'bdist_wheel'}.intersection(sys.argv))
protos_built = bool(_compiled_protos()) and 'clean' not in sys.argv

if 'build_proto' in sys.argv or (is_build_step and not protos_built):
    setup_requires = ['grpcio-tools']
else:
    setup_requires = []


install_requires = ['grpcio>=1.11.0',
                    'protobuf>=3.5.0',
                    'pyyaml',
                    'cryptography']

# Due to quirks in setuptools/distutils dependency ordering, to get the java
# and protobuf sources to build automatically in most cases, we need to check
# for them in multiple locations. This is unfortunate, but seems necessary.
cmdclass = versioneer.get_cmdclass()
cmdclass.update({'build_java': build_java,    # directly build the java source
                 'build_proto': build_proto,  # directly build the proto source
                 'build': build,              # bdist_wheel or pip install .
                 'install': install,          # python setup.py install
                 'develop': develop,          # python setup.py develop
                 'clean': clean})             # extra cleanup


setup(name='skein',
      version=versioneer.get_version(),
      cmdclass=cmdclass,
      maintainer='Jim Crist',
      maintainer_email='jiminy.crist@gmail.com',
      license='BSD',
      description=('A simple tool and library for deploying applications on '
                   'Apache YARN'),
      long_description=(open('README.rst').read()
                        if os.path.exists('README.rst') else ''),
      url='http://github.com/jcrist/skein/',
      classifiers=["Development Status :: 5 - Production/Stable",
                   "License :: OSI Approved :: BSD License",
                   "Programming Language :: Java",
                   "Programming Language :: Python :: 2.7",
                   "Programming Language :: Python :: 3.5",
                   "Programming Language :: Python :: 3.6",
                   "Programming Language :: Python :: 3.7",
                   "Topic :: Software Development :: Libraries :: Java Libraries",
                   "Topic :: System :: Systems Administration",
                   "Topic :: System :: Distributed Computing"],
      keywords='YARN HDFS hadoop distributed cluster',
      packages=['skein', 'skein.proto', 'skein.recipes'],
      package_data={'skein': ['java/*.jar']},
      entry_points='''
        [console_scripts]
        skein=skein.cli:main
      ''',
      install_requires=install_requires,
      setup_requires=setup_requires,
      zip_safe=False)
