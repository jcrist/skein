import os
import subprocess
import sys
from distutils.command.build import build as _build
from distutils.command.clean import clean as _clean
from distutils.dir_util import remove_tree
from glob import glob

from setuptools import setup, Command
from setuptools.command.develop import develop as _develop
from setuptools.command.install import install as _install

import versioneer

ROOT_DIR = os.path.dirname(os.path.relpath(__file__))
JAVA_DIR = os.path.join(ROOT_DIR, 'java')
JAVA_TARGET_DIR = os.path.join(JAVA_DIR, 'target')
JAVA_INPLACE_DIR = os.path.join(ROOT_DIR, 'skein', 'java')


def _get_jars(dir):
    return [f for f in glob(os.path.join(dir, '*.jar'))
            if f.endswith('-jar-with-dependencies.jar')]


class build_java(Command):
    description = "build java artifacts"

    def initialize_options(self):
        pass

    finalize_options = initialize_options

    def run(self):
        # Compile the java code and copy the jar to skein/java/
        # This will be picked up as package_data later
        self.mkpath(JAVA_INPLACE_DIR)
        code = subprocess.call(['mvn', '-f', os.path.join(JAVA_DIR, 'pom.xml'),
                                'assembly:assembly'])
        if code:
            sys.exit(code)

        jar_files = _get_jars(JAVA_TARGET_DIR)

        if not jar_files:
            self.warn('Maven compilation produced no jar files')
            sys.exit(1)

        for jar_file in jar_files:
            self.announce("copying %s -> %s" % (jar_file, JAVA_INPLACE_DIR))
            self.copy_file(jar_file, JAVA_INPLACE_DIR)


def _ensure_java(command):
    if not _get_jars(JAVA_INPLACE_DIR):
        command.run_command('build_java')


class build(_build):
    def run(self):
        _ensure_java(self)
        _build.run(self)


class install(_install):
    def run(self):
        _ensure_java(self)
        _install.run(self)


class develop(_develop):
    def run(self):
        if not self.uninstall:
            _ensure_java(self)
        _develop.run(self)


class clean(_clean):
    def run(self):
        if self.all:
            for d in [JAVA_INPLACE_DIR, JAVA_TARGET_DIR]:
                if os.path.exists(d):
                    remove_tree(d, dry_run=self.dry_run)
        _clean.run(self)


# Due to quirks in setuptools/distutils dependency ordering, to get the java
# source to build automatically in most cases, we need to check for it in
# multiple locations. This is unfortunate, but seems necessary.
cmdclass = versioneer.get_cmdclass()
cmdclass.update({'build_java': build_java,  # directly build the java source
                 'build': build,            # bdist_wheel or pip install .
                 'install': install,        # python setup.py install
                 'develop': develop,        # python setup.py develop
                 'clean': clean})           # extra cleanup


setup(name='skein',
      version=versioneer.get_version(),
      cmdclass=cmdclass,
      maintainer='Jim Crist',
      maintainer_email='jiminy.crist@gmail.com',
      license='BSD',
      packages=['skein'],
      package_data={'skein': ['java/*.jar']},
      install_requires=['requests'])
