from setuptools import setup
import versioneer

setup(name='pycrochet',
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      maintainer='Jim Crist',
      maintainer_email='jiminy.crist@gmail.com',
      license='BSD',
      packages=['pycrochet'],
      install_requires=['requests'])
