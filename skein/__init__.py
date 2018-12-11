from . import kv
from .core import Client, ApplicationClient, properties
from .exceptions import (SkeinError, ConnectionError, DriverNotRunningError,
                         ApplicationNotRunningError, DriverError,
                         ApplicationError)
from .model import (Security, ApplicationSpec, Service, File, Resources,
                    FileType, FileVisibility, ACLs, Master)

# TODO: deprecated, remove after next release cycle
from .exceptions import DaemonError, DaemonNotRunningError

from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
