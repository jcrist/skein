from __future__ import absolute_import, print_function, division

import json
import os
from datetime import datetime, timedelta
from getpass import getuser

import yaml

from . import proto as _proto
from .compatibility import urlparse, string, integer, math_ceil, makedirs
from .objects import Enum, ProtobufMessage, Specification, required
from .exceptions import context, FileNotFoundError, FileExistsError
from .utils import (implements, format_list, datetime_from_millis, runtime,
                    xor, lock_file)

__all__ = ('ApplicationSpec', 'Service', 'Resources', 'File', 'FileType',
           'FileVisibility', 'ACLs', 'Master', 'Security', 'ApplicationState',
           'FinalStatus', 'ResourceUsageReport', 'ApplicationReport',
           'ContainerState', 'Container', 'LogLevel', 'NodeState', 'NodeReport',
           'QueueState', 'Queue')


def _check_is_filename(target):
    orig = target
    # Allow local relative paths
    if target.startswith("./"):
        target = target[2:]
    if "/" in target:
        raise context.ValueError(
            "Keys in `files` must be filenames only (no directories allowed), "
            "found %r" % orig
        )


def _pop_origin(kwargs):
    _origin = kwargs.pop('_origin', None)
    if kwargs:
        raise TypeError("from_dict() got an unexpected keyword argument "
                        "%s" % next(iter(kwargs)))
    return _origin


def container_instance_from_string(id):
    """Create a ContainerInstance from an id string"""
    try:
        service, instance = id.rsplit('_', 1)
        instance = int(instance)
    except (TypeError, ValueError):
        raise context.ValueError("Invalid container id %r" % id)
    return _proto.ContainerInstance(service_name=service, instance=instance)


def container_instance_to_string(id):
    """Create an id string from a ContainerInstance"""
    return '%s_%d' % (id.service_name, id.instance)


def parse_memory(s):
    """Converts bytes expression to number of mebibytes.

    If no unit is specified, ``MiB`` is used."""
    if isinstance(s, integer):
        out = s
    elif isinstance(s, float):
        out = math_ceil(s)
    elif isinstance(s, string):
        s = s.replace(' ', '')

        if not s:
            raise context.ValueError("Could not interpret %r as a byte unit" % s)

        if s[0].isdigit():
            for i, c in enumerate(reversed(s)):
                if not c.isalpha():
                    break

            index = len(s) - i
            prefix = s[:index]
            suffix = s[index:]

            try:
                n = float(prefix)
            except ValueError:
                raise context.ValueError("Could not interpret %r as a number" % prefix)
        else:
            n = 1
            suffix = s

        try:
            multiplier = _byte_sizes[suffix.lower()]
        except KeyError:
            raise context.ValueError("Could not interpret %r as a byte unit" % suffix)

        out = math_ceil(n * multiplier / (2 ** 20))
    else:
        raise context.TypeError("memory must be an integer, got %r"
                                % type(s).__name__)

    if out < 0:
        raise context.ValueError("memory must be positive")

    return out


_byte_sizes = {
    'kb': 10**3,
    'mb': 10**6,
    'gb': 10**9,
    'tb': 10**12,
    'pb': 10**15,
    'kib': 2**10,
    'mib': 2**20,
    'gib': 2**30,
    'tib': 2**40,
    'pib': 2**50,
    'b': 1,
    '': 2 ** 20
}
_byte_sizes.update({k[:-1]: v for k, v in _byte_sizes.items() if len(k) >= 2})


def check_no_cycles(dependencies):
    completed = set()
    seen = set()

    for key in dependencies:
        if key in completed:
            continue
        nodes = [key]
        while nodes:
            # Keep current node on the stack until all descendants are visited
            cur = nodes[-1]
            if cur in completed:
                # Already fully traversed descendants of cur
                nodes.pop()
                continue
            seen.add(cur)

            # Add direct descendants of cur to nodes stack
            next_nodes = []
            for nxt in dependencies[cur]:
                if nxt not in completed:
                    if nxt in seen:
                        cycle = [nxt]
                        while nodes[-1] != nxt:
                            cycle.append(nodes.pop())
                        cycle.append(nodes.pop())
                        raise context.ValueError(
                            'Dependency cycle detected between services: %s' %
                            '->'.join(str(x) for x in reversed(cycle)))
                    next_nodes.append(nxt)

            if next_nodes:
                nodes.extend(next_nodes)
            else:
                completed.add(cur)
                seen.remove(cur)
                nodes.pop()


def _infer_format(path, format='infer'):
    if format == 'infer':
        _, ext = os.path.splitext(path)
        if ext == '.json':
            format = 'json'
        elif ext in {'.yaml', '.yml'}:
            format = 'yaml'
        else:
            if context.is_cli:
                msg = "Unsupported file type %r" % ext
            else:
                msg = ("Can't infer format from filepath %r, please "
                       "specify manually" % path)
            raise context.ValueError(msg)
    elif format not in {'json', 'yaml'}:
        raise ValueError("Unknown file format: %r" % format)
    return format


class Security(Specification):
    """Security configuration.

    Secrets may be specified either as file paths or raw bytes, but not both.

    Parameters
    ----------
    cert_file : string, File, optional
        The TLS certificate file, in pem format. Either a path or a fully
        specified File object.
    key_file : string, File, optional
        The TLS private key file, in pem format. Either a path or a fully
        specified File object.
    cert_bytes : bytes, optional
        The contents of the TLS certificate file, in pem format.
    key_bytes : bytes, optional
        The contents of the TLS private key file, in pem format.
    """
    __slots__ = ('_cert_file', '_key_file', '_cert_bytes', '_key_bytes')
    _params = ('cert_file', 'key_file', 'cert_bytes', 'key_bytes')
    _protobuf_cls = _proto.Security

    def __init__(self, cert_file=None, key_file=None, cert_bytes=None,
                 key_bytes=None):
        self.cert_file = cert_file
        self.key_file = key_file
        self.cert_bytes = cert_bytes
        self.key_bytes = key_bytes

        self._validate()

    def _get_bytes(self, kind):
        self._validate()

        out = getattr(self, '%s_bytes' % kind)
        if out is not None:
            return out

        file = getattr(self, '%s_file' % kind)
        if not file.source.startswith('file://'):
            raise context.ValueError(
                "Cannot read security file %r locally" % file.source
            )
        path = file.source[7:]
        if not os.path.exists(path):
            raise context.FileNotFoundError(
                "Security %s file not found at %r" % (kind, path)
            )
        with open(path, 'rb') as fil:
            return fil.read()

    def _validate(self):
        if not xor(self.cert_file is None, self.cert_bytes is None):
            raise context.ValueError("Must specify exactly one of "
                                     "cert_file or cert_bytes")
        if not xor(self.key_file is None, self.key_bytes is None):
            raise context.ValueError("Must specify exactly one of "
                                     "key_file or key_bytes")

    def _assign_file(self, field, value):
        if isinstance(value, string):
            value = File(value)
        elif not (value is None or isinstance(value, File)):
            raise context.TypeError("%s must be a File or None" % field)
        setattr(self, '_%s' % field, value)

    def _assign_bytes(self, field, value):
        if value is not None:
            if isinstance(value, string):
                value = value.encode()
            elif not isinstance(value, bytes):
                raise context.TypeError("%s must be a bytes or None" % field)
        setattr(self, '_%s' % field, value)

    @property
    def cert_file(self):
        return self._cert_file

    @cert_file.setter
    def cert_file(self, value):
        self._assign_file('cert_file', value)

    @property
    def key_file(self):
        return self._key_file

    @key_file.setter
    def key_file(self, value):
        self._assign_file('key_file', value)

    @property
    def cert_bytes(self):
        return self._cert_bytes

    @cert_bytes.setter
    def cert_bytes(self, value):
        self._assign_bytes('cert_bytes', value)

    @property
    def key_bytes(self):
        return self._key_bytes

    @key_bytes.setter
    def key_bytes(self, value):
        self._assign_bytes('key_bytes', value)

    def __repr__(self):
        return 'Security<...>'

    @classmethod
    @implements(Specification.from_dict)
    def from_dict(cls, obj, **kwargs):
        _origin = _pop_origin(kwargs)
        cls._check_keys(obj)

        cert_file = obj.pop('cert_file', None)
        if cert_file is not None:
            cert_file = File.from_dict(cert_file, _origin=_origin)

        key_file = obj.pop('key_file', None)
        if key_file is not None:
            key_file = File.from_dict(key_file, _origin=_origin)

        return cls(cert_file=cert_file, key_file=key_file, **obj)

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        kwargs = {}
        for name in ['cert', 'key']:
            which = obj.WhichOneof(name)
            if which == '%s_bytes' % name:
                kwargs[which] = getattr(obj, '%s_bytes' % name)
            elif which == '%s_file' % name:
                kwargs[which] = File.from_protobuf(getattr(obj, '%s_file' % name))
        return cls(**kwargs)

    @implements(Specification.to_dict)
    def to_dict(self, skip_nulls=True):
        obj = super(Security, self).to_dict(skip_nulls=skip_nulls)
        return {k: v.decode() if isinstance(v, bytes) else v
                for k, v in obj.items()}

    @classmethod
    def from_default(cls):
        """The default security configuration.

        Usually this loads the credentials stored in the configuration
        directory (``~/.skein`` by default). If these credentials don't already
        exist, new ones will be created.

        When run in a YARN container started by Skein, this loads the same
        security credentials as used for the current application.
        """
        from .core import properties

        # Are we in a container started by skein?
        if properties.application_id is not None:
            if properties.container_dir is not None:
                cert_path = os.path.join(properties.container_dir, '.skein.crt')
                key_path = os.path.join(properties.container_dir, '.skein.pem')
                if os.path.exists(cert_path) and os.path.exists(key_path):
                    return Security(cert_file=cert_path, key_file=key_path)
            raise context.FileNotFoundError(
                "Failed to resolve .skein.{crt,pem} in 'LOCAL_DIRS'")

        # Try to load from config_dir, and fallback to minting new credentials
        try:
            return cls.from_directory(properties.config_dir)
        except FileNotFoundError:
            pass

        new = cls.new_credentials()
        try:
            out = new.to_directory(properties.config_dir)
            context.warn("Skein global security credentials not found, "
                         "writing now to %r." % properties.config_dir)
        except FileExistsError:
            # Race condition between competing processes, use the credentials
            # written by the other process.
            out = cls.from_directory(properties.config_dir)
        return out

    @classmethod
    def from_directory(cls, directory):
        """Create a security object from a directory.

        Relies on standard names for each file (``skein.crt`` and
        ``skein.pem``)."""
        cert_path = os.path.join(directory, 'skein.crt')
        key_path = os.path.join(directory, 'skein.pem')
        for path, name in [(cert_path, 'cert'), (key_path, 'key')]:
            if not os.path.exists(path):
                raise context.FileNotFoundError(
                    "Security %s file not found at %r" % (name, path)
                )
        return Security(cert_file=cert_path, key_file=key_path)

    def to_directory(self, directory, force=False):
        """Write this security object to a directory.

        Parameters
        ----------
        directory : str
            The directory to write the configuration to.
        force : bool, optional
            If security credentials already exist at this location, an error
            will be raised by default. Set to True to overwrite existing files.

        Returns
        -------
        security : Security
            A new security object backed by the written files.
        """
        self._validate()

        # Create directory if it doesn't exist
        makedirs(directory, exist_ok=True)

        cert_path = os.path.join(directory, 'skein.crt')
        key_path = os.path.join(directory, 'skein.pem')
        cert_bytes = self._get_bytes('cert')
        key_bytes = self._get_bytes('key')

        lock_path = os.path.join(directory, 'skein.lock')
        with lock_file(lock_path):
            for path, name in [(cert_path, 'skein.crt'), (key_path, 'skein.pem')]:
                if os.path.exists(path):
                    if force:
                        os.unlink(path)
                    else:
                        msg = ("%r file already exists, use `%s` to overwrite" %
                               (name, '--force' if context.is_cli else 'force'))
                        raise context.FileExistsError(msg)

            flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
            for path, data in [(cert_path, cert_bytes), (key_path, key_bytes)]:
                with os.fdopen(os.open(path, flags, 0o600), 'wb') as fil:
                    fil.write(data)

        return Security(cert_file=cert_path, key_file=key_path)

    @classmethod
    def new_credentials(cls):
        """Create a new Security object with a new certificate/key pair."""
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        key = rsa.generate_private_key(public_exponent=65537,
                                       key_size=2048,
                                       backend=default_backend())
        key_bytes = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        subject = issuer = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, u'skein-internal')])
        now = datetime.utcnow()
        cert = (x509.CertificateBuilder()
                    .subject_name(subject)
                    .issuer_name(issuer)
                    .public_key(key.public_key())
                    .serial_number(x509.random_serial_number())
                    .not_valid_before(now)
                    .not_valid_after(now + timedelta(days=365))
                    .sign(key, hashes.SHA256(), default_backend()))

        cert_bytes = cert.public_bytes(serialization.Encoding.PEM)

        return cls(cert_bytes=cert_bytes, key_bytes=key_bytes)


class ApplicationState(Enum):
    """Enum of application states.

    Attributes
    ----------
    NEW : ApplicationState
        Application was just created.
    NEW_SAVING : ApplicationState
        Application is being saved.
    SUBMITTED : ApplicationState
        Application has been submitted.
    ACCEPTED : ApplicationState
        Application has been accepted by the scheduler.
    RUNNING : ApplicationState
        Application is currently running.
    FINISHED : ApplicationState
        Application finished successfully.
    FAILED : ApplicationState
        Application failed.
    KILLED : ApplicationState
        Application was terminated by a user or admin.
    """
    _values = ('NEW',
               'NEW_SAVING',
               'SUBMITTED',
               'ACCEPTED',
               'RUNNING',
               'FINISHED',
               'FAILED',
               'KILLED')


class FinalStatus(Enum):
    """Enum of application final statuses.

    Attributes
    ----------
    SUCCEEDED : FinalStatus
        Application finished successfully.
    KILLED : FinalStatus
        Application was terminated by a user or admin.
    FAILED : FinalStatus
        Application failed.
    UNDEFINED : FinalStatus
        Application has not yet finished.
    """
    _values = ('SUCCEEDED',
               'KILLED',
               'FAILED',
               'UNDEFINED')


class Resources(Specification):
    """Resource requests per container.

    Parameters
    ----------
    memory : string or int
        The amount of memory to request. Can be either a string with units
        (e.g. ``"5 GiB"``), or numeric. If numeric, specifies the amount of
        memory in *MiB*. Note that the units are in mebibytes (MiB) NOT
        megabytes (MB) - the former being binary based (1024 MiB in a GiB), the
        latter being decimal based (1000 MB in a GB).

        Requests smaller than the minimum allocation will receive the minimum
        allocation (1024 MiB by default). Requests larger than the maximum
        allocation will error on application submission.
    vcores : int
        The number of virtual cores to request. Depending on your system
        configuration one virtual core may map to a single actual core, or a
        fraction of a core. Requests larger than the maximum allocation will
        error on application submission.
    gpus : int, optional
        The number of gpus to request. Requires Hadoop >= 3.1, sets
        resource requirements for ``yarn.io/gpu``. Default is 0.
    fpgas : int, optional
        The number of fpgas to request. Requires Hadoop >= 3.1, sets
        resource requirements for ``yarn.io/fpga``. Default is 0.
    """
    __slots__ = ('_memory', 'vcores', 'gpus', 'fpgas')
    _params = ('memory', 'vcores', 'gpus', 'fpgas')
    _protobuf_cls = _proto.Resources

    def __init__(self, memory=required, vcores=required, gpus=0, fpgas=0):
        self._assign_required('memory', memory)
        self._assign_required('vcores', vcores)
        self.gpus = gpus
        self.fpgas = fpgas
        self._validate()

    @property
    def memory(self):
        return self._memory

    @memory.setter
    def memory(self, value):
        self._memory = parse_memory(value)

    def __repr__(self):
        return 'Resources<memory=%d, vcores=%d>' % (self.memory, self.vcores)

    def _validate(self, is_request=False):
        min = 1 if is_request else 0
        self._check_is_bounded_int('vcores', min=min)
        self._check_is_bounded_int('memory', min=min)
        self._check_is_bounded_int('gpus', min=0)
        self._check_is_bounded_int('fpgas', min=0)


class FileVisibility(Enum):
    """Enum of possible file visibilities.

    Determines how the file can be shared between containers.

    Attributes
    ----------
    APPLICATION : FileVisibility
        Shared only among containers of the same application on the node.
    PUBLIC : FileVisibility
        Shared by all users on the node.
    PRIVATE : FileVisibility
        Shared among all applications of the same user on the node.
    """
    _values = ('APPLICATION', 'PUBLIC', 'PRIVATE')


class FileType(Enum):
    """Enum of possible file types to distribute with the application.

    Attributes
    ----------
    FILE : FileType
        Regular file
    ARCHIVE : FileType
        A ``.zip``, ``.tar.gz``, or ``.tgz`` file to be automatically
        unarchived in the containers.
    """
    _values = ('FILE', 'ARCHIVE')


class File(Specification):
    """A file/archive to distribute with the service.

    Parameters
    ----------
    source : str
        The path to the file/archive. If no scheme is specified, path is
        assumed to be on the local filesystem (``file://`` scheme).
    type : FileType or str, optional
        The type of file to distribute. Archive's are automatically extracted
        by yarn into a directory with the same name as their destination.
        By default the type is inferred from the file extension.
    visibility : FileVisibility or str, optional
        The resource visibility, default is ``FileVisibility.APPLICATION``
    size : int, optional
        The resource size in bytes. If not provided will be determined by the
        file system.
    timestamp : int, optional
        The time the resource was last modified. If not provided will be
        determined by the file system.
    """
    __slots__ = ('_source', '_type', '_visibility', 'size', 'timestamp')
    _params = ('source', 'type', 'visibility', 'size', 'timestamp')
    _protobuf_cls = _proto.File

    def __init__(self, source=required, type='infer',
                 visibility=FileVisibility.APPLICATION, size=0, timestamp=0):
        self._assign_required('source', source)
        self.type = type
        self.visibility = visibility
        self.size = size
        self.timestamp = timestamp
        self._validate()

    def __repr__(self):
        return 'File<source=%r, type=%r>' % (self.source, self.type)

    def _validate(self):
        self._check_is_type('source', string)
        self._check_is_type('type', FileType)
        self._check_is_type('visibility', FileVisibility)
        self._check_is_bounded_int('size')
        self._check_is_bounded_int('timestamp')

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, val):
        if not isinstance(val, string):
            raise context.TypeError("'source' must be a string")
        self._source = self._normpath(val)

    @property
    def type(self):
        if self._type == 'infer':
            return (FileType.ARCHIVE
                    if any(map(self._source.endswith, ('.zip', '.tar.gz', '.tgz')))
                    else FileType.FILE)
        return self._type

    @type.setter
    def type(self, val):
        self._type = val if val == 'infer' else FileType(val)

    @property
    def visibility(self):
        return self._visibility

    @visibility.setter
    def visibility(self, val):
        self._visibility = FileVisibility(val)

    @staticmethod
    def _normpath(path, origin=None):
        url = urlparse(path)
        if not url.scheme:
            if not os.path.isabs(url.path):
                if origin is not None:
                    path = os.path.normpath(os.path.join(origin, url.path))
                else:
                    path = os.path.abspath(url.path)
            else:
                path = url.path
            return 'file://%s%s' % (url.netloc, path)
        return path

    @implements(ProtobufMessage.to_protobuf)
    def to_protobuf(self):
        self._validate()
        url = urlparse(self.source)
        urlmsg = _proto.Url(scheme=url.scheme,
                            host=url.hostname,
                            port=url.port,
                            file=url.path)
        return _proto.File(source=urlmsg,
                           type=str(self.type),
                           visibility=str(self.visibility),
                           size=self.size,
                           timestamp=self.timestamp)

    @classmethod
    def from_dict(cls, obj, **kwargs):
        """Create an instance from a dict.

        Keys in the dict should match parameter names"""
        _origin = _pop_origin(kwargs)

        if isinstance(obj, string):
            obj = {'source': obj}

        cls._check_keys(obj)
        if _origin:
            if 'source' not in obj:
                raise context.TypeError("parameter 'source' is required but "
                                        "wasn't provided")
            obj = dict(obj)
            obj['source'] = cls._normpath(obj['source'], _origin)
        return cls(**obj)

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        if not isinstance(obj, cls._protobuf_cls):
            raise TypeError("Expected message of type "
                            "%r" % cls._protobuf_cls.__name__)
        url = obj.source
        netloc = '%s:%d' % (url.host, url.port) if url.host else ''
        source = '%s://%s%s' % (url.scheme, netloc, url.file)
        return cls(source=source,
                   type=_proto.File.Type.Name(obj.type),
                   visibility=_proto.File.Visibility.Name(obj.visibility),
                   size=obj.size,
                   timestamp=obj.timestamp)


class Service(Specification):
    """Description of a Skein service.

    Parameters
    ----------
    resources : Resources
        Describes the resources needed to run the service.
    script : str
        A bash script to run the service.
    instances : int, optional
        The number of instances to create on startup. Default is 1.
    files : dict, optional
        Describes any files needed to run the service. A mapping of destination
        relative paths to ``File`` or ``str`` objects describing the sources
        for these paths. If a ``str``, the file type is inferred from the
        extension.
    env : dict, optional
        A mapping of environment variables needed to run the service.
    depends : set, optional
        A set of service names that this service depends on. The service will
        only be started after all its dependencies have been started.
    max_restarts : int, optional
        The maximum number of restarts to allow for this service. Containers
        are only restarted on failure, and the cap is set for all containers in
        the service, not per container. Set to -1 to allow infinite restarts.
        Default is 0.
    allow_failures : bool, optional
        If False (default), the whole application will shutdown if the number
        of failures for this service exceeds ``max_restarts``. Set to True to
        keep the application running even if this service exceeds its failure
        limit.
    node_label : str, optional
        The node label expression to use when requesting containers for this
        service. If not set, defaults to the application-level ``node_label``
        (if set).
    nodes : list, optional
        A list of node host names to restrict containers for this service to.
        If not set, defaults to no node restrictions.
    racks : list, optional
        A list of rack names to restrict containers for this service to. The
        racks corresponding to any nodes requested will be automatically added
        to this list. If not set, defaults to no rack restrictions.
    relax_locality : bool, optional
        If true, containers for this request may be assigned on hosts and racks
        other than the ones explicitly requested. If False, those restrictions
        are strictly enforced. Default is False.
    """
    __slots__ = ('resources', 'script', 'instances', 'files', 'env',
                 'depends', 'max_restarts', 'allow_failures', 'node_label',
                 'nodes', 'racks', 'relax_locality')
    _protobuf_cls = _proto.Service

    def __init__(self, resources=required, script=required, instances=1,
                 files=None, env=None, depends=None, max_restarts=0,
                 allow_failures=False, node_label='', nodes=None, racks=None,
                 relax_locality=False):
        self._assign_required('resources', resources)
        self._assign_required('script', script)
        self.instances = instances
        self.files = ({k: v if isinstance(v, File) else File(v)
                       for (k, v) in files.items()}
                      if files is not None else {})
        self.env = {} if env is None else env
        self.depends = set() if depends is None else set(depends)
        self.max_restarts = max_restarts
        self.allow_failures = allow_failures
        self.node_label = node_label
        self.nodes = [] if nodes is None else nodes
        self.racks = [] if racks is None else racks
        self.relax_locality = relax_locality
        self._validate()

    def __repr__(self):
        return 'Service<instances=%d, ...>' % self.instances

    def _validate(self):
        self._check_is_bounded_int('instances', min=0)
        self._check_is_type('node_label', string)
        self._check_is_list_of('nodes', string)
        self._check_is_list_of('racks', string)
        self._check_is_type('relax_locality', bool)
        self._check_is_bounded_int('max_restarts', min=-1)
        self._check_is_type('allow_failures', bool)

        self._check_is_type('resources', Resources)
        self.resources._validate(is_request=True)

        self._check_is_dict_of('files', string, File)
        for target, f in self.files.items():
            _check_is_filename(target)
            f._validate()

        self._check_is_dict_of('env', string, string)

        self._check_is_type('script', string)
        if not self.script:
            raise context.ValueError("A script must be provided")

        self._check_is_set_of('depends', string)

    @classmethod
    @implements(Specification.from_dict)
    def from_dict(cls, obj, **kwargs):
        _origin = _pop_origin(kwargs)
        obj = obj.copy()

        cls._check_keys(obj, cls.__slots__)

        resources = obj.pop('resources', None)
        if resources is not None:
            resources = Resources.from_dict(resources)

        files = obj.pop('files', None)
        if files is not None:
            files = {k: File.from_dict(v, _origin=_origin)
                     for k, v in files.items()}

        return cls(resources=resources,
                   files=files,
                   **obj)

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        resources = Resources.from_protobuf(obj.resources)
        files = {k: File.from_protobuf(v) for k, v in obj.files.items()}
        kwargs = {'instances': obj.instances,
                  'node_label': obj.node_label,
                  'nodes': list(obj.nodes),
                  'racks': list(obj.racks),
                  'relax_locality': obj.relax_locality,
                  'max_restarts': obj.max_restarts,
                  'allow_failures': obj.allow_failures,
                  'resources': resources,
                  'files': files,
                  'env': dict(obj.env),
                  'script': obj.script,
                  'depends': set(obj.depends)}
        return cls(**kwargs)


class ACLs(Specification):
    """Skein Access Control Lists.

    Maps access types to users/groups to provide that access.

    The following access types are supported:

    - VIEW : view application details
    - MODIFY : modify the application via YARN (e.g. killing the application)
    - UI : access the application Web UI

    The VIEW and MODIFY access types are handled by YARN directly; permissions
    for these can be set by users and/or groups. Authorizing UI access is
    handled by Skein internally, and only user-level access control is
    supported.

    The application owner (the user who submitted the application) will always
    have permission for all access types.

    By default, ACLs are disabled - to enable, set ``enable=True``. If enabled,
    access is restricted only to the application owner by default - add
    users/groups to the access types you wish to expand to other users.

    Parameters
    ----------
    enable : bool, optional
        If True, the ACLs will be enforced. Default is False.
    view_users, view_groups : list, optional
        Lists of users/groups to give VIEW access to this application. If both
        are empty, only the application owner has access (default). If either
        contains ``"*"``, all users are given access (default). See the YARN
        documentation for more information on what VIEW access entails.
    modify_users, modify_groups : list, optional
        Lists of users/groups to give MODIFY access to this application. If
        both are empty, only the application owner has access (default). If
        either contains ``"*"``, all users are given access (default). See the
        YARN documentation for more information on what MODIFY access entails.
    ui_users : list, optional
        A list of users to give access to the application Web UI. If empty,
        only the application owner has access (default). If it contains
        ``"*"``, all users are given access.

    Examples
    --------
    By default ACLs are disabled, and all users have access.

    >>> import skein
    >>> acls = skein.ACLs()

    Enabling ACLs results in only the application owner having access (provided
    YARN is also configured with ACLs enabled).

    >>> acls = skein.ACLs(enable=True)

    To give access to other users, add users/groups to the desired access
    types. Here we enable view access for all users in group ``engineering``,
    and modify access for user ``nancy``.

    >>> acls = skein.ACLs(enable=True,
    ...                   view_groups=['engineering'],
    ...                   modify_users=['nancy'])

    You can use the wildcard character ``"*"`` to enable access for all users.
    Here we give view access to all users:

    >>> acls = skein.ACLs(enable=True,
    ...                   view_users=['*'])
    """
    __slots__ = ('enable', 'view_users', 'view_groups', 'modify_users',
                 'modify_groups', 'ui_users')
    _protobuf_cls = _proto.Acls

    def __init__(self, enable=False, view_users=None, view_groups=None,
                 modify_users=None, modify_groups=None, ui_users=None):
        self.enable = enable
        self.view_users = [] if view_users is None else view_users
        self.view_groups = [] if view_groups is None else view_groups
        self.modify_users = [] if modify_users is None else modify_users
        self.modify_groups = [] if modify_groups is None else modify_groups
        self.ui_users = [] if ui_users is None else ui_users
        self._validate()

    def __repr__(self):
        return 'ACLs<enable=%r, ...>' % self.enable

    def _validate(self):
        self._check_is_type('enable', bool)
        self._check_is_list_of('view_users', string)
        self._check_is_list_of('view_groups', string)
        self._check_is_list_of('modify_users', string)
        self._check_is_list_of('modify_groups', string)
        self._check_is_list_of('ui_users', string)

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        if not isinstance(obj, cls._protobuf_cls):
            raise TypeError("Expected message of type "
                            "%r" % cls._protobuf_cls.__name__)
        return cls(enable=obj.enable,
                   view_users=list(obj.view_users),
                   view_groups=list(obj.view_groups),
                   modify_users=list(obj.modify_users),
                   modify_groups=list(obj.modify_groups),
                   ui_users=list(obj.ui_users))


class LogLevel(Enum):
    """Enum of log levels.

    Corresponds with ``log4j`` logging levels.

    Attributes
    ----------
    OFF : LogLevel
        Turns on all logging.
    TRACE : LogLevel
        The finest level of events.
    DEBUG : LogLevel
        Fine-grained informational events that are most useful to debug an
        application.
    INFO : LogLevel
        Informational messages that highlight the progress of the application
        at a coarse-grained level. The default LogLevel.
    WARN : LogLevel
        Potentially harmful situations that still allow the application to
        continue running.
    ERROR : LogLevel
        Error events that might still allow the application to continue
        running.
    FATAL : LogLevel
        Severe error events that will lead the application to abort.
    OFF : LogLevel
        Turns off all logging.
    """
    _values = ('ALL',
               'TRACE',
               'DEBUG',
               'INFO',
               'WARN',
               'ERROR',
               'FATAL',
               'OFF')


class Master(Specification):
    """Configuration for the Application Master.

    Parameters
    ----------
    resources : Resources, optional
        Describes the resources needed to run the application master. Default
        is 512 MiB, 1 virtual core.
    script : str, optional
        An optional bash script to run after starting the application master.
        If provided, the application will terminate once the script has
        completed.
    files : dict, optional
        Describes any additional files needed to run the application master. A
        mapping of destination relative paths to ``File`` or ``str`` objects
        describing the sources for these paths. If a ``str``, the file type is
        inferred from the extension.
    env : dict, optional
        A mapping of environment variables to set on the application master.
    log_level : str or LogLevel, optional
        The application master log level. Sets the ``skein.log.level`` system
        property. One of {'ALL', 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR',
        'FATAL', 'OFF'} (from most to least verbose). Default is 'INFO'.
    log_config : str or File, optional
        A custom ``log4j.properties`` file to use for the application master.
        If not provided, the default logging configuration will be used.
    security : Security, optional
        The security credentials to use for the application master. If not
        provided, these will be the same as those used by the submitting
        client.
    """
    __slots__ = ('resources', 'script', 'files', 'env',
                 '_log_level', 'log_config', 'security')
    _params = ('resources', 'script', 'files', 'env',
               'log_level', 'log_config', 'security')
    _protobuf_cls = _proto.Master

    def __init__(self, resources=None, script="", files=None, env=None,
                 log_level=LogLevel.INFO, log_config=None, security=None):
        self.resources = (Resources(memory='512 MiB', vcores=1)
                          if resources is None else resources)
        self.script = script
        self.files = ({k: v if isinstance(v, File) else File(v)
                       for (k, v) in files.items()}
                      if files is not None else {})
        self.env = {} if env is None else env
        self.log_level = log_level
        self.log_config = (File(log_config) if isinstance(log_config, string)
                           else log_config)
        self.security = security

        self._validate()

    def _validate(self):
        self._check_is_type('resources', Resources)
        self.resources._validate(is_request=True)

        self._check_is_dict_of('files', string, File)
        for target, f in self.files.items():
            _check_is_filename(target)
            f._validate()

        self._check_is_dict_of('env', string, string)
        self._check_is_type('script', string)

        if self.log_config is not None:
            self._check_is_type('log_config', File)
            self.log_config._validate()

        if self.security is not None:
            self._check_is_type('security', Security)
            self.security._validate()

    @property
    def log_level(self):
        return self._log_level

    @log_level.setter
    def log_level(self, log_level):
        self._log_level = LogLevel(log_level)

    def __repr__(self):
        return 'Master<...>'

    @classmethod
    @implements(Specification.from_dict)
    def from_dict(cls, obj, **kwargs):
        cls._check_keys(obj)

        obj = obj.copy()
        log_config = obj.pop('log_config', None)
        if log_config is not None:
            log_config = File.from_dict(log_config, **kwargs)

        security = obj.pop('security', None)
        if security is not None:
            security = Security.from_dict(security, **kwargs)

        resources = obj.pop('resources', None)
        if resources is not None:
            resources = Resources.from_dict(resources)

        files = obj.pop('files', None)
        if files is not None:
            files = {k: File.from_dict(v, **kwargs) for k, v in files.items()}

        return cls(log_config=log_config,
                   security=security,
                   resources=resources,
                   files=files,
                   **obj)

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        resources = Resources.from_protobuf(obj.resources)
        files = {k: File.from_protobuf(v) for k, v in obj.files.items()}
        log_level = _proto.Log.Level.Name(obj.log_level)
        log_config = (File.from_protobuf(obj.log_config)
                      if obj.HasField('log_config')
                      else None)
        security = (Security.from_protobuf(obj.security)
                    if obj.HasField('security')
                    else None)
        return cls(resources=resources,
                   files=files,
                   script=obj.script,
                   env=dict(obj.env),
                   log_level=log_level,
                   log_config=log_config,
                   security=security)


class ApplicationSpec(Specification):
    """A complete description of an application.

    Parameters
    ----------
    services : dict, optional
        A mapping of service-name to services. Applications must either specify
        at least one service, or a script for the application master to run
        (see ``skein.Master`` for more information).
    master : Master, optional
        Additional configuration for the application master service. See
        ``skein.Master`` for more information.
    name : string, optional
        The name of the application, defaults to 'skein'.
    queue : string, optional
        The queue to submit to. Defaults to the default queue.
    user : string, optional
        The user name to submit the application as. Requires that the
        submitting user have permission to proxy as this user name. Default is
        the submitter's user name.
    node_label : string, optional
        The node label expression to use when requesting containers for this
        application. Services can override this setting by specifying
        ``node_label`` on the service directly. Default is no label.
    tags : set, optional
        A set of strings to use as tags for this application.
    file_systems : list, optional
        A list of Hadoop file systems to acquire delegation tokens for.
        A token is always acquired for the ``defaultFS``.
    acls : ACLs, optional
        Allows restricting users/groups to subsets of application access. See
        ``skein.ACLs`` for more information.
    max_attempts : int, optional
        The maximum number of submission attempts before marking the
        application as failed. Note that this only considers failures of the
        application master during startup. Default is 1.
    """
    __slots__ = ('services', 'master', 'name', 'queue', 'user', 'node_label',
                 'tags', 'file_systems', 'acls', 'max_attempts')
    _protobuf_cls = _proto.ApplicationSpec

    def __init__(self, services=None, master=None, name='skein',
                 queue='default', user='', node_label='', tags=None,
                 file_systems=None, acls=None, max_attempts=1):
        self.services = {} if services is None else services
        self.master = Master() if master is None else master
        self.name = name
        self.queue = queue
        self.user = user
        self.node_label = node_label
        self.tags = set() if tags is None else set(tags)
        self.file_systems = [] if file_systems is None else file_systems
        self.acls = ACLs() if acls is None else acls
        self.max_attempts = max_attempts
        self._validate()

    def __repr__(self):
        return ('ApplicationSpec<name=%r, queue=%r, services=...>' %
                (self.name, self.queue))

    def _validate(self):
        self._check_is_type('name', string)
        self._check_is_type('queue', string)
        self._check_is_type('user', string)
        self._check_is_type('node_label', string)
        self._check_is_set_of('tags', string)
        self._check_is_list_of('file_systems', string)
        self._check_is_bounded_int('max_attempts', min=1)
        self._check_is_type('acls', ACLs)
        self.acls._validate()
        self._check_is_type('master', Master)
        self.master._validate()
        self._check_is_dict_of('services', string, Service)

        if not self.services and not self.master.script:
            raise context.ValueError("There must be at least one service")

        for name, service in self.services.items():
            service._validate()
            missing = set(service.depends).difference(self.services)
            if missing:
                raise context.ValueError(
                    "Unknown service dependencies for service %r:\n"
                    "%s" % (name, format_list(missing)))

        dependencies = {name: service.depends
                        for name, service in self.services.items()}
        check_no_cycles(dependencies)

    @classmethod
    def _from_any(cls, spec):
        """Generic creation method for all types accepted as ``spec``"""
        if isinstance(spec, str):
            spec = cls.from_file(spec)
        elif isinstance(spec, dict):
            spec = cls.from_dict(spec)
        elif not isinstance(spec, cls):
            raise context.TypeError("spec must be either an ApplicationSpec, "
                                    "path, or dict, got "
                                    "%s" % type(spec).__name__)
        return spec

    @classmethod
    @implements(Specification.from_dict)
    def from_dict(cls, obj, **kwargs):
        _origin = _pop_origin(kwargs)
        cls._check_keys(obj)

        obj = obj.copy()

        services = obj.pop('services', None)
        if services is not None and isinstance(services, dict):
            services = {k: Service.from_dict(v, _origin=_origin)
                        for k, v in services.items()}

        acls = obj.pop('acls', None)
        if acls is not None and isinstance(acls, dict):
            acls = ACLs.from_dict(acls)

        master = obj.pop('master', None)
        if master is not None and isinstance(master, dict):
            master = Master.from_dict(master, _origin=_origin)

        return cls(services=services, acls=acls, master=master, **obj)

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        services = {k: Service.from_protobuf(v)
                    for k, v in obj.services.items()}
        return cls(name=obj.name,
                   queue=obj.queue,
                   user=obj.user,
                   node_label=obj.node_label,
                   tags=set(obj.tags),
                   file_systems=list(obj.file_systems),
                   max_attempts=min(1, obj.max_attempts),
                   acls=ACLs.from_protobuf(obj.acls),
                   master=Master.from_protobuf(obj.master),
                   services=services)

    @classmethod
    def from_file(cls, path, format='infer'):
        """Create an instance from a json or yaml file.

        Parameters
        ----------
        path : str
            The path to the file to load.
        format : {'infer', 'json', 'yaml'}, optional
            The file format. By default the format is inferred from the file
            extension.
        """
        format = _infer_format(path, format=format)
        origin = os.path.abspath(os.path.dirname(path))

        with open(path) as f:
            data = f.read()
        if format == 'json':
            obj = json.loads(data)
        else:
            obj = yaml.safe_load(data)
        return cls.from_dict(obj, _origin=origin)

    def to_file(self, path, format='infer', skip_nulls=True):
        """Write object to a file.

        Parameters
        ----------
        path : str
            The path to the file to load.
        format : {'infer', 'json', 'yaml'}, optional
            The file format. By default the format is inferred from the file
            extension.
        skip_nulls : bool, optional
            By default null values are skipped in the output. Set to True to
            output all fields.
        """
        format = _infer_format(path, format=format)
        data = getattr(self, 'to_' + format)(skip_nulls=skip_nulls)
        with open(path, mode='w') as f:
            f.write(data)


class ResourceUsageReport(ProtobufMessage):
    """Resource usage report.

    Parameters
    ----------
    memory_seconds : int
        The total amount of memory (in MBs) the application has allocated times
        the number of seconds the application has been running.
    vcore_seconds : int
        The total number of vcores that the application has allocated times the
        number of seconds the application has been running.
    num_used_containers : int
        Current number of containers in use.
    needed_resources : Resources
        The needed resources.
    reserved_resources : Resources
        The reserved resources.
    used_resources : Resources
        The used resources.
    """
    __slots__ = ('memory_seconds', 'vcore_seconds', 'num_used_containers',
                 'needed_resources', 'reserved_resources', 'used_resources')
    _protobuf_cls = _proto.ResourceUsageReport

    def __init__(self, memory_seconds, vcore_seconds, num_used_containers,
                 needed_resources, reserved_resources, used_resources):
        self.memory_seconds = memory_seconds
        self.vcore_seconds = vcore_seconds
        self.num_used_containers = num_used_containers
        self.needed_resources = needed_resources
        self.reserved_resources = reserved_resources
        self.used_resources = used_resources

        self._validate()

    def __repr__(self):
        return 'ResourceUsageReport<...>'

    def _validate(self):
        for k in ['memory_seconds', 'vcore_seconds', 'num_used_containers']:
            self._check_is_bounded_int(k)
        for k in ['needed_resources', 'reserved_resources', 'used_resources']:
            self._check_is_type(k, Resources)
            getattr(self, k)._validate()

    @classmethod
    @implements(Specification.from_protobuf)
    def from_protobuf(cls, obj):
        kwargs = dict(memory_seconds=obj.memory_seconds,
                      vcore_seconds=obj.vcore_seconds,
                      num_used_containers=obj.num_used_containers)
        for k in ['needed_resources', 'reserved_resources', 'used_resources']:
            kwargs[k] = Resources.from_protobuf(getattr(obj, k))
        return cls(**kwargs)


class ApplicationReport(ProtobufMessage):
    """Report of application status.

    Parameters
    ----------
    id : str
        The application ID.
    name : str
        The application name.
    user : str
        The user that started the application.
    queue : str
        The application queue.
    tags : set of strings
        The application tags.
    host : str
        The host the application master is running on.
    port : int
        The rpc port for the application master
    tracking_url : str
        The application tracking url.
    state : ApplicationState
        The application state.
    final_status : FinalStatus
        The application final status.
    progress : float
        The progress of the application, from 0.0 to 1.0.
    usage : ResourceUsageReport
        Report on application resource usage.
    diagnostics : str
        The diagnostic message in the case of failures.
    start_time : datetime
        The application start time.
    finish_time : datetime
        The application finish time.
    """
    __slots__ = ('id', 'name', 'user', 'queue', 'tags', 'host', 'port',
                 'tracking_url', '_state', '_final_status', 'progress', 'usage',
                 'diagnostics', 'start_time', 'finish_time')
    _params = ('id', 'name', 'user', 'queue', 'tags', 'host', 'port',
               'tracking_url', 'state', 'final_status', 'progress', 'usage',
               'diagnostics', 'start_time', 'finish_time')
    _protobuf_cls = _proto.ApplicationReport

    def __init__(self, id, name, user, queue, tags, host, port,
                 tracking_url, state, final_status, progress, usage,
                 diagnostics, start_time, finish_time):
        self.id = id
        self.name = name
        self.user = user
        self.queue = queue
        self.tags = set() if tags is None else set(tags)
        self.host = host
        self.port = port
        self.tracking_url = tracking_url
        self.state = state
        self.final_status = final_status
        self.progress = progress
        self.usage = usage
        self.diagnostics = diagnostics
        self.start_time = start_time
        self.finish_time = finish_time

        self._validate()

    def __repr__(self):
        return 'ApplicationReport<id=%r, name=%r>' % (self.id, self.name)

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = ApplicationState(state)

    @property
    def final_status(self):
        return self._final_status

    @final_status.setter
    def final_status(self, state):
        self._final_status = FinalStatus(state)

    @property
    def runtime(self):
        """The total runtime of the container."""
        return runtime(self.start_time, self.finish_time)

    def _validate(self):
        self._check_is_type('id', string)
        self._check_is_type('name', string)
        self._check_is_type('user', string)
        self._check_is_type('queue', string)
        self._check_is_set_of('tags', string)
        self._check_is_type('host', string)
        self._check_is_type('port', integer)
        self._check_is_type('tracking_url', string)
        self._check_is_type('state', ApplicationState)
        self._check_is_type('final_status', FinalStatus)
        self._check_is_type('progress', float)
        self._check_is_type('usage', ResourceUsageReport)
        self.usage._validate()
        self._check_is_type('diagnostics', string)
        self._check_is_type('start_time', datetime, nullable=True)
        self._check_is_type('finish_time', datetime, nullable=True)

    @classmethod
    @implements(ProtobufMessage.from_protobuf)
    def from_protobuf(cls, obj):
        state = ApplicationState(_proto.ApplicationState.Type.Name(obj.state))
        final_status = FinalStatus(_proto.FinalStatus.Type.Name(obj.final_status))

        return cls(id=obj.id,
                   name=obj.name,
                   user=obj.user,
                   queue=obj.queue,
                   tags=obj.tags,
                   host=obj.host,
                   port=obj.port,
                   tracking_url=obj.tracking_url,
                   state=state,
                   final_status=final_status,
                   progress=obj.progress,
                   usage=ResourceUsageReport.from_protobuf(obj.usage),
                   diagnostics=obj.diagnostics,
                   start_time=datetime_from_millis(obj.start_time),
                   finish_time=datetime_from_millis(obj.finish_time))


class ContainerState(Enum):
    """Enum of container states.

    Attributes
    ----------
    WAITING : ContainerState
        Container is waiting on another service to startup before being
        requested.
    REQUESTED : ContainerState
        Container has been requested but is not currently running.
    RUNNING : ContainerState
        Container is currently running.
    SUCCEEDED : ContainerState
        Container finished successfully.
    FAILED : ContainerState
        Container failed.
    KILLED : ContainerState
        Container was terminated by a user or admin.
    """
    _values = ('WAITING',
               'REQUESTED',
               'RUNNING',
               'SUCCEEDED',
               'FAILED',
               'KILLED')


class Container(ProtobufMessage):
    """Current container state.

    Parameters
    ----------
    service_name : str
        The name of the service this container is running.
    instance : int
        The container instance number.
    state : ContainerState
        The current container state.
    yarn_container_id : str
        The YARN container id.
    yarn_node_http_address : str
        The YARN node HTTP address given as ``host:port``.
    start_time : datetime
        The start time, None if container has not started.
    finish_time : datetime
        The finish time, None if container has not finished.
    exit_message : str
        The diagnostic exit message for completed containers.
    """
    __slots__ = ('service_name', 'instance', '_state', 'yarn_container_id',
                 'yarn_node_http_address', 'start_time', 'finish_time',
                 'exit_message')
    _params = ('service_name', 'instance', 'state', 'yarn_container_id',
               'yarn_node_http_address', 'start_time', 'finish_time',
               'exit_message')
    _protobuf_cls = _proto.Container

    def __init__(self, service_name, instance, state, yarn_container_id,
                 yarn_node_http_address, start_time, finish_time, exit_message):
        self.service_name = service_name
        self.instance = instance
        self.state = state
        self.yarn_container_id = yarn_container_id
        self.yarn_node_http_address = yarn_node_http_address
        self.start_time = start_time
        self.finish_time = finish_time
        self.exit_message = exit_message

        self._validate()

    def __repr__(self):
        return ('Container<service_name=%r, instance=%d, state=%s>'
                % (self.service_name, self.instance, self.state))

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = ContainerState(state)

    def _validate(self):
        self._check_is_type('service_name', string)
        self._check_is_type('instance', integer)
        self._check_is_type('state', ContainerState)
        self._check_is_type('yarn_container_id', string)
        self._check_is_type('yarn_node_http_address', string)
        self._check_is_type('start_time', datetime, nullable=True)
        self._check_is_type('finish_time', datetime, nullable=True)
        self._check_is_type('exit_message', string)

    @property
    def id(self):
        """The complete service_name & instance identity of this container."""
        return '%s_%d' % (self.service_name, self.instance)

    @property
    def runtime(self):
        """The total runtime of the application."""
        return runtime(self.start_time, self.finish_time)

    @property
    def yarn_container_logs(self):
        if not self.yarn_node_http_address or not self.yarn_container_id:
            return ""

        return "/".join([
            self.yarn_node_http_address,
            "node",
            "containerlogs",
            self.yarn_container_id,
            getuser()
        ])

    @classmethod
    @implements(ProtobufMessage.from_protobuf)
    def from_protobuf(cls, obj):
        return cls(service_name=obj.service_name,
                   instance=obj.instance,
                   state=ContainerState(_proto.Container.State.Name(obj.state)),
                   yarn_container_id=obj.yarn_container_id,
                   yarn_node_http_address=obj.yarn_node_http_address,
                   start_time=datetime_from_millis(obj.start_time),
                   finish_time=datetime_from_millis(obj.finish_time),
                   exit_message=obj.exit_message)


class NodeState(Enum):
    """Enum of node states.

    Attributes
    ----------
    DECOMMISSIONED : NodeState
        Node is out of service.
    DECOMMISSIONING : NodeState
        Node is currently decommissioning.
    LOST : NodeState
        Node has not sent responded for some time.
    NEW : NodeState
        New has just started.
    REBOOTED : NodeState
        Node is just rebooted.
    RUNNING : NodeState
        Node is currently running.
    SHUTDOWN : NodeState
        Node has been shutdown gracefully.
    UNHEALTHY : NodeState
        Node is unhealthy.
    """
    _values = ('DECOMMISSIONED',
               'DECOMMISSIONING',
               'LOST',
               'NEW',
               'REBOOTED',
               'RUNNING',
               'SHUTDOWN',
               'UNHEALTHY')


class NodeReport(ProtobufMessage):
    """Report of node status.

    Attributes
    ----------
    id : str
        The node id.
    http_address : str
        The http address to the node manager.
    rack_name : str
        The rack name for this node.
    labels : set
        Node labels for this node.
    state : NodeState
        The node's current state.
    health_report : str
        The diagnostic health report for this node.
    total_resources : Resources
        Total resources available on this node.
    used_resources : Resources
        Used resources available on this node.
    """
    __slots__ = ('id', 'http_address', 'rack_name', 'labels', '_state',
                 'health_report', 'total_resources', 'used_resources')
    _params = ('id', 'http_address', 'rack_name', 'labels', 'state',
               'health_report', 'total_resources', 'used_resources')
    _protobuf_cls = _proto.NodeReport

    def __init__(self, id, http_address, rack_name, labels, state,
                 health_report, total_resources, used_resources):
        self.id = id
        self.http_address = http_address
        self.rack_name = rack_name
        self.labels = labels
        self.state = state
        self.health_report = health_report
        self.total_resources = total_resources
        self.used_resources = used_resources

        self._validate()

    def __repr__(self):
        return 'NodeReport<id=%r>' % self.id

    @property
    def host(self):
        """The node manager host for this node."""
        return self.id.split(':')[0]

    @property
    def port(self):
        """The node manager port for this node."""
        return int(self.id.split(':')[1])

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = NodeState(state)

    def _validate(self):
        self._check_is_type('id', string)
        self._check_is_type('http_address', string)
        self._check_is_type('rack_name', string)
        self._check_is_set_of('labels', string)
        self._check_is_type('health_report', string)
        self._check_is_type('total_resources', Resources)
        self.total_resources._validate()
        self._check_is_type('used_resources', Resources)
        self.used_resources._validate()

    @classmethod
    @implements(ProtobufMessage.from_protobuf)
    def from_protobuf(cls, obj):
        return cls(id=obj.id,
                   http_address=obj.http_address,
                   rack_name=obj.rack_name,
                   labels=set(obj.labels),
                   state=NodeState(_proto.NodeState.Type.Name(obj.state)),
                   health_report=obj.health_report,
                   total_resources=Resources.from_protobuf(obj.total_resources),
                   used_resources=Resources.from_protobuf(obj.used_resources))


class QueueState(Enum):
    """Enum of queue states.

    Attributes
    ----------
    RUNNING : QueueState
        Queue is running, normal operation.
    STOPPED : QueueState
        Queue is stopped, no longer taking new requests.
    """
    _values = ('RUNNING',
               'STOPPED')


class Queue(ProtobufMessage):
    """Information about a specific YARN queue.

    Attributes
    ----------
    name : str
        The queue's name.
    state : QueueState
        The queue's state.
    capacity : float
        The queue's capacity as a percentage. For the capacity scheduler, the
        queue is guaranteed access to this percentage of the parent queue's
        resources (if sibling queues are running over their limit, there may be
        a lag accessing resources as those applications scale down). For the
        fair scheduler, this number is the percentage of the total cluster this
        queue currently has in its fair share (this will shift dynamically
        during cluster use).
    max_capacity : float
        The queue's max capacity as a percentage. For the capacity scheduler,
        this queue may elastically expand to use up to this percentage of its
        parent's resources if its siblings aren't running at their capacity.
        For the fair scheduler this is always 100%.
    percent_used : float
        The percent of this queue's capacity that's currently in use. This may
        be over 100% if elasticity is in effect.
    node_labels : set
        A set of all accessible node labels for this queue. If all node labels
        are accessible this is the set ``{"*"}``.
    default_node_label : str
        The default node label for this queue. This will be used if the
        application doesn't specify a node label itself.
    """
    __slots__ = ('name', '_state', 'capacity', 'max_capacity', 'percent_used',
                 'node_labels', 'default_node_label')
    _params = ('name', 'state', 'capacity', 'max_capacity', 'percent_used',
               'node_labels', 'default_node_label')
    _protobuf_cls = _proto.Queue

    def __init__(self, name, state, capacity, max_capacity, percent_used,
                 node_labels, default_node_label):
        self.name = name
        self.state = state
        self.capacity = capacity
        self.max_capacity = max_capacity
        self.percent_used = percent_used
        self.node_labels = node_labels
        self.default_node_label = default_node_label

        self._validate()

    def __repr__(self):
        return ('Queue<name=%r, percent_used=%.2f>'
                % (self.name, self.percent_used))

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, state):
        self._state = QueueState(state)

    def _validate(self):
        self._check_is_type('name', string)
        self._check_is_type('capacity', float)
        self._check_is_type('max_capacity', float)
        self._check_is_type('percent_used', float)
        self._check_is_set_of('node_labels', string)
        self._check_is_type('default_node_label', string)

    @classmethod
    @implements(ProtobufMessage.from_protobuf)
    def from_protobuf(cls, obj):
        return cls(name=obj.name,
                   state=QueueState(_proto.Queue.State.Name(obj.state)),
                   capacity=obj.capacity,
                   max_capacity=obj.max_capacity,
                   percent_used=obj.percent_used,
                   node_labels=set(obj.node_labels),
                   default_node_label=obj.default_node_label)
