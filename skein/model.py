from __future__ import absolute_import, print_function, division

import json
import os
from datetime import datetime, timedelta

import yaml

from . import proto as _proto
from .compatibility import urlparse, with_metaclass
from .exceptions import context
from .utils import implements, format_list

__all__ = ('Job', 'Service', 'Resources', 'File', 'FileType', 'FileVisibility',
           'ApplicationState', 'FinalStatus', 'ResourceUsageReport',
           'ApplicationReport')


required = type('required', (object,),
                {'__repr__': lambda s: 'required'})()


_EPOCH = datetime(1970, 1, 1)


def _datetime_from_millis(x):
    return _EPOCH + timedelta(milliseconds=x)


def _if_none(x, y):
    return x if x is not None else y


def _pop_origin(kwargs):
    _origin = kwargs.pop('_origin', None)
    if kwargs:
        raise TypeError("from_dict() got an unexpected keyword argument "
                        "%s" % next(iter(kwargs)))
    return _origin


def is_list_of(x, typ):
    return isinstance(x, list) and all(isinstance(i, typ) for i in x)


def is_dict_of(x, ktyp, vtyp):
    return (isinstance(x, dict) and
            all(isinstance(k, ktyp) for k in x.keys()) and
            all(isinstance(v, vtyp) for v in x.values()))


def _convert(x, method, *args):
    if hasattr(x, method):
        return getattr(x, method)(*args)
    elif type(x) is list:
        return [_convert(i, method, *args) for i in x]
    elif type(x) is dict:
        return {k: _convert(v, method, *args) for k, v in x.items()}
    elif type(x) is datetime:
        return int(x.timestamp() * 1000)
    elif isinstance(x, Enum):
        return str(x)
    else:
        return x


def _infer_format(path, format='infer'):
    if format is 'infer':
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


class EnumMeta(type):
    def __init__(cls, name, parents, dct):
        for name in cls._values:
            out = object.__new__(cls)
            out._value = name
            setattr(cls, name, out)
        return super(EnumMeta, cls).__init__(name, parents, dct)

    def __iter__(cls):
        return (getattr(cls, f) for f in cls._values)

    def __len__(cls):
        return len(cls._values)


class Enum(with_metaclass(EnumMeta)):
    _values = ()
    __slots__ = ('_value',)

    def __new__(cls, x):
        if isinstance(x, cls):
            return x
        if type(x) is not str:
            raise TypeError("Expected 'str' or %r" % cls.__name__)
        x = x.upper()
        if x not in cls._values:
            raise context.ValueError("%r must be in %r"
                                     % (cls.__name__, cls._values))
        return getattr(cls, x)

    def __reduce__(self):
        return (getattr, (type(self), self._value))

    def __repr__(self):
        return '%s.%s' % (type(self).__name__, self._value)

    def __str__(self):
        return self._value

    def __eq__(self, other):
        return (self is other or
                (type(other) is str and self._value == other.upper()))

    def __hash__(self):
        return hash(self._value)

    @classmethod
    def values(cls):
        """The constants of this enum type, in the order they are declared."""
        return cls._values


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


class Base(object):
    __slots__ = ()

    def __eq__(self, other):
        return (type(self) == type(other) and
                all(getattr(self, k) == getattr(other, k)
                    for k in self.__slots__))

    @classmethod
    def _get_params(cls):
        return getattr(cls, '_params', cls.__slots__)

    def _assign_required(self, name, val):
        if val is required:
            raise context.TypeError("parameter %r is required but wasn't "
                                    "provided" % name)
        setattr(self, name, val)

    @classmethod
    def _check_keys(cls, obj, keys=None):
        keys = keys or cls._get_params()
        if not isinstance(obj, dict):
            raise context.TypeError("Expected mapping for %r" % cls.__name__)
        extra = set(obj).difference(keys)
        if extra:
            raise context.ValueError("Unknown extra keys for %s:\n"
                                     "%s" % (cls.__name__, format_list(extra)))

    def _check_is_type(self, field, type, nullable=False):
        val = getattr(self, field)
        if not (isinstance(val, type) or (nullable and val is None)):
            if nullable:
                msg = "%s must be an instance of %s, or None"
            else:
                msg = "%s must be an instance of %s"
            raise context.TypeError(msg % (field, type.__name__))

    def _check_is_list_of(self, field, type, nullable=False):
        val = getattr(self, field)
        if not (is_list_of(val, type) or (nullable and val is None)):
            if nullable:
                msg = "%s must be a list of %s, or None"
            else:
                msg = "%s must be a list of %s"
            raise context.TypeError(msg % (field, type.__name__))

    def _check_is_dict_of(self, field, key, val, nullable=False):
        attr = getattr(self, field)
        if not (is_dict_of(attr, key, val) or (nullable and attr is None)):
            if nullable:
                msg = "%s must be a dict of %s -> %s, or None"
            else:
                msg = "%s must be a list of %s -> %s"
            raise context.TypeError(msg % (field, key.__name__, val.__name__))

    def _check_is_bounded_int(self, field, min=0, nullable=False):
        x = getattr(self, field)
        self._check_is_type(field, int, nullable=nullable)
        if x is not None and x < min:
            raise context.ValueError("%s must be >= %d" % (field, min))

    @classmethod
    def from_protobuf(cls, msg):
        """Create an instance from a protobuf message."""
        if not isinstance(msg, cls._protobuf_cls):
            raise TypeError("Expected message of type "
                            "%r" % cls._protobuf_cls.__name__)
        kwargs = {k: getattr(msg, k) for k in cls._get_params()}
        return cls(**kwargs)

    @classmethod
    def from_dict(cls, obj):
        """Create an instance from a dict.

        Keys in the dict should match parameter names"""
        cls._check_keys(obj)
        return cls(**obj)

    @classmethod
    def from_json(cls, b):
        """Create an instance from a json string.

        Keys in the json object should match parameter names"""
        return cls.from_dict(json.loads(b))

    @classmethod
    def from_yaml(cls, b):
        """Create an instance from a yaml string."""
        return cls.from_dict(yaml.safe_load(b))

    def to_protobuf(self):
        """Convert object to a protobuf message"""
        self._validate()
        kwargs = {k: _convert(getattr(self, k), 'to_protobuf')
                  for k in self._get_params()}
        return self._protobuf_cls(**kwargs)

    def to_dict(self, skip_nulls=True):
        """Convert object to a dict"""
        self._validate()
        out = {}
        for k in self._get_params():
            val = getattr(self, k)
            if not skip_nulls or val is not None:
                out[k] = _convert(val, 'to_dict', skip_nulls)
        return out

    def to_json(self, skip_nulls=True):
        """Convert object to a json string"""
        return json.dumps(self.to_dict(skip_nulls=skip_nulls))

    def to_yaml(self, skip_nulls=True):
        """Convert object to a yaml string"""
        return yaml.dump(self.to_dict(skip_nulls=skip_nulls),
                         default_flow_style=False)


class Resources(Base):
    """Resource requests per container.

    Parameters
    ----------
    memory : int
        The memory to request, in MB
    vcores : int
        The number of virtual cores to request.
    """
    __slots__ = ('memory', 'vcores')
    _protobuf_cls = _proto.Resources

    def __init__(self, memory=required, vcores=required):
        self._assign_required('memory', memory)
        self._assign_required('vcores', vcores)
        self._validate()

    def __repr__(self):
        return 'Resources<memory=%d, vcores=%d>' % (self.memory, self.vcores)

    def _validate(self, is_request=False):
        min = 1 if is_request else 0
        self._check_is_bounded_int('vcores', min=min)
        self._check_is_bounded_int('memory', min=min)


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
        A ``zip`` or ``tar.gz`` file to be automatically unarchived in the
        containers.
    """
    _values = ('FILE', 'ARCHIVE')


class File(Base):
    """A file/archive to distribute with the service.

    Parameters
    ----------
    source : str
        The path to the file/archive. If no scheme is specified, path is
        assumed to be on the local filesystem (``file://`` scheme).
    type : FileType or str, optional
        The type of file to distribute. Archive's are automatically extracted
        by yarn into a directory with the same name as ``dest``. Default is
        ``FileType.FILE``.
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

    def __init__(self, source=required, type=FileType.FILE,
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
        self._check_is_type('source', str)
        self._check_is_type('type', FileType)
        self._check_is_type('visibility', FileVisibility)
        self._check_is_bounded_int('size')
        self._check_is_bounded_int('timestamp')

    @property
    def source(self):
        return self._source

    @source.setter
    def source(self, val):
        if not isinstance(val, str):
            raise context.TypeError("'source' must be an instance of 'str'")
        self._source = self._normpath(val)

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, val):
        self._type = FileType(val)

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
                    raise ValueError("paths must be absolute")
            else:
                path = url.path
            return 'file://%s%s' % (url.netloc, path)
        return path

    @implements(Base.to_protobuf)
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
        cls._check_keys(obj)
        if _origin:
            if 'source' not in obj:
                raise context.TypeError("parameter 'source' is required but "
                                        "wasn't provided")
            obj = dict(obj)
            obj['source'] = cls._normpath(obj['source'], _origin)
        return cls(**obj)

    @classmethod
    @implements(Base.from_protobuf)
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

    @classmethod
    def _parse_file_spec(cls, obj, _origin=None):
        if not isinstance(obj, dict):
            raise context.TypeError("Expected mapping for File")

        if 'archive' not in obj and 'file' not in obj:
            cls._check_keys(obj, cls._params + ('dest',))
            source = obj['source']
            type = FileType(obj.get('type', FileType.FILE))
        elif 'archive' in obj and 'file' in obj:
            raise context.ValueError("Both 'archive' and 'file' specified")
        else:
            typefield = 'archive' if 'archive' in obj else 'file'
            cls._check_keys(obj, ('visibility', 'size', 'timestamp',
                                  typefield, 'dest'))
            source = obj[typefield]
            type = FileType(typefield)

        if 'dest' not in obj:
            source = urlparse(source).path
            base, name = os.path.split(source)
            if name is None:
                raise context.ValueError("Distributed files must be "
                                         "files/archives, not directories")
            dest = name
            if type is FileType.ARCHIVE:
                for ext in ['.zip', '.tar.gz', '.tgz']:
                    if name.endswith(ext):
                        dest = name[:-len(ext)]
                        break
        else:
            dest = obj['dest']

        source = cls._normpath(source, origin=_origin)
        visibility = obj.get('visibility', FileVisibility.APPLICATION)
        size = obj.get('size', 0)
        timestamp = obj.get('timestamp', 0)

        resource = cls(source=source, type=type, visibility=visibility,
                       size=size, timestamp=timestamp)

        return dest, resource


class Service(Base):
    """Description of a Skein service.

    Parameters
    ----------
    commands : list
        Shell commands to startup the service. Commands are run in the order
        provided, with subsequent commands only run if the prior commands
        succeeded. At least one command must be provided
    resources : Resources
        Describes the resources needed to run the service.
    instances : int, optional
        The number of instances to create on startup. Default is 1.
    files : dict, optional
        Describes any files needed to run the service. A mapping of destination
        relative paths to ``File`` objects describing the sources for these
        paths.
    env : dict, optional
        A mapping of environment variables needed to run the service.
    depends : list, optional
        A list of string keys in the keystore that this service depends on. The
        service will not be started until these keys are present.
    """
    __slots__ = ('commands', 'resources', 'instances', 'files', 'env',
                 'depends')
    _protobuf_cls = _proto.Service

    def __init__(self, commands=required, resources=required, instances=1,
                 files=None, env=None, depends=None):
        self._assign_required('commands', commands)
        self._assign_required('resources', resources)
        self.instances = instances
        self.files = _if_none(files, {})
        self.env = _if_none(env, {})
        self.depends = _if_none(depends, [])
        self._validate()

    def __repr__(self):
        return 'Service<instances=%d, ...>' % self.instances

    def _validate(self):
        self._check_is_bounded_int('instances', min=0)

        self._check_is_type('resources', Resources)
        self.resources._validate(is_request=True)

        self._check_is_dict_of('files', str, File)
        for f in self.files.values():
            f._validate()

        self._check_is_dict_of('env', str, str)

        self._check_is_list_of('commands', str)
        if not self.commands:
            raise context.ValueError("There must be at least one command")

        self._check_is_list_of('depends', str)

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj, **kwargs):
        _origin = _pop_origin(kwargs)
        cls._check_keys(obj, cls.__slots__)

        resources = obj.get('resources')
        if resources is not None:
            resources = Resources.from_dict(resources)

        files = obj.get('files')

        if files is not None:
            if isinstance(files, list):
                files = dict(File._parse_file_spec(v, _origin=_origin)
                             for v in files)
            elif isinstance(files, dict):
                files = {k: File.from_dict(v, _origin=_origin)
                         for k, v in files.items()}

        kwargs = {'resources': resources,
                  'files': files,
                  'env': obj.get('env'),
                  'commands': obj.get('commands'),
                  'depends': obj.get('depends')}

        if 'instances' in obj:
            kwargs['instances'] = obj['instances']

        return cls(**kwargs)

    @classmethod
    @implements(Base.from_protobuf)
    def from_protobuf(cls, obj):
        resources = Resources.from_protobuf(obj.resources)
        files = {k: File.from_protobuf(v) for k, v in obj.files.items()}
        kwargs = {'instances': obj.instances,
                  'resources': resources,
                  'files': files,
                  'env': dict(obj.env),
                  'commands': list(obj.commands),
                  'depends': list(obj.depends)}
        return cls(**kwargs)


class Job(Base):
    """A single Skein job.

    Parameters
    ----------
    services : dict
        A mapping of service-name to services. At least one service is required.
    name : string, optional
        The name of the application, defaults to 'skein'.
    queue : string, optional
        The queue to submit to. Defaults to the default queue.
    """
    __slots__ = ('name', 'queue', 'services')
    _protobuf_cls = _proto.Job

    def __init__(self, services=required, name='skein', queue=''):
        self._assign_required('services', services)
        self.name = name
        self.queue = queue
        self._validate()

    def __repr__(self):
        return 'Job<name=%r, queue=%r, services=...>' % (self.name, self.queue)

    def _validate(self):
        self._check_is_type('name', str)
        self._check_is_type('queue', str, nullable=True)
        self._check_is_dict_of('services', str, Service)
        if not self.services:
            raise context.ValueError("There must be at least one service")
        for s in self.services.values():
            s._validate()

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj, **kwargs):
        _origin = _pop_origin(kwargs)
        cls._check_keys(obj)

        name = obj.get('name')
        queue = obj.get('queue')

        services = obj.get('services')
        if services is not None and isinstance(services, dict):
            services = {k: Service.from_dict(v, _origin=_origin)
                        for k, v in services.items()}

        return cls(name=name, queue=queue, services=services)

    @classmethod
    @implements(Base.from_protobuf)
    def from_protobuf(cls, obj):
        services = {k: Service.from_protobuf(v)
                    for k, v in obj.services.items()}
        return cls(name=obj.name,
                   queue=obj.queue,
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


def _to_camel_case(x):
    parts = x.split('_')
    return parts[0] + ''.join(x.title() for x in parts[1:])


class ResourceUsageReport(Base):
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

    _keys = tuple(_to_camel_case(k) for k in __slots__)

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
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        cls._check_keys(obj, cls._keys)
        kwargs = dict(memory_seconds=obj['memorySeconds'],
                      vcore_seconds=obj['vcoreSeconds'],
                      num_used_containers=max(0, obj['numUsedContainers']))
        for k, k2 in [('needed_resources', 'neededResources'),
                      ('reserved_resources', 'reservedResources'),
                      ('used_resources', 'usedResources')]:
            val = obj[k2]
            kwargs[k] = Resources(vcores=max(0, val['vcores']),
                                  memory=max(0, val['memory']))
        return cls(**kwargs)

    @classmethod
    @implements(Base.from_protobuf)
    def from_protobuf(cls, obj):
        kwargs = dict(memory_seconds=obj.memory_seconds,
                      vcore_seconds=obj.vcore_seconds,
                      num_used_containers=obj.num_used_containers)
        for k in ['needed_resources', 'reserved_resources', 'used_resources']:
            kwargs[k] = Resources.from_protobuf(getattr(obj, k))
        return cls(**kwargs)


class ApplicationReport(Base):
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
    tags : list of strings
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
                 'tracking_url', 'state', 'final_status', 'progress', 'usage',
                 'diagnostics', 'start_time', 'finish_time')
    _protobuf_cls = _proto.ApplicationReport

    _keys = tuple(_to_camel_case(k) for k in __slots__)

    def __init__(self, id, name, user, queue, tags, host, port,
                 tracking_url, state, final_status, progress, usage,
                 diagnostics, start_time, finish_time):
        self.id = id
        self.name = name
        self.user = user
        self.queue = queue
        self.tags = tags
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
        return 'ApplicationReport<name=%r>' % self.name

    def _validate(self):
        self._check_is_type('id', str)
        self._check_is_type('name', str)
        self._check_is_type('user', str)
        self._check_is_type('queue', str)
        self._check_is_list_of('tags', str)
        self._check_is_type('host', str, nullable=True)
        self._check_is_type('port', int, nullable=True)
        self._check_is_type('tracking_url', str, nullable=True)
        self._check_is_type('state', ApplicationState)
        self._check_is_type('final_status', FinalStatus)
        self._check_is_type('progress', float)
        self._check_is_type('usage', ResourceUsageReport)
        self.usage._validate()
        self._check_is_type('diagnostics', str, nullable=True)
        self._check_is_type('start_time', datetime)
        self._check_is_type('finish_time', datetime)

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        cls._check_keys(obj, cls._keys)
        kwargs = {k: obj.get(k2) for k, k2 in zip(cls.__slots__, cls._keys)}
        kwargs['usage'] = ResourceUsageReport.from_dict(kwargs['usage'])
        kwargs['state'] = ApplicationState(kwargs['state'])
        kwargs['final_status'] = FinalStatus(kwargs['final_status'])
        for k in ['start_time', 'finish_time']:
            kwargs[k] = _datetime_from_millis(kwargs[k])
        return cls(**kwargs)

    @classmethod
    @implements(Base.from_protobuf)
    def from_protobuf(cls, obj):
        state = ApplicationState(_proto.ApplicationState.Type.Name(obj.state))
        final_status = FinalStatus(_proto.FinalStatus.Type.Name(obj.final_status))

        return cls(id=obj.id,
                   name=obj.name,
                   user=obj.user,
                   queue=obj.queue,
                   tags=list(obj.tags),
                   host=obj.host,
                   port=obj.port,
                   tracking_url=obj.tracking_url,
                   state=state,
                   final_status=final_status,
                   progress=obj.progress,
                   usage=ResourceUsageReport.from_protobuf(obj.usage),
                   diagnostics=obj.diagnostics,
                   start_time=_datetime_from_millis(obj.start_time),
                   finish_time=_datetime_from_millis(obj.finish_time))
