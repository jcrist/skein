from __future__ import absolute_import, print_function, division

import json
import os
from datetime import datetime, timedelta

import yaml

from .compatibility import urlparse
from .utils import implements, format_list

__all__ = ('Job', 'Service', 'Resources', 'File')


def is_list_of(x, typ):
    return isinstance(x, list) and all(isinstance(i, typ) for i in x)


def is_dict_of(x, ktyp, vtyp):
    return (isinstance(x, dict) and
            all(isinstance(k, ktyp) for k in x.keys()) and
            all(isinstance(v, vtyp) for v in x.values()))


def _to_dict(x, skip_nulls):
    if hasattr(x, 'to_dict'):
        return x.to_dict(skip_nulls=skip_nulls)
    elif type(x) is list:
        return [_to_dict(i, skip_nulls) for i in x]
    elif type(x) is dict:
        return {k: _to_dict(v, skip_nulls) for k, v in x.items()}
    elif type(x) is datetime:
        return int(x.timestamp() * 1000)
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
            raise ValueError("Can't infer format from filepath %r, please "
                             "specify manually" % path)
    elif format not in {'json', 'yaml'}:
        raise ValueError("Unknown file format: %r" % format)
    return format


class Base(object):
    __slots__ = ()

    @classmethod
    def _check_keys(cls, obj, keys=None):
        keys = keys or cls.__slots__
        if not isinstance(obj, dict):
            raise TypeError("Expected mapping for %r" % cls.__name__)
        extra = set(obj).difference(keys)
        if extra:
            raise ValueError("Unknown extra keys for %s:\n"
                             "%s" % (cls.__name__, format_list(extra)))

    def _check_is_type(self, field, type, nullable=False):
        val = getattr(self, field)
        if not (isinstance(val, type) or (nullable and val is None)):
            if nullable:
                msg = "%s must be an instance of %s, or None"
            else:
                msg = "%s must be an instance of %s"
            raise TypeError(msg % (field, type.__name__))

    def _check_is_list_of(self, field, type, nullable=False):
        val = getattr(self, field)
        if not (is_list_of(val, type) or (nullable and val is None)):
            if nullable:
                msg = "%s must be a list of %s, or None"
            else:
                msg = "%s must be a list of %s"
            raise TypeError(msg % (field, type.__name__))

    def _check_is_dict_of(self, field, key, val, nullable=False):
        attr = getattr(self, field)
        if not (is_dict_of(attr, key, val) or (nullable and attr is None)):
            if nullable:
                msg = "%s must be a dict of %s -> %s, or None"
            else:
                msg = "%s must be a list of %s -> %s"
            raise TypeError(msg % (field, key.__name__, val.__name__))

    def _check_is_bounded_int(self, field, min=0):
        x = getattr(self, field)
        if not (isinstance(x, int) and min <= x):
            raise ValueError("%s must be an integer >= %d" % (field, min))

    def _validate(self):
        pass

    @classmethod
    def from_dict(cls, obj):
        """Create an instance from a dict.

        Keys in the dict should match parameter names"""
        cls._check_keys(obj)
        return cls(**obj)

    @classmethod
    def from_json(cls, b):
        """Create an instance from a json object.

        Keys in the json object should match parameter names"""
        return cls.from_dict(json.loads(b))

    @classmethod
    def from_file(cls, path, format='infer'):
        """Create an instance from a json or yaml file.

        Parameter
        ---------
        path : str
            The path to the file to load.
        format : {'infer', 'json', 'yaml'}, optional
            The file format. By default the format is inferred from the file
            extension.
        """
        format = _infer_format(path, format=format)

        if format == 'json':
            with open(path) as f:
                data = f.read()
            return cls.from_json(data)
        else:
            with open(path) as f:
                data = yaml.safe_load(f)
            return cls.from_dict(data)

    def to_dict(self, skip_nulls=True):
        """Convert object to a dict"""
        self._validate()
        out = {}
        for k in self.__slots__:
            val = getattr(self, k)
            if not skip_nulls or val is not None:
                out[k] = _to_dict(val, skip_nulls)
        return out

    def to_json(self, skip_nulls=True):
        """Convert object to a json string"""
        return json.dumps(self.to_dict(skip_nulls=skip_nulls))

    def to_yaml(self, skip_nulls=True):
        """Convert object to a yaml string"""
        return yaml.dump(self.to_dict(skip_nulls=skip_nulls),
                         default_flow_style=False)

    def to_file(self, path, format='infer', skip_nulls=True):
        """Write object to a file.

        Parameter
        ---------
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


class Resources(Base):
    """Resource requests per container.

    Parameters
    ----------
    memory : int
        The memory to request, in MB
    vcores : int, optional
        The number of virtual cores to request. Default is 1.
    """
    __slots__ = ('vcores', 'memory')

    def __init__(self, memory, vcores=1):
        self.memory = memory
        self.vcores = vcores

        self._validate()

    def __repr__(self):
        return 'Resources<memory=%d, vcores=%d>' % (self.memory, self.vcores)

    def _validate(self, is_request=False):
        min = 1 if is_request else 0
        self._check_is_bounded_int('vcores', min=min)
        self._check_is_bounded_int('memory', min=min)


class File(Base):
    """A file/archive to distribute with the service.

    Parameters
    ----------
    source : str
        The path to the file/archive. If no scheme is specified, path is
        assumed to be on the local filesystem (``file://`` scheme).
    dest : str, optional
        The localized path on the cluster to the file/archive. If not
        specified, the file name is used.
    type : {'FILE', 'ARCHIVE'}, optional
        The type of file to distribute. Archive's are automatically extracted
        by yarn into a directory with the same name as ``dest``. Default is
        ``'FILE'``.
    """
    __slots__ = ('source', 'dest', 'type')

    def __init__(self, source, dest, type='FILE'):
        self.source = source
        self.dest = dest
        self.type = type

        self._validate()

    def __repr__(self):
        return 'File<source=%r, ...>' % self.source

    def _validate(self):
        self._check_is_type('source', str)
        self._check_is_type('dest', str)
        if self.type not in {'FILE', 'ARCHIVE'}:
            raise ValueError("type must be 'File' or 'ARCHIVE'")

    @classmethod
    def _from_local_resource(cls, key, val):
        url = val['url']
        host = url.get('host', '')
        port = str(url.get('port', ''))
        address = ':'.join(filter(bool, [host, host and port]))
        source = '%s://%s%s' % (url['scheme'], address, url['file'])
        return cls(source=source, dest=key, type=val['type'])

    @classmethod
    def _from_dict_shorthand(cls, obj, type):
        cls._check_keys(obj, [type, 'dest'])
        path = obj[type]
        type = type.upper()
        if 'dest' not in obj:
            path = urlparse(path).path
            base, name = os.path.split(path)
            if name is None:
                raise ValueError("Distributed files must be files/archives, "
                                 "not directories")
            dest = name
            if type == 'ARCHIVE':
                for ext in ['.zip', '.tar.gz', '.tgz']:
                    if name.endswith(ext):
                        dest = name[:-len(ext)]
                        break
        return cls(source=path, dest=dest, type=type)

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        if not isinstance(obj, dict):
            raise TypeError("Expected mapping for File")

        # Handle shorthands
        if 'file' in obj:
            return cls._from_dict_shorthand(obj, 'file')
        elif 'archive' in obj:
            return cls._from_dict_shorthand(obj, 'archive')
        cls._check_keys(obj)
        return cls(**obj)


class Service(Base):
    """Description of a Skein service.

    Parameters
    ----------
    instances : int, optional
        The number of instances to create on startup. Default is 1.
    resources : Resources, optional
        Describes the resources needed to run the service. If not provided, 1
        vcore and the minimal memory request for the cluster will be used.
    files : list, optional
        A list of ``File`` objects needed to run the service.
    env : dict, optional
        A mapping of environment variables needed to run the service.
    commands : list, optional
        Shell commands to startup the service. Commands are run in the order
        provided, with subsequent commands only run if the prior commands
        succeeded.
    depends : list, optional
        A list of string keys in the keystore that this service depends on. The
        service will not be started until these keys are present.
    """
    __slots__ = ('instances', 'resources', 'files', 'env', 'commands', 'depends')

    def __init__(self, instances=1, resources=None, files=None,
                 env=None, commands=None, depends=None):
        self.instances = instances
        self.resources = resources
        self.files = files
        self.env = env
        self.commands = commands
        self.depends = depends

        self._validate()

    def __repr__(self):
        return 'Service<instances=%d, ...>' % self.instances

    def _validate(self):
        self._check_is_bounded_int('instances', min=0)

        self._check_is_type('resources', Resources)
        self.resources._validate(is_request=True)

        self._check_is_list_of('files', File, nullable=True)
        if self.files is not None:
            for f in self.files:
                f._validate()

        self._check_is_dict_of('env', str, str, nullable=True)

        self._check_is_list_of('commands', str)
        if not self.commands:
            raise ValueError("There must be at least one command")

        self._check_is_list_of('depends', str, nullable=True)

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        cls._check_keys(obj, cls.__slots__ + ('localResources',))

        resources = obj.get('resources')
        if resources is not None:
            resources = Resources.from_dict(resources)

        local_resources = obj.get('localResources')
        files = obj.get('files')

        if files is not None and local_resources is not None:
            raise ValueError("Unknown extra keys for Service:\n"
                             "- localResources")
        elif files is not None:
            if isinstance(files, list):
                files = [File.from_dict(f) for f in files]
        elif local_resources is not None:
            if isinstance(local_resources, dict):
                files = [File._from_local_resource(k, v)
                         for k, v in local_resources.items()]
            else:
                files = None

        kwargs = {'resources': resources,
                  'files': files,
                  'env': obj.get('env'),
                  'commands': obj.get('commands'),
                  'depends': obj.get('depends')}

        if 'instances' in obj:
            kwargs['instances'] = obj['instances']

        return cls(**kwargs)


class Job(Base):
    """A single Skein job.

    Parameters
    ----------
    name : string, optional
        The name of the application, defaults to 'skein'.
    queue : string, optional
        The queue to submit to. Defaults to the default queue.
    services : dict, optional
        A mapping of service-name to services.
    """
    __slots__ = ('name', 'queue', 'services')

    def __init__(self, name='skein', queue=None, services=None):
        self.name = name
        self.queue = queue
        self.services = services

        self._validate()

    def __repr__(self):
        return 'Job<name=%r, queue=%r, services=...>' % (self.name, self.queue)

    def _validate(self):
        self._check_is_type('name', str)
        self._check_is_type('queue', str, nullable=True)
        self._check_is_dict_of('services', str, Service)
        if not self.services:
            raise ValueError("There must be at least one service")
        for s in self.services.values():
            s._validate()

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        cls._check_keys(obj)

        name = obj.get('name')
        queue = obj.get('queue')

        services = obj.get('services')
        if services is not None and isinstance(services, dict):
            services = {k: Service.from_dict(v) for k, v in services.items()}

        return cls(name=name, queue=queue, services=services)


def _to_camel_case(x):
    parts = x.split('_')
    return parts[0] + ''.join(x.title() for x in parts[1:])


class Usage(Base):
    __slots__ = ('memory_seconds', 'vcore_seconds', 'num_used_containers',
                 'needed_resources', 'reserved_resources', 'used_resources')
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
        return 'Usage<...>'

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


class ApplicationReport(Base):
    __slots__ = ('id', 'name', 'user', 'queue', 'tags', 'host', 'port',
                 'tracking_url', 'state', 'final_status', 'progress', 'usage',
                 'diagnostics', 'start_time', 'finish_time')
    _keys = tuple(_to_camel_case(k) for k in __slots__)
    _epoch = datetime(1970, 1, 1)

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
        self._check_is_list_of('tags', str, nullable=True)
        self._check_is_type('host', str, nullable=True)
        self._check_is_type('port', int, nullable=True)
        self._check_is_type('tracking_url', str, nullable=True)
        self._check_is_type('state', str)
        self._check_is_type('final_status', str)
        self._check_is_type('progress', float)
        self._check_is_type('usage', Usage)
        self.usage._validate()
        self._check_is_type('diagnostics', str, nullable=True)
        self._check_is_type('start_time', datetime)
        self._check_is_type('finish_time', datetime)

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        cls._check_keys(obj, cls._keys)
        kwargs = {k: obj.get(k2) for k, k2 in zip(cls.__slots__, cls._keys)}
        kwargs['usage'] = Usage.from_dict(kwargs['usage'])
        for k in ['start_time', 'finish_time']:
            kwargs[k] = cls._epoch + timedelta(milliseconds=kwargs[k])
        return cls(**kwargs)
