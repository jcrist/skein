from __future__ import absolute_import, print_function, division

import json
import os

from .compatibility import urlparse
from .utils import implements

__all__ = ('Job', 'Service', 'Resources', 'File')


def is_list_of(x, typ):
    return isinstance(x, list) and all(isinstance(i, typ) for i in x)


def is_dict_of(x, ktyp, vtyp):
    return (isinstance(x, dict) and
            all(isinstance(k, ktyp) for k in x.keys()) and
            all(isinstance(v, vtyp) for v in x.values()))


def is_bounded_int(x, min=None, max=None):
    return (isinstance(x, int) and
            (min is None or min <= x) and
            (max is None or x <= max))


def _to_dict(x, skip_nulls):
    if hasattr(x, 'to_dict'):
        return x.to_dict(skip_nulls=skip_nulls)
    elif type(x) is list:
        return [_to_dict(i, skip_nulls) for i in x]
    elif type(x) is dict:
        return {k: _to_dict(v, skip_nulls) for k, v in x.items()}
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

    if format == 'yaml':
        try:
            import yaml  # noqa
        except ImportError:
            raise ImportError("PyYaml is required to use yaml functionality, "
                              "please install it.")
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
            extra = "\n".join("- %s" % e for e in extra)
            raise ValueError(("Unknown extra keys for {cls}:\n"
                              "{extra}").format(cls=cls.__name__, extra=extra))

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
            import yaml
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
        """Convert object to a json object"""
        return json.dumps(self.to_dict())

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

        obj = self.to_dict(skip_nulls=skip_nulls)

        if format == 'json':
            with open(path, mode='w') as f:
                json.dump(obj, f)
        else:
            import yaml
            with open(path, mode='w') as f:
                yaml.dump(obj, f, default_flow_style=False)


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

    def _validate(self):
        if not is_bounded_int(self.vcores, min=1):
            raise ValueError("vcores must be a positive integer")

        if not is_bounded_int(self.memory, min=1):
            raise ValueError("memory must be a positive integer")


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

    def __init__(self, source, dest=None, type='FILE'):
        self.source = source
        self.dest = dest
        self.type = type

        self._validate()

    def __repr__(self):
        return 'File<source=%r, ...>' % self.source

    def _validate(self):
        if not isinstance(self.source, str):
            raise TypeError("source must be a str")

        if not isinstance(self.dest, str):
            raise TypeError("dest must be a str")

        if self.type not in {'FILE', 'ARCHIVE'}:
            raise ValueError("type must be 'File' or 'ARCHIVE'")

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
        if not is_bounded_int(self.instances, min=0):
            raise ValueError("instances must be an integer >= 1")

        if not (self.resources is None or
                isinstance(self.resources, Resources)):
            raise TypeError("resources must be Resources or None")

        self.resources._validate()

        if is_list_of(self.files, File):
            for f in self.files:
                f._validate()
        elif self.files is not None:
            raise TypeError("files must be a list of Files or None")

        if not (self.env is None or is_dict_of(self.env, str, str)):
            raise TypeError("env must be a dict of str -> str, or None")

        if not is_list_of(self.commands, str):
            raise TypeError("commands must be a list of str")
        if not self.commands:
            raise ValueError("There must be at least one command")

        if not (self.depends is None or is_list_of(self.depends, str)):
            raise TypeError("depends must be a list of str, or None")

    @classmethod
    @implements(Base.from_dict)
    def from_dict(cls, obj):
        cls._check_keys(obj)

        resources = obj.get('resources')
        if resources is not None:
            resources = Resources.from_dict(resources)

        files = obj.get('files')
        if files is not None and isinstance(files, list):
            files = [File.from_dict(f) for f in files]

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
        if not isinstance(self.name, str):
            raise TypeError("name must be a str")

        if not (self.queue is None or isinstance(self.queue, str)):
            raise TypeError("queue must be a str or None")

        if not is_dict_of(self.services, str, Service):
            raise TypeError("services must be a dict of str -> Service")
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
