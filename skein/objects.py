from __future__ import absolute_import, print_function, division

import json
from datetime import datetime

import yaml

from .compatibility import with_metaclass, string, integer
from .exceptions import context
from .utils import format_list, ensure_unicode, datetime_to_millis


def typename(cls):
    if cls is string:
        return 'string'
    elif cls is integer:
        return 'integer'
    return cls.__name__


# constants used to detect no parameter passed to keyword argument
# These are all used semantically the same, but have different reprs
# to show up nicely when used interactively/in docs.
required = type('required', (object,),
                dict.fromkeys(['__repr__', '__reduce__'],
                              lambda s: 'required'))()
no_change = type('no_change', (object,),
                 dict.fromkeys(['__repr__', '__reduce__'],
                               lambda s: 'no_change'))()


def is_list_of(x, typ):
    return isinstance(x, list) and all(isinstance(i, typ) for i in x)


def is_set_of(x, typ):
    return isinstance(x, set) and all(isinstance(i, typ) for i in x)


def is_dict_of(x, ktyp, vtyp):
    return (isinstance(x, dict) and
            all(isinstance(k, ktyp) for k in x.keys()) and
            all(isinstance(v, vtyp) for v in x.values()))


class EnumMeta(type):
    def __init__(cls, name, parents, dct):
        cls._values = tuple(ensure_unicode(v) for v in cls._values)
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
        if not isinstance(x, string):
            raise TypeError("Expected 'str' or %r" % cls.__name__)
        x = ensure_unicode(x).upper()
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
                (isinstance(other, string) and self._value == other.upper()))

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash(self._value)

    @classmethod
    def values(cls):
        """The constants of this enum type, in the order they are declared."""
        return cls._values


def _convert(x, method, *args):
    if hasattr(x, method):
        return getattr(x, method)(*args)
    typ = type(x)
    if typ in (list, set, tuple):
        return [_convert(i, method, *args) for i in x]
    elif typ is dict:
        return {k: _convert(v, method, *args) for k, v in x.items()}
    elif typ is datetime:
        return datetime_to_millis(x)
    elif isinstance(x, Enum):
        return str(x)
    else:
        return x


def rebuild(cls, params):
    return cls(**params)


class Base(object):
    """Base class for typed objects"""
    __slots__ = ()

    def __eq__(self, other):
        return (type(self) == type(other) and
                all(getattr(self, k) == getattr(other, k)
                    for k in self._get_params()))

    def __ne__(self, other):
        return not (self == other)

    def __reduce__(self):
        params = {p: getattr(self, p) for p in self._get_params()}
        return (rebuild, (type(self), params))

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
                msg = '%s must be a %s, or None'
            else:
                msg = '%s must be a %s'
            raise context.TypeError(msg % (field, typename(type)))

    def _check_is_set_of(self, field, type):
        if not is_set_of(getattr(self, field), type):
            msg = "%s must be a set of %s"
            raise context.TypeError(msg % (field, typename(type)))

    def _check_is_list_of(self, field, type):
        if not is_list_of(getattr(self, field), type):
            msg = "%s must be a list of %s"
            raise context.TypeError(msg % (field, typename(type)))

    def _check_is_dict_of(self, field, key, val):
        if not is_dict_of(getattr(self, field), key, val):
            msg = "%s must be a dict of %s -> %s"
            raise context.TypeError(msg % (field, typename(key), typename(val)))

    def _check_is_bounded_int(self, field, min=0, nullable=False):
        x = getattr(self, field)
        self._check_is_type(field, integer, nullable=nullable)
        if x is not None and x < min:
            raise context.ValueError("%s must be >= %d" % (field, min))


class ProtobufMessage(Base):
    __slots__ = ()

    @classmethod
    def from_protobuf(cls, msg):
        """Create an instance from a protobuf message."""
        if not isinstance(msg, cls._protobuf_cls):
            raise TypeError("Expected message of type "
                            "%r" % cls._protobuf_cls.__name__)
        kwargs = {k: getattr(msg, k) for k in cls._get_params()}
        return cls(**kwargs)

    def to_protobuf(self):
        """Convert object to a protobuf message"""
        self._validate()
        kwargs = {k: _convert(getattr(self, k), 'to_protobuf')
                  for k in self._get_params()}
        return self._protobuf_cls(**kwargs)


class Specification(ProtobufMessage):
    """Base class for objects that are part of the specification"""
    __slots__ = ()

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
        return yaml.safe_dump(self.to_dict(skip_nulls=skip_nulls),
                              default_flow_style=False)
