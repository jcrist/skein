from __future__ import absolute_import, print_function, division

from collections import namedtuple, MutableMapping, OrderedDict

from . import proto
from .model import (container_instance_from_string,
                    container_instance_to_string)

from .objects import Base, no_change


__all__ = ('ValueOwnerPair',
           'ops',
           'count', 'keys', 'contains',
           'get', 'get_prefix', 'get_range',
           'pop', 'pop_prefix', 'pop_range',
           'discard', 'discard_prefix', 'discard_range',
           'put', 'swap')


def _next_key(prefix):
    b = bytearray(prefix.encode('utf-8'))
    b[-1] += 1
    return bytes(b).decode('utf-8')


@object.__new__
class ops(object):
    """A registry of key-value store operators"""
    pass


def register_op(cls):
    """Register a key-value store operator"""
    setattr(ops, cls.__name__, cls)
    return cls


class Operator(Base):
    """Base class for all operators"""
    pass


class ValueOwnerPair(namedtuple('ValueOwnerPair', ['value', 'owner'])):
    """A Value and owner pair in the Key-Value store.

    Parameters
    ----------
    'value': bytes
        The value.
    'owner': str or None
        The owner container_id, or None for no owner.
    """
    @classmethod
    def _from_kv_protobuf(cls, obj):
        if obj.HasField("owner"):
            owner = container_instance_to_string(obj.owner)
        else:
            owner = None
        return cls(obj.value, owner)


class _CountOrKeys(Operator):
    """Base class for count & keys"""
    __slots__ = ('range_start', 'range_end', 'prefix')
    _rpc = 'GetRange'

    def __init__(self, range_start=None, range_end=None, prefix=None):
        self.range_start = range_start
        self.range_end = range_end
        self.prefix = prefix
        self._validate()

    @property
    def _is_prefix(self):
        return self.prefix is not None

    @property
    def _is_range(self):
        return self.range_start is not None or self.range_end is not None

    def _validate(self):
        self._check_is_type('range_start', str, nullable=True)
        self._check_is_type('range_end', str, nullable=True)
        self._check_is_type('prefix', str, nullable=True)
        if self._is_prefix and self._is_range:
            raise ValueError("Cannot specify `prefix` and `range_start`/`range_end`")

    def __repr__(self):
        typ = type(self).__name__
        if self._is_prefix:
            return '%s(prefix=%r)' % (typ, self.prefix)
        return ('%s(range_start=%r, range_end=%r)'
                % (typ, self.range_start, self.range_end))

    def _to_protobuf(self):
        self._validate()
        if self._is_prefix:
            return proto.GetRangeRequest(range_start=self.prefix,
                                         range_end=_next_key(self.prefix),
                                         result_type=self._result_type)
        return proto.GetRangeRequest(range_start=self.range_start,
                                     range_end=self.range_end,
                                     result_type=self._result_type)


@register_op
class count(_CountOrKeys):
    """A request to count keys in the key-value store.

    Parameters
    ----------
    range_start : str, optional
        The lower bound of the a key range, inclusive. If not provided no
        lower bound will be used.
    range_end : str, optional
        The upper bound of the a key range, exclusive. If not provided, no
        upper bound will be used.
    prefix : str, optional
        If provided, will count the number keys matching this prefix.
    """
    _result_type = 'NONE'

    def _build_result(self, result):
        return result.count


@register_op
class keys(_CountOrKeys):
    """A request to get a list of keys in the key-value store.

    Parameters
    ----------
    range_start : str, optional
        The lower bound of the a key range, inclusive. If not provided no
        lower bound will be used.
    range_end : str, optional
        The upper bound of the a key range, exclusive. If not provided, no
        upper bound will be used.
    prefix : str, optional
        If provided, will return all keys matching this prefix.
    """
    _result_type = 'KEYS'

    def _build_result(self, result):
        return [kv.key for kv in result.result]


class _GetOrPop(Operator):
    """Base class for get & pop"""
    __slots__ = ('key', 'default', 'return_owner')

    def __init__(self, key, default=None, return_owner=False):
        self.key = key
        self.default = default
        self.return_owner = return_owner
        self._validate()

    def _validate(self):
        self._check_is_type('key', str)
        self._check_is_type('default', bytes, nullable=True)
        self._check_is_type('return_owner', bool)

    def __repr__(self):
        return ('%s(%r, default=%r, return_owner=%r)'
                % (type(self).__name__, self.key, self.default,
                   self.return_owner))

    def _to_protobuf(self):
        self._validate()
        return self._proto(range_start=self.key,
                           range_end=self.key + '\x00',
                           result_type='ITEMS')

    def _build_result(self, result):
        if result.count == 0:
            if self.return_owner:
                return ValueOwnerPair(self.default, None)
            return self.default
        if self.return_owner:
            return ValueOwnerPair._from_kv_protobuf(result.result[0])
        return result.result[0].value


@register_op
class get(_GetOrPop):
    """A request for a single key.

    Parameters
    ----------
    key : str
        The key to get.
    default : bytes or None, optional
        Default value to return if the key is not present.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    _proto = proto.GetRangeRequest
    _rpc = 'GetRange'


@register_op
class pop(_GetOrPop):
    """A request to remove a single key and return its corresponding value.

    Parameters
    ----------
    key : str
        The key to pop.
    default : bytes or None, optional
        Default value to return if the key is not present.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    _proto = proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


def _output_to_ordered_dict(result, return_owner=False):
    if return_owner:
        return OrderedDict((kv.key, ValueOwnerPair._from_kv_protobuf(kv))
                           for kv in result.result)
    return OrderedDict((kv.key, kv.value) for kv in result.result)


class _GetOrPopPrefix(Operator):
    """Base class for (get/pop)_prefix"""
    __slots__ = ('prefix', 'return_owner')

    def __init__(self, prefix, return_owner=False):
        self.prefix = prefix
        self.return_owner = return_owner
        self._validate()

    def _validate(self):
        self._check_is_type('prefix', str)
        self._check_is_type('return_owner', bool)

    def __repr__(self):
        return ('%s(%r, return_owner=%r)'
                % (type(self).__name__, self.prefix, self.return_owner))

    def _to_protobuf(self):
        self._validate()
        return self._proto(range_start=self.prefix,
                           range_end=_next_key(self.prefix),
                           result_type='ITEMS')

    def _build_result(self, result):
        return _output_to_ordered_dict(result, self.return_owner)


@register_op
class get_prefix(_GetOrPopPrefix):
    """A request to get all key-value pairs whose keys start with ``prefix``.

    Parameters
    ----------
    prefix : str
        The key prefix.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    _proto = proto.GetRangeRequest
    _rpc = 'GetRange'


@register_op
class pop_prefix(_GetOrPopPrefix):
    """A request to remove all key-value pairs whose keys start with ``prefix``,
    and return their corresponding values.

    Parameters
    ----------
    prefix : str
        The key prefix.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    _proto = proto.GetRangeRequest
    _rpc = 'DeleteRange'


class _GetOrPopRange(Operator):
    """Base class for (get/pop)_prefix"""
    __slots__ = ('start', 'end', 'return_owner')

    def __init__(self, start=None, end=None, return_owner=False):
        self.start = start
        self.end = end
        self.return_owner = return_owner
        self._validate()

    def _validate(self):
        self._check_is_type('start', str, nullable=True)
        self._check_is_type('end', str, nullable=True)
        self._check_is_type('return_owner', bool)

    def __repr__(self):
        return ('%s(start=%r, end=%r, return_owner=%r)'
                % (type(self).__name__, self.start, self.end,
                   self.return_owner))

    def _to_protobuf(self):
        self._validate()
        return self._proto(range_start=self.start,
                           range_end=self.end,
                           result_type='ITEMS')

    def _build_result(self, result):
        return _output_to_ordered_dict(result, self.return_owner)


@register_op
class get_range(_GetOrPopRange):
    """A request to get a range of keys.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no lower
        bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no upper
        bound will be used.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    _proto = proto.GetRangeRequest
    _rpc = 'GetRange'


@register_op
class pop_range(_GetOrPopRange):
    """A request to remove a range of keys and return their corresponding values.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no lower
        bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no upper
        bound will be used.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    _proto = proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


class _ContainsOrDiscard(Operator):
    """Base class for contains & discard"""
    __slots__ = ('key',)

    def __init__(self, key):
        self.key = key
        self._validate()

    def _validate(self):
        self._check_is_type('key', str)

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.key)

    def _to_protobuf(self):
        self._validate()
        return self._proto(range_start=self.key,
                           range_end=self.key + '\x00',
                           result_type='NONE')

    def _build_result(self, result):
        return result.count == 1


@register_op
class contains(_ContainsOrDiscard):
    """A request to see if a key is in the key-value store.

    Parameters
    ----------
    key : str
        The key to get.
    """
    _proto = proto.GetRangeRequest
    _rpc = 'GetRange'


@register_op
class discard(_ContainsOrDiscard):
    """A request to discard a single key.

    Parameters
    ----------
    key : str
        The key to discard.
    """
    _proto = proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


def _build_discard_result(result, return_keys=False):
    if return_keys:
        return [kv.key for kv in result.result]
    return result.count


@register_op
class discard_prefix(Operator):
    """A request to discard all key-value pairs whose keys start with ``prefix``.

    Parameters
    ----------
    prefix : str
        The key prefix.
    return_keys : bool, optional
        If True, the discarded keys will be returned instead of their count.
        Default is False.
    """
    __slots__ = ('prefix', 'return_owner')
    _rpc = 'DeleteRange'

    def __init__(self, prefix, return_keys=False):
        self.prefix = prefix
        self.return_keys = return_keys
        self._validate()

    def _validate(self):
        self._check_is_type('prefix', str)
        self._check_is_type('return_keys', bool)

    def __repr__(self):
        return ('discard_prefix(%r, return_keys=%r)' %
                (self.prefix, self.return_keys))

    def _to_protobuf(self):
        self._validate()
        result_type = 'KEYS' if self.return_keys else 'NONE'
        return proto.DeleteRangeRequest(range_start=self.prefix,
                                        range_end=_next_key(self.prefix),
                                        result_type=result_type)

    def _build_result(self, result):
        return _build_discard_result(result, self.return_keys)


@register_op
class discard_range(Operator):
    """A request to discard a range of keys.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no lower
        bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no upper
        bound will be used.
    return_keys : bool, optional
        If True, the discarded keys will be returned instead of their count.
        Default is False.
    """
    __slots__ = ('start', 'end', 'return_keys')
    _rpc = 'DeleteRange'

    def __init__(self, start=None, end=None, return_keys=False):
        self.start = start
        self.end = end
        self.return_keys = return_keys
        self._validate()

    def _validate(self):
        self._check_is_type('start', str, nullable=True)
        self._check_is_type('end', str, nullable=True)
        self._check_is_type('return_keys', bool)

    def __repr__(self):
        return ('discard_range(start=%r, end=%r, return_keys=%r)'
                % (self.start, self.end, self.return_keys))

    def _to_protobuf(self):
        self._validate()
        result_type = 'KEYS' if self.return_keys else 'NONE'
        return proto.DeleteRangeRequest(range_start=self.start,
                                        range_end=self.end,
                                        result_type=result_type)

    def _build_result(self, result):
        return _build_discard_result(result, self.return_keys)


class _PutOrSwap(Operator):
    """Shared base class between put and swap"""
    __slots__ = ('_owner', '_owner_proto')
    _rpc = 'PutKey'

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, owner):
        if owner is no_change:
            self._owner_proto = None
            self._owner = no_change
        elif owner is None:
            self._owner_proto = self._owner = None
        elif isinstance(owner, str):
            # do this before setting owner to nice python owner,
            # ensures validity check is performed beforehand
            self._owner_proto = container_instance_from_string(owner)
            self._owner = owner
        else:
            raise TypeError("owner must be a string or None")

    def _validate(self):
        self._check_is_type('key', str)
        if self.value is no_change and self.owner is no_change:
            raise ValueError("Must specify 'value', 'owner', or both")
        if self.value is not no_change:
            self._check_is_type('value', bytes)

    def _to_protobuf(self):
        self._validate()
        ignore_value = self.value is no_change
        value = None if ignore_value else self.value
        ignore_owner = self.owner is no_change
        owner = self._owner_proto
        return proto.PutKeyRequest(key=self.key,
                                   ignore_value=ignore_value,
                                   value=value,
                                   ignore_owner=ignore_owner,
                                   owner=owner,
                                   return_previous=self._return_previous)


@register_op
class put(_PutOrSwap):
    """A request to put a single key.

    Parameters
    ----------
    key : str
        The key to put.
    value : bytes, optional
        The value to put. Default is to leave value unchanged;
        an error will be raised if the key doesn't exist.
    owner : str or None, optional
        The container id to claim ownership. Provide ``None`` to set to
        no owner. Default is to leave value unchanged.
    """
    __slots__ = ('key', 'value')
    _return_previous = False

    def __init__(self, key, value=no_change, owner=no_change):
        self.key = key
        self.value = value
        self.owner = owner
        self._validate()

    def __repr__(self):
        return ('put(key=%r, value=%r, owner=%r)'
                % (self.key, self.value, self.owner))

    def _build_result(self, result):
        return None


@register_op
class swap(_PutOrSwap):
    """A request to swap a single key.

    Parameters
    ----------
    key : str
        The key to put.
    value : bytes, optional
        The value to put. Default is to leave value unchanged;
        an error will be raised if the key doesn't exist.
    owner : str or None, optional
        The container id to claim ownership. Provide ``None`` to set to
        no owner. Default is to leave value unchanged.
    return_owner : bool, optional
        If True, the owner will also be returned along with the value. Default
        is False.
    """
    __slots__ = ('key', 'value', 'return_owner')
    _return_previous = False

    def __init__(self, key, value=no_change, owner=no_change, return_owner=False):
        self.key = key
        self.value = value
        self.owner = owner
        self.return_owner = return_owner
        self._validate()

    def __repr__(self):
        return ('swap(key=%r, value=%r, owner=%r, return_owner=%r)'
                % (self.key, self.value, self.owner, self.return_owner))

    def _build_result(self, result):
        if self.return_owner:
            return ValueOwnerPair._from_kv_protobuf(result.previous)
        return result.previous.value


class KeyValueStore(MutableMapping):
    """The Skein Key-Value store.

    Used by applications to coordinate configuration and global state.
    """
    def __init__(self, client):
        self._client = client

    def _apply_op(self, op, timeout=None):
        req = op._to_protobuf()
        resp = self._client._call(op._rpc, req, timeout=timeout)
        return op._build_result(resp)

    def __iter__(self):
        return iter(self._apply_op(ops.keys()))

    def __len__(self):
        return self._apply_op(ops.count())

    def __setitem__(self, key, value):
        self._apply_op(ops.put(key, value=value))

    def __getitem__(self, key):
        result = self._apply_op(ops.get(key))
        if result is None:
            raise KeyError(key)
        return result

    def __delitem__(self, key):
        if not self._apply_op(ops.discard(key)):
            raise KeyError(key)

    def __contains__(self, key):
        return self._apply_op(ops.contains(key))

    def clear(self):
        self._apply_op(ops.discard_range())
