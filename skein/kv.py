from __future__ import absolute_import, print_function, division

from collections import namedtuple, MutableMapping, OrderedDict

from . import proto
from .objects import Base, Enum
from .utils import implements


__all__ = ('ResultType',
           'get_range',
           'get',
           'delete_range',
           'delete',
           'put',
           'GetRangeResponse',
           'DeleteRangeResponse',
           'PutResponse')


def _next_key(prefix):
    b = bytearray(prefix.encode('utf-8'))
    b[-1] += 1
    return bytes(b).decode('utf-8')


class ResultType(Enum):
    """Enum of Result Types for get_range.

    Attributes
    ----------
    ITEMS : ResultType
        Both keys and values are returned for the selection.
    KEYS : ResultType
        Only keys are returned for the selection.
    NONE : ResultType
        Neither keys or values are returned for the selection.
    """
    _values = ('ITEMS',
               'KEYS',
               'NONE')
    _extra_mappings = {None: 'NONE'}


class get_range(Base):
    """A request to get a range of keys.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no lower
        bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no upper
        bound will be used.
    limit : int, optional
        If provided, only ``limit`` results will be returned.
    result_type : ResultType, optional
        Determines the result type. One of:
        - 'items': Both keys and values are returned for the selection
        - 'keys': Only keys are returned
        - 'none': Neither keys or items are returned, ``results`` is None.
    """
    __slots__ = ('start', 'end', 'limit', '_result_type')
    _params = ('start', 'end', 'limit', 'result_type')

    def __init__(self, start=None, end=None, limit=None, result_type='items'):
        self.start = start
        self.end = end
        self.limit = limit
        self.result_type = result_type

        self._validate()

    def _validate(self):
        self._check_is_type('start', str, nullable=True)
        self._check_is_type('end', str, nullable=True)
        self._check_is_bounded_int('limit', 1, nullable=True)
        self._check_is_type('result_type', ResultType)

    @property
    def result_type(self):
        return self._result_type

    @result_type.setter
    def result_type(self, val):
        self._result_type = ResultType(val)

    def __repr__(self):
        return 'get_range<start=%r, end=%r>' % (self.start, self.end)

    @classmethod
    @implements(Base.from_protobuf)
    def from_protobuf(cls, obj):
        return cls(start=obj.range_start,
                   end=obj.range_end,
                   limit=obj.limit or None,
                   result_type=obj.result_type)

    @implements(Base.to_protobuf)
    def to_protobuf(self):
        self._validate()
        return proto.GetRangeRequest(range_start=self.start,
                                     range_end=self.end,
                                     limit=self.limit,
                                     result_type=str(self.result_type))


def get(key):
    """A request for a single key.

    Parameters
    ----------
    key : str, optional
        The key to get.

    Returns
    -------
    req : get_range
        The get_range request.
    """
    return get_range(start=key, end=key + '\x00')


class delete_range(Base):
    """A request to delete a range of keys.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no lower
        bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no upper
        bound will be used.
    result_type : ResultType, optional
        Determines the result type. One of:
        - 'none': Neither keys or items are returned, ``results`` is None.
        - 'items': Both keys and values are returned for the previous items
          in the selection.
        - 'keys': Only keys are returned for the previous items in the selection.
    """
    __slots__ = ('start', 'end', '_result_type')
    _params = ('start', 'end', 'result_type')

    def __init__(self, start=None, end=None, result_type='items'):
        self.start = start
        self.end = end
        self.result_type = result_type

        self._validate()

    def _validate(self):
        self._check_is_type('start', str, nullable=True)
        self._check_is_type('end', str, nullable=True)
        self._check_is_type('result_type', ResultType)

    @property
    def result_type(self):
        return self._result_type

    @result_type.setter
    def result_type(self, val):
        self._result_type = ResultType(val)

    def __repr__(self):
        return 'delete_range<start=%r, end=%r>' % (self.start, self.end)

    @classmethod
    @implements(Base.from_protobuf)
    def from_protobuf(cls, obj):
        return cls(start=obj.range_start,
                   end=obj.range_end,
                   result_type=obj.result_type)

    @implements(Base.to_protobuf)
    def to_protobuf(self):
        self._validate()
        return proto.DeleteRangeRequest(range_start=self.start,
                                        range_end=self.end,
                                        result_type=str(self.result_type))


def delete(key):
    """A request to delete a single key.

    Parameters
    ----------
    key : str, optional
        The key to delete.

    Returns
    -------
    req : delete_range
        The delete_range request.
    """
    return delete_range(start=key, end=key + '\x00')


class put(Base):
    """A request to put a key-value pair.

    Parameters
    ----------
    key : str
        The key to put.
    value : bytes
        The value to put.
    return_previous : bool, optional
        If True, the previous key-value pair will be returned. Default is False.
    """
    __slots__ = ('key', 'value', 'return_previous')

    def __init__(self, key, value, return_previous=False):
        self.key = key
        self.value = value
        self.return_previous = return_previous

        self._validate()

    def _validate(self):
        self._check_is_type('key', str)
        self._check_is_type('value', bytes)
        self._check_is_type('return_previous', bool)

    def __repr__(self):
        return 'put<key=%r>' % self.key


class GetRangeResponse(namedtuple('GetRangeResponse',
                                  ['count', 'result_type', 'results'])):
    """Response from a ``get_range`` action.

    Attributes
    ----------
    count : int
        The total number of key-value pairs that matched the query.  If a limit
        was specified in the query, this number may be greater than the number
        of items actually returned.
    result_type : ResultType
        The type of the result.
    result : OrderedDict, list, or None
        The type of this attribute depends on ``result_type``:
        - ``ResultType.ITEMS`` (default): an OrderedDict of key-value pairs
          matching the query, sorted by key.
        - ``ResultType.KEYS``: a list of keys, sorted by key.
        - ``ResultType.NONE``: None.
    """
    pass


class DeleteRangeResponse(namedtuple('DeleteRangeResponse',
                                     ['count', 'result_type', 'results'])):
    """Response from a ``del_range`` action.

    Attributes
    ----------
    count : int
        The total number of key-value pairs that were deleted.
    result_type : ResultType
        The type of the result.
    previous : None or OrderedDict
        The type of this attribute depends on ``result_type``:
        - By default this is None
        - If ``prev_kv`` was ``True``, this is an OrderedDict of the deleted
          key-value pairs, sorted by key.
    """
    pass


class PutResponse(namedtuple('PutResponse', ['previous'])):
    """Response from a ``put`` action.

    Attributes
    ----------
    previous : None or tuple
        The type of this attribute depends on the ``put`` action:
        - By default this is None
        - If ``return_previous`` was ``True``, this is an tuple of
        ``(key, previous_value)``.
    """
    pass


class KeyValueStore(MutableMapping):
    """The Skein Key-Value store.

    Used by applications to coordinate configuration and global state.

    This implements the standard MutableMapping interface, along with the
    ability to "wait" for keys to be set.
    """
    def __init__(self, client):
        self._client = client

    def __iter__(self):
        req = proto.GetRangeRequest(result_type='KEYS')
        resp = self._client._call('GetRange', req)
        return (kv.key for kv in resp.kv)

    def __len__(self):
        req = proto.GetRangeRequest(result_type='NONE')
        resp = self._client._call('GetRange', req)
        return resp.count

    def __setitem__(self, key, value):
        req = proto.PutRequest(kv=proto.KeyValue(key=key, value=value))
        self._client._call('Put', req)

    @staticmethod
    def _check_slice(k):
        if k.step is not None:
            raise TypeError("Slicing with step not supported")

    @staticmethod
    def _check_limit(limit):
        if limit is not None and limit <= 0:
            raise ValueError("limit must be > 0")

    def __getitem__(self, key):
        if isinstance(key, slice):
            self._check_slice(key)
            req = proto.GetRangeRequest(range_start=key.start,
                                        range_end=key.stop)
            resp = self._client._call('GetRange', req)
            return OrderedDict((kv.key, kv.value) for kv in resp.kv)
        else:
            req = proto.GetRangeRequest(range_start=key,
                                        range_end=key + '\x00')
            resp = self._client._call('GetRange', req)
            if resp.count == 0:
                raise KeyError(key)
            return resp.kv[0].value

    def __delitem__(self, key):
        if isinstance(key, slice):
            self._check_slice(key)
            req = proto.DeleteRangeRequest(range_start=key.start,
                                           range_end=key.stop)
            self._client._call('DeleteRange', req)
        else:
            req = proto.DeleteRangeRequest(range_start=key,
                                           range_end=key + '\x00')
            resp = self._client._call('DeleteRange', req)
            if resp.count == 0:
                raise KeyError(key)

    def clear(self):
        req = proto.DeleteRangeRequest()
        self._cleint.call('DeleteRange', req)

    def pop(self, key, default=None):
        req = proto.DeleteRangeRequest(range_start=key,
                                       range_end=key + '\x00',
                                       result_type='ITEMS')
        resp = self._client._call('DeleteRange', req)
        if resp.count == 0:
            if default is None:
                raise KeyError(key)
            return default
        return resp.prev_kv[0].value

    def get_prefix(self, prefix, limit=None):
        """Get all key-value pairs whose keys start with ``prefix``.

        Parameters
        ----------
        prefix : str
            The key prefix.
        limit : int, optional
            The maximum number of key-value pairs to return. Default is all
            matching pairs.

        Returns
        -------
        submap : OrderedDict
            A sub-map of the key-value store, containing all matching key-value
            pairs, sorted by key.
        """
        self._check_limit(limit)
        req = proto.GetRangeRequest(range_start=prefix,
                                    range_end=_next_key(prefix),
                                    limit=limit)
        resp = self._client._call('GetRange', req)
        return OrderedDict((kv.key, kv.value) for kv in resp.kv)

    def delete_prefix(self, prefix):
        """Delete all key-value pairs whose keys start with ``prefix``.

        Parameters
        ----------
        prefix : str
            The key prefix.

        Returns
        -------
        count : int
            The number of items deleted
        """
        req = proto.DeleteRangeRequest(range_start=prefix,
                                       range_end=_next_key(prefix))
        resp = self._client._call('DeleteRange', req)
        return resp.count
