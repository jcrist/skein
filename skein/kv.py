from __future__ import (absolute_import as _,
                        print_function as _,
                        division as _)

import textwrap as _textwrap
import threading as _threading
import weakref as _weakref
from collections import (namedtuple as _namedtuple,
                         deque as _deque,
                         OrderedDict as _OrderedDict)
from functools import wraps as _wraps

import grpc as _grpc

from . import proto as _proto
from .compatibility import (bind_method as _bind_method,
                            Queue as _Queue,
                            string as _string,
                            Mapping as _Mapping,
                            MutableMapping as _MutableMapping)
from .exceptions import (ApplicationError as _ApplicationError,
                         ConnectionError as _ConnectionError)
from .model import (
    container_instance_from_string as _container_instance_from_string,
    container_instance_to_string as _container_instance_to_string)

from .objects import (Base as _Base,
                      Enum as _Enum,
                      no_change as _no_change)


__all__ = ('KeyValueStore',
           'ValueOwnerPair',
           'TransactionResult',
           'Condition', 'is_condition',
           'Operation', 'is_operation',
           'value', 'owner', 'comparison',
           'count', 'list_keys',
           'exists', 'missing',
           'get', 'get_prefix', 'get_range',
           'pop', 'pop_prefix', 'pop_range',
           'discard', 'discard_prefix', 'discard_range',
           'put', 'swap',
           'EventType', 'Event', 'EventFilter', 'EventQueue')


class Operation(_Base):
    """Base class for all key-value store operations"""
    __slots__ = ()

    def _build_operation(self):
        raise NotImplementedError  # pragma: no cover

    def _build_result(self, result):
        raise NotImplementedError  # pragma: no cover


class Condition(_Base):
    """Base class for all key-value store conditional expressions"""
    __slots__ = ()

    def _build_condition(self):
        raise NotImplementedError  # pragma: no cover


def is_operation(obj):
    """Return if ``obj`` is a valid skein key-value store operation"""
    return isinstance(obj, Operation)


def is_condition(obj):
    """Return if ``x`` is a valid skein key-value store condition"""
    return isinstance(obj, Condition)


class EventType(_Enum):
    """Event types to listen on.

    Attributes
    ----------
    ALL : EventType
        All events.
    PUT : EventType
        Only ``PUT`` events.
    DELETE : EventType
        Only ``DELETE`` events.
    """
    _values = ('ALL', 'PUT', 'DELETE')


class EventFilter(object):
    """An event filter.

    Specifies a subset of events to watch for. May specify one of ``key``,
    ``prefix``, or ``start``/``end``. If no parameters are
    provided, selects all events.

    Parameters
    ----------
    key : str, optional
        If present, only events from this key will be selected.
    prefix : str, optional
        If present, only events with this key prefix will be selected.
    start : str, optional
        If present, specifies the lower bound of the key range, inclusive.
    end : str, optional
        If present, specifies the upper bound of the key range, exclusive.
    event_type : EventType, optional.
        The type of event. Default is ``'ALL'``
    """
    __slots__ = ('_start', '_end', '_event_type')

    def __init__(self, key=None, prefix=None, start=None,
                 end=None, event_type=None):
        has_key = key is not None
        has_prefix = prefix is not None
        has_range = start is not None or end is not None
        if (has_key + has_prefix + has_range) > 1:
            raise ValueError("Must specify at most one of `key`, `prefix`, or "
                             "`start`/`end`")
        if has_key:
            if not isinstance(key, _string):
                raise TypeError("key must be a string")
            start = key
            end = key + '\x00'
        elif has_prefix:
            if not isinstance(prefix, _string):
                raise TypeError("prefix must be a string")
            start = prefix
            end = _next_key(prefix)
        else:
            if not (start is None or isinstance(start, _string)):
                raise TypeError("start must be a string or None")
            if not (end is None or isinstance(end, _string)):
                raise TypeError("end must be a string or None")

        event_type = (EventType.ALL if event_type is None
                      else EventType(event_type))

        self._start = start
        self._end = end
        self._event_type = event_type

    start = property(lambda s: s._start)
    end = property(lambda s: s._end)
    event_type = property(lambda s: s._event_type)

    def __repr__(self):
        return ('EventFilter(start=%r, end=%r, event_type=%r)'
                % (self._start, self._end, self._event_type))

    def __reduce__(self):
        return (EventFilter, (None, None, self.start,
                              self.end, self.event_type))

    def __eq__(self, other):
        return (type(self) is type(other) and
                self.start == other.start and
                self.end == other.end and
                self.event_type == other.event_type)

    def __ne__(self, other):
        return not (self == other)

    def __hash__(self):
        return hash((self.start, self._end, self.event_type))


class Event(_namedtuple('Event',
                        ['key', 'result', 'event_type', 'event_filter'])):
    """An event in the key-value store.

    Parameters
    ----------
    key : str
        The key affected.
    result : ValueOwnerPair or None
        The value and owner for the key.  None if a ``'DELETE'`` event.
    event_type : EventType
        The type of event.
    event_filter : EventFilter
        The event filter that generated the event.
    """
    pass


class TransactionResult(_namedtuple('TransactionResult',
                                    ['succeeded', 'results'])):
    """A result from a key-value store transaction.

    Parameters
    ----------
    succeeded : bool
        Whether the transaction conditions evaluated to True.
    results : sequence
        A sequence of results from applying all operations in the transaction
        ``on_success`` or ``on_failure`` parameters, depending on whether the
        conditions evaluated to True or False.
    """
    pass


class ValueOwnerPair(_namedtuple('ValueOwnerPair', ['value', 'owner'])):
    """A (value, owner) pair in the key-value store.

    Parameters
    ----------
    value : bytes
        The value.
    owner : str or None
        The owner container_id, or None for no owner.
    """
    pass


def _value_owner_pair(kv):
    """Build a ValueOwnerPair from a KeyValue object"""
    return ValueOwnerPair(kv.value, (_container_instance_to_string(kv.owner)
                                     if kv.HasField("owner") else None))


class EventQueue(object):
    """A queue of events on the key-value store.

    Besides the normal ``Queue`` interface, also supports iteration.

    >>> for event in app.kv.events(prefix='bar'):
    ...     print(event)

    If an event falls into multiple selected filters, it will be placed in the
    event queue once for each filter. For example, ``prefix='bar'`` and
    ``key='bart'`` would both recieve events on ``key='bart'``. If a queue was
    subscribed to both events, changes to this key would be placed in the queue
    twice, once for each filter.

    All events are unsubscribed when this object is collected. Can also be used
    as a contextmanager to unsubscribe-all on ``__exit__``, or explicitly call
    ``unsubscribe_all``.
    """
    def __init__(self, kv):
        self._kv = kv
        self.filters = set()
        self._queue = _Queue()
        self._exception = None
        self._ref = _weakref.ref(self)

    def __repr__(self):
        return 'EventQueue<%d filters>' % len(self.filters)

    def __iter__(self):
        while True:
            yield self.get()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.unsubscribe_all()

    def __del__(self):
        self.unsubscribe_all()

    def _build_event_filter(self, event_filter=None, **kwargs):
        if event_filter is not None:
            if any(v is not None for v in kwargs.values()):
                raise ValueError("Cannot provide ``event_filter`` and "
                                 "other arguments")
            if not isinstance(event_filter, EventFilter):
                raise TypeError("event_filter must be an EventFilter")
        else:
            event_filter = EventFilter(**kwargs)
        return event_filter

    @_wraps(_Queue.get)
    def get(self, block=True, timeout=None):
        if self._exception is not None:
            raise self._exception
        out = self._queue.get(block=block, timeout=timeout)
        if isinstance(out, Exception):
            self._exception = out
            raise out
        return out

    @_wraps(_Queue.put)
    def put(self, item, block=True, timeout=None):
        self._queue.put(item, block=block, timeout=timeout)

    def subscribe(self, event_filter=None, key=None, prefix=None,
                  start=None, end=None, event_type=None):
        """Subscribe to an event filter.

        May provide either an explicit event filter, or provide arguments to
        create a new one and add it to the queue. In either case, the event
        filter is returned.

        If no arguments are provided, subscribes to all events.

        Parameters
        ----------
        event_filter : EventFilter
            An explicit EventFilter. If provided, no other keyword arguments
            may be provided.
        key : str, optional
            If present, only events from this key will be selected.
        prefix : str, optional
            If present, only events with this key prefix will be selected.
        start : str, optional
            If present, specifies the lower bound of the key range, inclusive.
        end : str, optional
            If present, specifies the upper bound of the key range, exclusive.
        event_type : EventType, optional.
            The type of event. Default is ``'ALL'``.

        Returns
        -------
        EventFilter
        """
        event_filter = self._build_event_filter(event_filter=event_filter,
                                                key=key,
                                                prefix=prefix,
                                                start=start,
                                                end=end,
                                                event_type=event_type)
        if event_filter in self.filters:
            return event_filter
        self._kv._add_subscription(self, event_filter)
        self.filters.add(event_filter)
        return event_filter

    def unsubscribe(self, event_filter=None, key=None, prefix=None,
                    start=None, end=None, event_type=None):
        """Unsubscribe from an event filter.

        May provide either an explicit event filter, or provide arguments to
        create a new one and add it to the queue.

        If no arguments are provided, unsubscribes from a filter of all events.

        A ``ValueError`` is raised if the specified filter isn't currently
        subscribed to.

        Parameters
        ----------
        event_filter : EventFilter
            An explicit EventFilter. If provided, no other keyword arguments
            may be provided.
        key : str, optional
            If present, only events from this key will be selected.
        prefix : str, optional
            If present, only events with this key prefix will be selected.
        start : str, optional
            If present, specifies the lower bound of the key range, inclusive.
        end : str, optional
            If present, specifies the upper bound of the key range, exclusive.
        event_type : EventType, optional.
            The type of event. Default is ``'ALL'``.

        Returns
        -------
        EventFilter
        """
        event_filter = self._build_event_filter(event_filter=event_filter,
                                                key=key,
                                                prefix=prefix,
                                                start=start,
                                                end=end,
                                                event_type=event_type)
        if event_filter not in self.filters:
            raise ValueError("not currently subscribed to %r" % (event_filter,))
        self._kv._remove_subscription(self, event_filter)
        self.filters.discard(event_filter)
        return event_filter

    def unsubscribe_all(self):
        """Unsubscribe from all event filters"""
        # make a copy while iterating
        for filter in list(self.filters):
            self.unsubscribe(filter)


class KeyValueStore(_MutableMapping):
    """The Skein Key-Value store.

    Used by applications to coordinate configuration and global state.
    """
    def __init__(self, client):
        # The application client
        self._client = client

        # A lock to secure internal state
        self._lock = _threading.Lock()

        # Event listener thread is None initially
        self._event_listener_started = False

    def _ensure_event_listener(self):
        with self._lock:
            if not self._event_listener_started:
                # A queue of input requests, used by the input iterator
                self._input_queue = _Queue()

                # A deque of event filters waiting to be paired with a watch_id
                self._create_deque = _deque()

                # Mapping of watch_id to EventFilter
                self._id_to_filter = {}

                # Mapping of EventFilter to watch_id
                self._filter_to_id = {}

                # Mapping of EventFilter to a set of EventQueue weakrefs
                self._filter_to_queues = {}

                # Mapping of EventFilter to threading.Event/True
                self._filter_subscribed = {}

                # The output from the watch stream, processed by the handler loop
                self._output_iter = self._client._call('Watch', self._input_iter())

                # A thread for managing outputs from the watch stream
                self._event_listener = _threading.Thread(target=self._handler_loop)
                self._event_listener.daemon = True
                self._event_listener.start()

                self._event_listener_started = True

    def _handler_loop(self):
        while True:
            try:
                resp = next(self._output_iter)
            except _grpc.RpcError as _exc:
                exc = _exc
            else:
                exc = None

            if exc is not None:
                # Stream errored (all exceptions are unexpected)
                # Shutdown stream state and notify all event queues
                exc = (_ConnectionError("Unable to connect to application")
                       if exc.code() == _grpc.StatusCode.UNAVAILABLE
                       else _ApplicationError(exc.details()))
                self._input_queue.put((None, None))
                with self._lock:
                    all_qs = set(q for qs in self._filter_to_queues.values()
                                 for q in qs)
                    for eq_ref in all_qs:
                        try:
                            eq_ref().put(exc)
                        except AttributeError:  # pragma: nocover
                            # reference dropped, but __del__ not yet run
                            # this is hard to test, so no covererage
                            pass
                    break

            watch_id = resp.watch_id

            if resp.type == _proto.WatchResponse.CREATE:
                event_filter = self._create_deque.popleft()
                with self._lock:
                    self._id_to_filter[watch_id] = event_filter
                    self._filter_to_id[event_filter] = watch_id
                    self._filter_subscribed[event_filter].set()
                    self._filter_subscribed[event_filter] = True
            elif resp.type != _proto.WatchResponse.CANCEL:
                event_type = EventType(_proto.WatchResponse.Type.Name(resp.type))
                with self._lock:
                    event_filter = self._id_to_filter.get(watch_id)
                    if event_filter is not None:
                        for kv in resp.event:
                            event = Event(key=kv.key,
                                          result=(_value_owner_pair(kv)
                                                  if event_type == EventType.PUT
                                                  else None),
                                          event_filter=event_filter,
                                          event_type=event_type)
                            for eq_ref in self._filter_to_queues[event_filter]:
                                try:
                                    eq_ref().put(event)
                                except AttributeError:  # pragma: nocover
                                    # reference dropped, but __del__ not yet run
                                    # this is hard to test, so no covererage
                                    pass

    def _input_iter(self):
        while True:
            req, event_filter = self._input_queue.get()
            if req is None:
                break  # shutdown flag
            elif event_filter is not None:
                # Create request, enque the event filter for later
                self._create_deque.append(event_filter)
            yield req

    def _add_subscription(self, event_queue, event_filter):
        with self._lock:
            eq_ref = event_queue._ref
            if event_filter in self._filter_to_queues:
                self._filter_to_queues[event_filter].add(eq_ref)
                subscribed = self._filter_subscribed[event_filter]
                if subscribed is True:
                    return
            else:
                self._filter_to_queues[event_filter] = {eq_ref}
                subscribed = self._filter_subscribed[event_filter] = _threading.Event()
                req = _proto.WatchCreateRequest(start=event_filter.start,
                                                end=event_filter.end,
                                                event_type=str(event_filter.event_type))
                self._input_queue.put((_proto.WatchRequest(create=req), event_filter))
        # Wait for subscription to occur
        subscribed.wait()

    def _remove_subscription(self, event_queue, event_filter):
        with self._lock:
            eq_ref = event_queue._ref
            if event_filter in self._filter_to_queues:
                self._filter_to_queues[event_filter].discard(eq_ref)
                if not self._filter_to_queues[event_filter]:
                    # Last queue registered for this filter - issue cancel
                    watch_id = self._filter_to_id[event_filter]
                    req = _proto.WatchCancelRequest(watch_id=watch_id)
                    self._input_queue.put((_proto.WatchRequest(cancel=req), None))
                    # Cleanup state
                    del self._id_to_filter[watch_id]
                    del self._filter_to_id[event_filter]
                    del self._filter_to_queues[event_filter]
                    del self._filter_subscribed[event_filter]

    def event_queue(self):
        """Create a new EventQueue subscribed to no events.

        Examples
        --------
        Subscribe to events starting with ``'foo'`` or ``'bar'``.

        >>> foo = skein.kv.EventFilter(prefix='foo')
        >>> bar = skein.kv.EventFilter(prefix='bar')
        >>> queue = app.kv.event_queue()              # doctest: skip
        >>> queue.subscribe(foo)                      # doctest: skip
        >>> queue.subscribe(bar)                      # doctest: skip
        >>> for event in queue:                       # doctest: skip
        ...     if event.filter == foo:
        ...         print("foo event")
        ...     else:
        ...         print("bar event")
        """
        self._ensure_event_listener()
        return EventQueue(self)

    def events(self, event_filter=None, key=None, prefix=None,
               start=None, end=None, event_type=None):
        """Shorthand for creating an EventQueue and adding a single filter.

        May provide either an explicit event filter, or provide arguments to
        create a new one and add it to the queue.

        If no arguments are provided, creates a queue subscribed to all events.

        Parameters
        ----------
        event_filter : EventFilter
            An explicit EventFilter. If provided, no other keyword arguments
            may be provided.
        key : str, optional
            If present, only events from this key will be selected.
        prefix : str, optional
            If present, only events with this key prefix will be selected.
        start : str, optional
            If present, specifies the lower bound of the key range, inclusive.
        end : str, optional
            If present, specifies the upper bound of the key range, exclusive.
        event_type : EventType, optional.
            The type of event. Default is ``'ALL'``.

        Returns
        -------
        EventQueue

        Examples
        --------
        Subscribe to all events with prefix ``'foo'``:

        >>> for event in app.kv.events(prefix='foo'):  # doctest: skip
        ...     if event.type == 'PUT':
        ...         print("PUT<key=%r, value=%r>" % (event.key, event.value))
        ...     else:  # DELETE
        ...         print("DELETE<key=%r>" % event.key)
        PUT<key='foo', value=b'bar'>
        PUT<key='food', value=b'biz'>
        DELETE<key='food'>
        PUT<key='foo', value=b'changed'>
        """
        queue = self.event_queue()
        queue.subscribe(event_filter=event_filter,
                        key=key,
                        prefix=prefix,
                        start=start,
                        end=end,
                        event_type=event_type)
        return queue

    def _apply_op(self, op, timeout=None):
        req = op._build_operation()
        resp = self._client._call(op._rpc, req, timeout=timeout)
        return op._build_result(resp)

    def __iter__(self):
        return iter(self.list_keys())

    def __len__(self):
        return self.count()

    def __setitem__(self, key, value):
        self.put(key, value=value)

    def __getitem__(self, key):
        result = self.get(key)
        if result is None:
            raise KeyError(key)
        return result

    def __delitem__(self, key):
        if not self.discard(key):
            raise KeyError(key)

    def __contains__(self, key):
        return self.exists(key)

    def wait(self, key, return_owner=False):
        """Get the value associated with a single key, blocking until the key
        exists if not present.

        Parameters
        ----------
        key : str
            The key to get.
        return_owner : bool, optional
            If True, the owner will also be returned along with the value. Default
            is False.

        Returns
        -------
        bytes or ValueOwnerPair
        """
        with self.events(key=key, event_type='put') as event_queue:
            # We `get` after creating an event queue to avoid the following
            # race condition:
            # - get fails to find key
            # - key is created by different client
            # - event queue is created, waiting for PUT events
            res = self.get(key=key, return_owner=return_owner)
            if ((return_owner and res.value is not None) or
                    (not return_owner and res is not None)):
                return res

            event = event_queue.get()

        return event.result if return_owner else event.result.value

    def clear(self):
        self.discard_range()

    def setdefault(self, key, default):
        """Get the value associated with key, setting it to default if
        not present.

        This transaction happens atomically on the key-value store.

        Parameters
        ----------
        key : str
            The key
        default : bytes
            The default value to set if the key isn't present.

        Returns
        -------
        value : bytes
        """
        res = self.transaction(conditions=[exists(key)],
                               on_success=[get(key)],
                               on_failure=[put(key, default)])
        return res.results[0] if res.succeeded else default

    def update(self, *args, **kwargs):
        """Update the key-value store with multiple key-value pairs atomically.

        Parameters
        ----------
        arg : mapping or iterable, optional
            Either a mapping or an iterable of ``(key, value)``.
        **kwargs
            Extra key-value pairs to set. Semantically these are applied after
            any present in ``arg``, and will thus override any intersecting
            keys between the two.
        """
        if len(args) > 1:
            raise TypeError('update expected at most 1 arguments, got %d' %
                            len(args))
        if args:
            other = args[0]
            if isinstance(other, _Mapping):
                ops = [put(k, v) for k, v in other.items()]
            elif hasattr(other, "keys"):
                ops = [put(k, other[k]) for k in other.keys()]
            else:
                ops = [put(k, v) for k, v in other]
        else:
            ops = []
        ops.extend(put(k, v) for k, v in kwargs.items())
        self.transaction(on_success=ops)

    def transaction(self, conditions=None, on_success=None, on_failure=None):
        """An atomic transaction on the key-value store.

        Parameters
        ----------
        conditions : Condition or sequence of Conditions, optional
            A sequence of conditions to evaluate together. The conditional
            expression succeeds if all conditions evaluate to True, and fails
            otherwise. If no conditions are provided the conditional expression
            also succeeds.
        on_success : Operation or sequence of Operation, optional
            A sequence of operations to apply if all conditions evaluate to
            True.
        on_failure : Operation or sequence of Operation, optional
            A sequence of operations to apply if any condition evaluates to
            False.

        Returns
        -------
        result : TransactionResult
            A namedtuple of (succeeded, results), where results is a list of
            results from either the ``on_success`` or ``on_failure``
            operations, depending on which branch was evaluated.

        Examples
        --------
        This implements an atomic `compare-and-swap
        <https://en.wikipedia.org/wiki/Compare-and-swap>`_ operation, a useful
        concurrency primitive. It sets ``key`` to ``new`` only if it currently
        is ``prev``:

        >>> from skein import kv
        >>> def compare_and_swap(app, key, new, prev):
        ...     result = app.kv.transaction(
        ...         conditions=[kv.value(key) == prev],  # if key == prev
        ...         on_success=[kv.put(key, new)])       # then set key = new
        ...     return result.succeeded

        >>> app.kv['key'] = b'value'  # doctest: skip

        Since ``'key'`` currently is ``b'value'``, the conditional expression
        succeeds and ``'key'`` is set to ``b'new_value'``

        >>> compare_and_swap(app, 'key', b'new_value', b'value')  # doctest: skip
        True

        Since ``'key'`` currently is ``b'value'`` and not ``b'wrong'``, the
        conditional expression fails and ``'key'`` remains unchanged.

        >>> compare_and_swap(app, 'key', b'another_value', b'wrong')  # doctest: skip
        False
        """
        conditions = conditions or []
        on_success = on_success or []
        on_failure = on_failure or []
        if not all(is_condition(c) for c in conditions):
            raise TypeError("conditions must be a sequence of Condition")
        if not all(is_operation(o) for o in on_success):
            raise TypeError("on_success must be a sequence of Operation")
        if not all(is_operation(o) for o in on_failure):
            raise TypeError("on_failure must be a sequence of Operation")

        lk = {'GetRange': 'get_range',
              'DeleteRange': 'delete_range',
              'PutKey': 'put_key'}

        def _build_req(op):
            return _proto.OpRequest(**{lk[op._rpc]: op._build_operation()})

        def _build_result(op, resp):
            return op._build_result(getattr(resp, lk[op._rpc]))

        req = _proto.TransactionRequest(
            condition=[c._build_condition() for c in conditions],
            on_success=[_build_req(o) for o in on_success],
            on_failure=[_build_req(o) for o in on_failure])

        resp = self._client._call('Transaction', req)

        ops = on_success if resp.succeeded else on_failure
        results = [_build_result(o, r) for (o, r) in zip(ops, resp.result)]
        return TransactionResult(resp.succeeded, results)


def _next_key(prefix):
    b = bytearray(prefix.encode('utf-8'))
    b[-1] += 1
    return bytes(b).decode('utf-8')


def _register_op(return_type=None):
    """Register a key-value store operator"""

    def inner(cls):
        @_wraps(cls.__init__)
        def method(self, *args, **kwargs):
            return self._apply_op(cls(*args, **kwargs))

        if cls.__doc__ is not None:
            prefix = 'A request to '
            assert cls.__doc__.startswith(prefix)
            doc = cls.__doc__[len(prefix):].strip()
            header, _, footer = doc.partition('\n\n')
            header_words = header.split()
            header_words[0] = header_words[0].capitalize()
            header = '\n'.join(_textwrap.wrap(' '.join(header_words),
                                              width=76,
                                              initial_indent="    ",
                                              subsequent_indent="    "))

            if return_type:
                returns = ("\n"
                           "\n"
                           "    Returns\n"
                           "    -------\n"
                           "    %s" % return_type)
            else:
                returns = ""

            method.__doc__ = "%s\n\n%s%s" % (header, footer, returns)

        _bind_method(KeyValueStore, cls.__name__, method)
        return cls

    return inner


class comparison(Condition):
    """A comparison of the value or owner for a specified key.

    Parameters
    ----------
    key : str
        The corresponding key.
    field : {'value', 'owner'}
        The field to compare on.
    operator : {'==', '!=', '>', '>=', '<', '<='}
        The comparison operator to use.
    rhs : bytes, str or None
        The right-hand-side of the condition expression.
        Must be a ``bytes`` if  ``field='value'``, or ``str`` or ``None`` if
        ``field='owner'``.
    """
    __slots__ = ('_key', '_field', '_operator', '_rhs', '_rhs_proto')
    _params = ('key', 'field', 'operator', 'rhs')
    _operator_lk = {'==': 'EQUAL', '!=': 'NOT_EQUAL',
                    '<': 'LESS', '<=': 'LESS_EQUAL',
                    '>': 'GREATER', '>=': 'GREATER_EQUAL'}

    def __init__(self, key, field, operator, rhs):
        if not isinstance(key, _string):
            raise TypeError("key must be a string")
        self._key = key

        if field not in {'value', 'owner'}:
            raise ValueError("field must be either 'value' or 'owner'")
        self._field = field

        if operator not in self._operator_lk:
            raise ValueError("operator must be in {'==', '!=', '<', '>', "
                             "'<=', '>='}")
        self._operator = operator

        if field == 'owner':
            if rhs is None:
                if operator not in ('==', '!='):
                    raise TypeError("Comparison (owner(%r) %s None) is "
                                    "unsupported" % (key, operator))
                self._rhs_proto = self._rhs = None
            elif isinstance(rhs, _string):
                self._rhs_proto = _container_instance_from_string(rhs)
                self._rhs = rhs
            else:
                raise TypeError("rhs must be a string or None")
        else:
            if not isinstance(rhs, bytes):
                raise TypeError("rhs must be bytes")
            self._rhs_proto = self._rhs = rhs

    key = property(lambda self: self._key)
    field = property(lambda self: self._field)
    operator = property(lambda self: self._operator)
    rhs = property(lambda self: self._rhs)

    def __repr__(self):
        return '%s(%r) %s %r' % (self._field, self.key, self.operator, self.rhs)

    def _build_condition(self):
        kwargs = {'key': self.key,
                  'operator': self._operator_lk[self.operator],
                  'field': self.field.upper(),
                  self.field: self._rhs_proto}
        return _proto.Condition(**kwargs)


class _ComparisonBuilder(_Base):
    """Base class for `value` and `owner`"""
    __slots__ = ('_key',)
    _params = ('key',)

    def __init__(self, key):
        if not isinstance(key, _string):
            raise TypeError("key must be a string")
        self._key = key

    key = property(lambda self: self._key)
    _field = property(lambda self: type(self).__name__)

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.key)

    def __eq__(self, other):
        return comparison(self.key, self._field, '==', other)

    def __ne__(self, other):
        return comparison(self.key, self._field, '!=', other)

    def __lt__(self, other):
        return comparison(self.key, self._field, '<', other)

    def __le__(self, other):
        return comparison(self.key, self._field, '<=', other)

    def __gt__(self, other):
        return comparison(self.key, self._field, '>', other)

    def __ge__(self, other):
        return comparison(self.key, self._field, '>=', other)


class value(_ComparisonBuilder):
    """Represents the value for a key, for use in transaction conditions.

    Parameters
    ----------
    key : str
        The key to lookup
    """
    pass


class owner(_ComparisonBuilder):
    """Represents the owner for a key, for use in transaction conditions.

    Parameters
    ----------
    key : str
        The key to lookup
    """
    pass


class _CountOrKeys(Operation):
    """Base class for count & keys"""
    __slots__ = ()
    _rpc = 'GetRange'

    def __init__(self, start=None, end=None, prefix=None):
        self.start = start
        self.end = end
        self.prefix = prefix
        self._validate()

    @property
    def _is_prefix(self):
        return self.prefix is not None

    @property
    def _is_range(self):
        return self.start is not None or self.end is not None

    def _validate(self):
        self._check_is_type('start', _string, nullable=True)
        self._check_is_type('end', _string, nullable=True)
        self._check_is_type('prefix', _string, nullable=True)
        if self._is_prefix and self._is_range:
            raise ValueError("Cannot specify `prefix` and `start`/`end`")

    def __repr__(self):
        typ = type(self).__name__
        if self._is_prefix:
            return '%s(prefix=%r)' % (typ, self.prefix)
        return ('%s(start=%r, end=%r)'
                % (typ, self.start, self.end))

    def _build_operation(self):
        self._validate()
        if self._is_prefix:
            return _proto.GetRangeRequest(start=self.prefix,
                                          end=_next_key(self.prefix),
                                          result_type=self._result_type)
        return _proto.GetRangeRequest(start=self.start,
                                      end=self.end,
                                      result_type=self._result_type)


@_register_op('int')
class count(_CountOrKeys):
    """A request to count keys in the key-value store.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no
        lower bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no
        upper bound will be used.
    prefix : str, optional
        If provided, will count the number keys matching this prefix.
    """
    __slots__ = ('start', 'end', 'prefix')
    _result_type = 'NONE'

    def _build_result(self, result):
        return result.count


@_register_op('list of keys')
class list_keys(_CountOrKeys):
    """A request to get a list of keys in the key-value store.

    Parameters
    ----------
    start : str, optional
        The lower bound of the key range, inclusive. If not provided no
        lower bound will be used.
    end : str, optional
        The upper bound of the key range, exclusive. If not provided, no
        upper bound will be used.
    prefix : str, optional
        If provided, will return all keys matching this prefix.
    """
    __slots__ = ('start', 'end', 'prefix')
    _result_type = 'KEYS'

    def _build_result(self, result):
        return [kv.key for kv in result.result]


class _GetOrPop(Operation):
    """Base class for get & pop"""
    __slots__ = ()

    def __init__(self, key, default=None, return_owner=False):
        self.key = key
        self.default = default
        self.return_owner = return_owner
        self._validate()

    def _validate(self):
        self._check_is_type('key', _string)
        self._check_is_type('default', bytes, nullable=True)
        self._check_is_type('return_owner', bool)

    def __repr__(self):
        return ('%s(%r, default=%r, return_owner=%r)'
                % (type(self).__name__, self.key, self.default,
                   self.return_owner))

    def _build_operation(self):
        self._validate()
        return self._proto(start=self.key,
                           end=self.key + '\x00',
                           result_type='ITEMS')

    def _build_result(self, result):
        if result.count == 0:
            if self.return_owner:
                return ValueOwnerPair(self.default, None)
            return self.default
        if self.return_owner:
            return _value_owner_pair(result.result[0])
        return result.result[0].value


@_register_op('bytes or ValueOwnerPair')
class get(_GetOrPop):
    """A request to get the value associated with a single key.

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
    __slots__ = ('key', 'default', 'return_owner')
    _proto = _proto.GetRangeRequest
    _rpc = 'GetRange'


@_register_op('bytes or ValueOwnerPair')
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
    __slots__ = ('key', 'default', 'return_owner')
    _proto = _proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


def _output_to_ordered_dict(result, return_owner=False):
    if return_owner:
        return _OrderedDict((kv.key, _value_owner_pair(kv))
                            for kv in result.result)
    return _OrderedDict((kv.key, kv.value) for kv in result.result)


class _GetOrPopPrefix(Operation):
    """Base class for (get/pop)_prefix"""
    __slots__ = ()

    def __init__(self, prefix, return_owner=False):
        self.prefix = prefix
        self.return_owner = return_owner
        self._validate()

    def _validate(self):
        self._check_is_type('prefix', _string)
        self._check_is_type('return_owner', bool)

    def __repr__(self):
        return ('%s(%r, return_owner=%r)'
                % (type(self).__name__, self.prefix, self.return_owner))

    def _build_operation(self):
        self._validate()
        return self._proto(start=self.prefix,
                           end=_next_key(self.prefix),
                           result_type='ITEMS')

    def _build_result(self, result):
        return _output_to_ordered_dict(result, self.return_owner)


@_register_op('OrderedDict')
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
    __slots__ = ('prefix', 'return_owner')
    _proto = _proto.GetRangeRequest
    _rpc = 'GetRange'


@_register_op('OrderedDict')
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
    __slots__ = ('prefix', 'return_owner')
    _proto = _proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


class _GetOrPopRange(Operation):
    """Base class for (get/pop)_prefix"""
    __slots__ = ()

    def __init__(self, start=None, end=None, return_owner=False):
        self.start = start
        self.end = end
        self.return_owner = return_owner
        self._validate()

    def _validate(self):
        self._check_is_type('start', _string, nullable=True)
        self._check_is_type('end', _string, nullable=True)
        self._check_is_type('return_owner', bool)

    def __repr__(self):
        return ('%s(start=%r, end=%r, return_owner=%r)'
                % (type(self).__name__, self.start, self.end,
                   self.return_owner))

    def _build_operation(self):
        self._validate()
        return self._proto(start=self.start,
                           end=self.end,
                           result_type='ITEMS')

    def _build_result(self, result):
        return _output_to_ordered_dict(result, self.return_owner)


@_register_op('OrderedDict')
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
    __slots__ = ('start', 'end', 'return_owner')
    _proto = _proto.GetRangeRequest
    _rpc = 'GetRange'


@_register_op('OrderedDict')
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
    __slots__ = ('start', 'end', 'return_owner')
    _proto = _proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


class _ExistsMissingDiscard(Operation):
    """Base class for exists, missing & discard"""
    __slots__ = ()

    def __init__(self, key):
        self.key = key
        self._validate()

    def _validate(self):
        self._check_is_type('key', _string)

    def __repr__(self):
        return '%s(%r)' % (type(self).__name__, self.key)

    def _build_operation(self):
        self._validate()
        return self._proto(start=self.key,
                           end=self.key + '\x00',
                           result_type='NONE')

    def _build_result(self, result):
        return result.count == 1


@_register_op('bool')
class exists(_ExistsMissingDiscard, Condition):
    """A request to check if a key exists in the key-value store.

    Parameters
    ----------
    key : str
        The key to check the presence of.
    """
    __slots__ = ('key',)
    _proto = _proto.GetRangeRequest
    _rpc = 'GetRange'

    def _build_condition(self):
        self._validate()
        return _proto.Condition(key=self.key,
                                operator='NOT_EQUAL',
                                field='VALUE',
                                value=None)


@_register_op('bool')
class missing(_ExistsMissingDiscard, Condition):
    """A request to check if a key is not in the key-value store.

    This is the inverse of ``exists``.

    Parameters
    ----------
    key : str
        The key to check the absence of.
    """
    __slots__ = ('key',)
    _proto = _proto.GetRangeRequest
    _rpc = 'GetRange'

    def _build_result(self, result):
        return result.count == 0

    def _build_condition(self):
        self._validate()
        return _proto.Condition(key=self.key,
                                operator='EQUAL',
                                field='VALUE',
                                value=None)


@_register_op('bool')
class discard(_ExistsMissingDiscard):
    """A request to discard a single key.

    Returns true if the key was present, false otherwise.

    Parameters
    ----------
    key : str
        The key to discard.
    """
    __slots__ = ('key',)
    _proto = _proto.DeleteRangeRequest
    _rpc = 'DeleteRange'


def _build_discard_result(result, return_keys=False):
    if return_keys:
        return [kv.key for kv in result.result]
    return result.count


@_register_op('int or list of keys')
class discard_prefix(Operation):
    """A request to discard all key-value pairs whose keys start with ``prefix``.

    Returns either the number of keys discarded or a list of those keys,
    depending on the value of ``return_keys``.

    Parameters
    ----------
    prefix : str
        The key prefix.
    return_keys : bool, optional
        If True, the discarded keys will be returned instead of their count.
        Default is False.
    """
    __slots__ = ('prefix', 'return_keys')
    _rpc = 'DeleteRange'

    def __init__(self, prefix, return_keys=False):
        self.prefix = prefix
        self.return_keys = return_keys
        self._validate()

    def _validate(self):
        self._check_is_type('prefix', _string)
        self._check_is_type('return_keys', bool)

    def __repr__(self):
        return ('discard_prefix(%r, return_keys=%r)' %
                (self.prefix, self.return_keys))

    def _build_operation(self):
        self._validate()
        result_type = 'KEYS' if self.return_keys else 'NONE'
        return _proto.DeleteRangeRequest(start=self.prefix,
                                         end=_next_key(self.prefix),
                                         result_type=result_type)

    def _build_result(self, result):
        return _build_discard_result(result, self.return_keys)


@_register_op('int or list of keys')
class discard_range(Operation):
    """A request to discard a range of keys.

    Returns either the number of keys discarded or a list of those keys,
    depending on the value of ``return_keys``.

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
        self._check_is_type('start', _string, nullable=True)
        self._check_is_type('end', _string, nullable=True)
        self._check_is_type('return_keys', bool)

    def __repr__(self):
        return ('discard_range(start=%r, end=%r, return_keys=%r)'
                % (self.start, self.end, self.return_keys))

    def _build_operation(self):
        self._validate()
        result_type = 'KEYS' if self.return_keys else 'NONE'
        return _proto.DeleteRangeRequest(start=self.start,
                                         end=self.end,
                                         result_type=result_type)

    def _build_result(self, result):
        return _build_discard_result(result, self.return_keys)


class _PutOrSwap(Operation):
    """Shared base class between put and swap"""
    __slots__ = ()
    _rpc = 'PutKey'

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, owner):
        if owner is _no_change:
            self._owner_proto = None
            self._owner = _no_change
        elif owner is None:
            self._owner_proto = self._owner = None
        elif isinstance(owner, _string):
            # do this before setting owner to nice python owner,
            # ensures validity check is performed beforehand
            self._owner_proto = _container_instance_from_string(owner)
            self._owner = owner
        else:
            raise TypeError("owner must be a string or None")

    def _validate(self):
        self._check_is_type('key', _string)
        if self.value is _no_change and self.owner is _no_change:
            raise ValueError("Must specify 'value', 'owner', or both")
        if self.value is not _no_change:
            self._check_is_type('value', bytes)

    def _build_operation(self):
        self._validate()
        ignore_value = self.value is _no_change
        value = None if ignore_value else self.value
        ignore_owner = self.owner is _no_change
        owner = self._owner_proto
        return _proto.PutKeyRequest(key=self.key,
                                    ignore_value=ignore_value,
                                    value=value,
                                    ignore_owner=ignore_owner,
                                    owner=owner,
                                    return_previous=self._return_previous)


@_register_op()
class put(_PutOrSwap):
    """A request to assign a value and/or owner for a single key.

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
    __slots__ = ('key', 'value', '_owner', '_owner_proto')
    _params = ('key', 'value', 'owner')
    _return_previous = False

    def __init__(self, key, value=_no_change, owner=_no_change):
        self.key = key
        self.value = value
        self.owner = owner
        self._validate()

    def __repr__(self):
        return ('put(%r, value=%r, owner=%r)'
                % (self.key, self.value, self.owner))

    def _build_result(self, result):
        return None


@_register_op('bytes or ValueOwnerPair')
class swap(_PutOrSwap):
    """A request to assign a new value and/or owner for a single key, and
    return the previous value.

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
    __slots__ = ('key', 'value', 'return_owner', '_owner', '_owner_proto')
    _params = ('key', 'value', 'owner', 'return_owner')
    _return_previous = True

    def __init__(self, key, value=_no_change, owner=_no_change,
                 return_owner=False):
        self.key = key
        self.value = value
        self.owner = owner
        self.return_owner = return_owner
        self._validate()

    def __repr__(self):
        return ('swap(%r, value=%r, owner=%r, return_owner=%r)'
                % (self.key, self.value, self.owner, self.return_owner))

    def _build_result(self, result):
        if result.HasField("previous"):
            if self.return_owner:
                return _value_owner_pair(result.previous)
            return result.previous.value
        return ValueOwnerPair(None, None) if self.return_owner else None
