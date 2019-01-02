Key-Value Store
===============

Every Skein application contains a `key-value store
<https://en.wikipedia.org/wiki/Key-value_database>`_ running on the application
master. This provides a way for containers to share runtime configuration
parameters (e.g. service addresses), as well as coordinate state between
containers.

Skein takes inspiration from other key-value stores (especially `etcd
<https://coreos.com/etcd/>`_), with simplifications and specializations for
YARN as seen fit.

.. contents:: :local:

Overview
--------

- Keys are UTF-8 encoded strings (``str`` in Python), with values as arbitrary
  raw bytes (``bytes`` in Python).

- The lifetime of a key or keys can be tied to the lifetime of a YARN container
  (referred to as the "owner" of the key). When the container completes, all
  keys owned by it will be deleted.

- Operations work on single keys, ranges of keys, or keys starting with a
  specific prefix (e.g. all keys beginning with ``"foo"``).

- Atomic bulk transactions are supported. Several operations can be applied in
  a single transaction, potentially based on a set of conditions.

- Clients can subscribe to a range (or ranges) of keys for changes and react
  accordingly.


Walkthrough
-----------

To illustrate all the functionality provided by the key-value store, and how it
might be useful, the remainder of this page is a complete runnable example. To
start off, we'll create a test application with a single container that sleeps
forever.

.. code-block:: python

    >>> import skein
    >>> spec = skein.ApplicationSpec.from_yaml("""
    ... name: example
    ... services:
    ...   sleeper:
    ...     resources:
    ...       memory: 128 MiB
    ...       vcores: 1
    ...     script: |
    ...       sleep infinity
    ... """)
    >>> client = skein.Client()
    >>> app = client.submit_and_connect(spec)


KeyValueStore Basics
~~~~~~~~~~~~~~~~~~~~

.. currentmodule:: skein.kv

The :class:`KeyValueStore` class implements the standard ``MutableMapping``
interface. As mentioned above, all keys must be ``str``, and all values must be
``bytes``. All of these methods happen *atomically* in a single transaction,
meaning there are no race conditions between one client checking state and
another client modifying it.

To start off, let's put some data on the key-value store. We'll use the
:func:`KeyValueStore.update` method to put multiple values in a single
transaction:

.. code-block:: python

    # Originally the key-value store is empty
    >>> len(app.kv)
    0

    # Add some data
    >>> app.kv.update({'apples': b'1.22',
                       'apricots': b'8.99',
                       'bananas': b'0.56',
                       'oranges': b'7.96'})

    # Now there are 4 items
    >>> len(app.kv)
    4

As with other mutable mappings, you can get, set, and delete values using the
standard interface:

.. code-block:: python

    # Get the value for 'apples'
    >>> app.kv['apples']
    b'1.22'

    # Add a new value for 'grapes'
    >>> app.kv['grapes'] = b'2.88'

    # Check if 'grapes' is present
    >>> 'grapes' in app.kv
    True

    # Update the value for 'grapes'
    >>> app.kv['grapes'] = b'2.61'

    # Delete 'grapes'
    >>> del app.kv['grapes']


Iteration
~~~~~~~~~

Since :class:`KeyValueStore` supports the ``MutableMapping`` interface, things
like iterating over the keys work as expected. The iteration order is
alphabetical based on keys, as the mapping is sorted internally (more on that
later).

.. code-block:: python

    >>> list(app.kv)
    ['apples', 'apricots', 'bananas', 'oranges']


However, sometimes using the standard iteration based protocols can unsafe or
inefficient. Since the key-value store is a shared mapping with potentially
many clients making changes concurrently, you need need to think about
race-conditions when passing the key-value store to functions that might make
use of iteration in a potentially unsafe way. For example, calling ``dict`` on
the key-value store object works as expected, but is not the best way to get
all key-value pairs for two reasons:

- Interally, ``dict`` iterates over the keys in the provided mapping. It's
  implemented something like:

  .. code-block:: python

   def dict(mapping):
       out = {}
       for k in mapping:
           out[k] = mapping[k]
       return out

  Since iterating over the keys and copying out their values happens over
  multiple operations, other clients may have changed the key-value store
  between operations, meaning the output ``dict`` may represent an inconsistent
  state.

- Additionally, calling ``dict`` on the mapping results in multiple operations,
  reducing efficiency as it makes more calls than required.

For these reasons, it's better to use methods that operate on `Ranges and
Prefixes`_ instead:

.. code-block:: python

    >>> dict(app.kv)  # unsafe and inefficient
    {'apples': b'1.22',
     'apricots': b'8.99',
     'bananas': b'0.56',
     'oranges': b'7.96'}

    >>> app.kv.get_range()  # safe and efficient
    OrderedDict([('apples', b'1.22'),
                 ('apricots', b'8.99'),
                 ('bananas', b'0.56'),
                 ('oranges', b'7.96')])


Ranges and Prefixes
~~~~~~~~~~~~~~~~~~~

Key-value pairs are sorted internally, allowing for efficient operations on
ranges of keys. Ranges are start *inclusive* and end *exclusive*. If start or
end are unspecified, the range is unbounded on that side. Many methods have
range versions, allowing for bulk operations in a single call. Operations on
keys starting with a certain "prefix" are also supported. These are special
cases of range operations (since ``get_prefix("abc")`` is equivalent to
``get_range(start="abc", end="abd")``).

.. code-block:: python

    # Lookup all keys starting with "ap"
    >>> app.kv.get_prefix("ap")
    OrderedDict([('apples', b'1.22'),
                 ('apricots', b'8.99')])

    # Lookup all keys in ["apricots", "oranges")
    >>> app.kv.get_range(start="apricots", end="oranges")
    OrderedDict([('apricots', b'8.99'),
                 ('bananas', b'0.56')])

    # Pop all keys after "m"
    >>> removed = app.kv.pop_range(start="m")
    >>> removed
    OrderedDict([('oranges', b'7.96')])

    >>> len(app.kv)  # the key was removed
    3

    # Put it back
    >>> app.kv.update(removed)
    >>> len(app.kv)
    4


Additional Methods
~~~~~~~~~~~~~~~~~~

Besides the ``MutableMapping`` interface, the :class:`KeyValueStore` class
provides a larger set of methods for common operations. Each of these runs as a
single atomic transaction, and most have variations for a single key, a range
of keys, or a key prefix.


.. autosummary::

    KeyValueStore.count
    KeyValueStore.list_keys
    KeyValueStore.exists
    KeyValueStore.missing
    KeyValueStore.get
    KeyValueStore.get_prefix
    KeyValueStore.get_range
    KeyValueStore.pop
    KeyValueStore.pop_prefix
    KeyValueStore.pop_range
    KeyValueStore.discard
    KeyValueStore.discard_prefix
    KeyValueStore.discard_range
    KeyValueStore.put
    KeyValueStore.swap


Ownership
~~~~~~~~~

Skein's key-value store provides a key ownership model. This allows the
lifetime of a key or keys to be tied to the lifetime of a YARN container
(referred to as the "owner" of the key). When the container finishes (whether
after success, failure, or being killed by the user), any keys owned by that
container are deleted. This can be useful for tracking container lifetimes, or
implementing robust locks that are released when a container exits.

Owners are specified as *skein* container ids. These are different than their
YARN counterparts, and are strings of the form
``{service-name}_{instance-number}`` (e.g. ``myservice_1``). Container ids can
be obtained in a few ways:

- From a :class:`Container` object:

  .. code-block:: python

    >>> [c.id for c in app.get_containers()]
    ['sleeper_0']

- When running code on a container, the executing container id is available in
  the :data:`skein.properties` object:

  .. code-block:: python

    # Example of code executing in a container
    >>> import skein
    >>> skein.properties.container_id
    'myservice_10'

To set or change the owner for a key, pass a container id to
:func:`KeyVaueStore.put` or :func:`KeyValueStore.swap` via the ``owner``
keyword. Likewise the owner of a key or keys can be retrieved using the normal
``get`` methods by providing ``return_owner=True``.

.. code-block:: python

    # First scale up the 'sleeper' service to 2 containers
    >>> app.scale('sleeper', 2)
    [Container<service_name='sleeper', instance=1, state=REQUESTED>]

    # Create a new key 'grapes', and set the owner to 'sleeper_0'
    >>> app.kv.put('grapes', b'2.88', owner='sleeper_0')

    # Get the full value & owner record for 'grapes'
    >>> app.kv.get('grapes', return_owner=True)
    ValueOwnerPair(value=b'2.88', owner='sleeper_0')

    # Change the value, owner stays the same
    >>> app.kv.put('grapes', b'2.61')
    >>> app.kv.get('grapes', return_owner=True)
    ValueOwnerPair(value=b'2.61', owner='sleeper_0')

    # Change the owner, value stays the same
    >>> app.kv.put('grapes', owner='sleeper_1')
    >>> app.kv.get('grapes', return_owner=True)
    ValueOwnerPair(value=b'2.61', owner='sleeper_1')

    # To clear the owner, set to None
    >>> app.kv.put('grapes', owner=None)
    >>> app.kv.get('grapes', return_owner=True)
    ValueOwnerPair(value=b'2.61', owner=None)

When a container exits, all of its owned keys (if any) are deleted.

.. code-block:: python

    # Set the owner of grapes to 'sleeper_1'
    >>> app.kv.put('grapes', owner='sleeper_1')

    >>> 'grapes' in app.kv
    True

    # Kill the 'sleeper_1' container
    >>> app.kill_container('sleeper_1')

    >>> 'grapes' in app.kv
    False


Transactions
~~~~~~~~~~~~

Skein provides support for applying several operations in a single transaction,
potentially based on a set of conditions. This is useful for preventing race
conditions.

For example, say you want to get the value of a key if it exists, and if it
doesn't you want to set the value to a default and then return the default
(this is the ``setdefault`` method from the ``MutableMapping`` interface).

A naive implementation would suffer from a couple race conditions:

.. code-block:: python

    def naive_setdefault(self, key, default):
        """A naive implementation of MutableMapping.setdefault"""
        if key in self:
            # Race condition 1: key could be deleted by a different client
            # between the contains check and the getitem
            return self[key]
        else:
            # Race condition 2: key could be created by a different client
            # between the contains check and the setitem
            self[key] = default
            return default


The :func:`KeyValueStore.transaction` method exists to solve this problem. This
takes 3 parameters:

- A sequence of :class:`Condition` objects to evaluate together. The
  conditional expression succeeds if all conditions evaluate to True, and fails
  otherwise. If no conditions are provided the conditional expression also
  succeeds.

- A sequence of :class:`Operation` objects to apply if all conditions evaluate
  to True.

- A sequence of :class:`Operation` objects to apply if any condition evaluates
  to False.


Conditions
^^^^^^^^^^

:class:`Condition` objects are prechecks on the state of the key-value store.
They can check for the existance or absence of a key, as well as comparisons
(equality or order) on the value or owner corresponding with a key.

.. autosummary::

    exists
    missing
    value
    owner
    comparison

A few examples

.. code-block:: python

    >>> from skein import kv

    # A condition to check if 'grapes' exists
    >>> kv.exists('grapes')
    exists('grapes')

    # A condition to check if the value of 'apples' is b'123'
    >>> kv.value('apples') == b'123'
    value('apples') == b'123'

    # A condition to check if the value of 'apples' is greater than b'000'
    >>> kv.value('apples') > b'000'
    value('apples') > b'000'

    # A condition to check if 'apples' has an owner
    >>> kv.owner('apples') != None
    owner('apples') != None


Operations
^^^^^^^^^^

:class:`Operation` objects are operations to apply to the key-value store.
These coincide with the `Additional Methods`_ described above - for every
method on the :class:`KeyValueStore` there is an identical operation in the
:mod:`skein.kv` namespace.

.. autosummary::

    count
    list_keys
    exists
    missing
    get
    get_prefix
    get_range
    pop
    pop_prefix
    pop_range
    discard
    discard_prefix
    discard_range
    put
    swap

A few examples

.. code-block:: python

    >>> from skein import kv

    # An operation to get the value of 'apples'
    >>> kv.get('apples')
    get('apples', default=None, return_owner=False)

    # An operation to set the value of 'grapes' to b'123'
    >>> kv.put('grapes', b'123')
    put('grapes', value=b'123', owner=no_change)


Implementing ``setdefault``
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using the above, we can do an atomic implementation of ``setdefault``.

.. code-block:: python

    from skein import kv

    def setdefault(self, key, default):
        """A safe, atomic implementation of MutableMapping.setdefault"""
        # If the key exists, get the value, otherwise set the value to default
        res = self.transaction(conditions=[kv.exists(key)],
                               on_success=[kv.get(key)],
                               on_failure=[kv.put(key, default)])
        if res.succeeded:
            # Condition succeeded, return result of get
            return res.results[0]
        else:
            # Condition failed, key didn't exist. Return default
            return default


Event Streams
~~~~~~~~~~~~~

Skein provides a way for clients to subscribe to a range (or ranges) of keys
for changes. This can be useful for waiting for certain keys to be set, or
creating larger abstractions for coordinating workers like locks or semaphores.

.. autosummary::

    KeyValueStore.event_queue
    KeyValueStore.events
    EventQueue
    EventFilter
    Event

To subscribe to an event stream you can use either
:func:`KeyValueStore.event_queue` and :func:`KeyValueStore.events` (the latter
is shorthand for creation and subscription). Both methods return an instance of
:func:`EventQueue`, an object similar to ``Queue`` from the standard library,
with a few additional features:

- :class:`EventQueue` objects are iterable, allowing for easy looping of events:

  .. code-block:: python

    # Iterate through all events for keys starting with foobar
    for event in app.kv.events(prefix='foobar'):
        ...

- :class:`EventQueue` objects have additional methods
  :func:`EventQueue.subscribe` and :func:`EventQueue.unsubscribe`, for
  adding/removing subscriptions.

  .. code-block:: python

    eq = app.kv.event_queue()
    eq.subscribe(prefix='service_1')
    eq.subscribe(prefix='service_2')

    for event in eq:
        ...

Internally, all :class:`EventQueue` objects are filled by the same worker
thread fed by a single client connection with the server. This helps minimize
the cost of creating additional event queues or subscribing to new event
ranges. When an :class:`EventQueue` object is collected, the event stream will
be automatically unsubscribed. To manually handle unsubscription you can also
use an event queue as a contextmanager, or call
:func:`EventQueue.unsubscribe_all`.

.. code-block:: python

    # All event filters will unsubscribe on exit from this block
    with app.kv.event_queue() as eq:
        eq.subscribe(prefix='service_1')
        eq.subscribe(prefix='service_2')

        while True:
            event = eq.get()
            ...

    eq.subscribe(start='a', end='b')
    ...
    # explicitly unsubscribe from all event filters
    eq.unsubscribe_all()

Iterating through the event queue or using methods like :func:`EventQueue.get`
yield instances of :class:`Event` - a ``namedtuple`` containing

- The ``key`` affected

- The ``result`` of the change. A :class:`ValueOwnerPair` if a PUT event, or
  ``None`` if a DELETE event.

- The ``event_type`` - either :data:`EventType.PUT` or :data:`EventType.DELETE`.

- The ``event_filter`` that generated this event.

Note that if a queue subscribes to any intersecting filters, events that fall
in the intersection will be placed in that queue once for every applicable
filter.

.. code-block:: python

    # Create a new event queue
    >>> eq = app.kv.event_queue()

    # Create two filters that intersect
    >>> starts_with_apple = eq.subscribe(prefix='apple')
    >>> between_a_and_b = eq.subscribe(start='a', end='b')

    # Create a new key starting with 'apple', triggering an event
    >>> app.kv['applesauce'] = b'11.30'

    # One event is put in the queue for each filter, since both filters hit the
    # key 'applesauce'
    >>> eq.get()
    Event(key='applesauce',
          result=ValueOwnerPair(value=b'11.30', owner=None),
          event_type=EventType.PUT,
          event_filter=EventFilter(start='apple', end='applf', event_type=EventType.ALL))

    >>> eq.get()
    Event(key='applesauce',
          result=ValueOwnerPair(value=b'11.30', owner=None),
          event_type=EventType.PUT,
          event_filter=EventFilter(start='a', end='b', event_type=EventType.ALL))


Waiting for a Single Key
^^^^^^^^^^^^^^^^^^^^^^^^

One common case is waiting for a single key to be set. This might contain a
runtime configuration value that one container sets that others need, or it may
signal that a service has started up to other containers that depend on it. The
:func:`KeyValueStore.wait` method is provided for this common case. It will
block until the key is set (usually by a different container).

.. code-block:: python

    # In one container, block until the key is set, returning the value
    address = app.kv.wait('dask.scheduler.address')

    # In another container set the key
    app.kv['dask.scheduler.address'] = b'172.18.0.2:8787'


Example - Atomic Counter
------------------------

Using the primitives above, we can create larger concurrency abstractions. Here
we provide a simple recipe for an atomic shared counter.

.. code-block:: python

    import uuid
    from contextlib import contextmanager

    import skein

    class Counter(object):
        """An atomic counter class.

        Parameters
        ----------
        kv : KeyValueStore
            The application key-value store.
        key : str, optional
            The key to use to store the counter state. If absent a new counter
            is created with a unique random key.

        Examples
        --------
        Create a new counter

        >>> c = Counter(app.kv)

        Increment the counter temporarily

        >>> with c.incrementing():
        ...     do_work()

        Block until the counter reaches 0
        >>> c.wait_until_zero()
        """
        def __init__(self, kv, key=None):
            key = key or uuid.uuid4().hex
            # Get the current value, or set it to 0 if new.
            value = kv.setdefault(key, int.to_bytes(0, 4, 'little'))

            if len(value) != 4:
                # The key already exists and isn't a valid counter
                raise ValueError("Key isn't a valid counter")

            self.kv = kv
            self.key = key

        def get(self):
            return int.from_bytes(self.kv[self.key], 'little')

        @contextmanager
        def incrementing(self):
            """Increment while inside a block"""
            try:
                self.increment()
                yield
            finally:
                self.decrement()

        def increment(self):
            """Increment and return the new value"""
            while True:
                # Try to increment atomically until success. Inefficient if high
                # increment/decrement rate, but fine for low bandwidth changes
                current = self.kv[self.key]
                new = (int.from_bytes(current, 'little') + 1)
                new_bytes = new.to_bytes(4, 'little')
                # Only set the value to the new value if the value hasn't
                # changed between when we got it above and now
                succeeded, _ = self.kv.transaction(
                    conditions=[skein.kv.value(self.key) == current],
                    on_success=[skein.kv.put(self.key, new_bytes)])
                if succeeded:
                    return new

        def decrement(self):
            """Decrement and return the new value"""
            while True:
                # Try to decrement atomically until success. Inefficient if high
                # increment/decrement rate, but fine for low bandwidth changes
                current = self.kv[self.key]
                new = (int.from_bytes(current, 'little') - 1)
                new_bytes = new.to_bytes(4, 'little')
                # Try to decrement atomically until success. Inefficient if high
                # increment/decrement rate, but fine for low bandwidth changes
                succeeded, _ = self.kv.transaction(
                    conditions=[skein.kv.value(self.key) == current],
                    on_success=[skein.kv.put(self.key, new_bytes)])
                if succeeded:
                    return new

        def wait_for_zero(self):
            """Block until the counter equals 0"""
            # Watch the event stream for changes, waiting for the value to
            # decrement to 0
            for event in self.kv.events(key=self.key, event_type='PUT'):
                if int.from_bytes(event.result.value, 'little') == 0:
                    return
