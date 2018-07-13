API Docs
========

.. currentmodule:: skein


Security
--------

.. autoclass:: Security
    :members:


Client
-------

.. autoclass:: Client
    :members:
    :inherited-members:


Application Client
------------------

.. autoclass:: ApplicationClient
    :members:
    :inherited-members:


Key Value Store
---------------

.. autoclass:: skein.kv.KeyValueStore
    :members:

.. autoclass:: skein.kv.ValueOwnerPair
    :members:

.. autoclass:: skein.kv.count
    :members:

.. autoclass:: skein.kv.count
    :members:

.. autoclass:: skein.kv.list_keys
    :members:

.. autoclass:: skein.kv.contains
    :members:

.. autoclass:: skein.kv.get
    :members:

.. autoclass:: skein.kv.get_prefix
    :members:

.. autoclass:: skein.kv.get_range
    :members:

.. autoclass:: skein.kv.pop
    :members:

.. autoclass:: skein.kv.pop_prefix
    :members:

.. autoclass:: skein.kv.pop_range
    :members:

.. autoclass:: skein.kv.discard
    :members:

.. autoclass:: skein.kv.discard_prefix
    :members:

.. autoclass:: skein.kv.discard_range
    :members:

.. autoclass:: skein.kv.put
    :members:

.. autoclass:: skein.kv.swap
    :members:


Application Specification
-------------------------

.. autoclass:: ApplicationSpec
    :members:
    :inherited-members:

.. autoclass:: Service
    :members:
    :inherited-members:

.. autoclass:: FileType
    :members:
    :inherited-members:

.. autoclass:: FileVisibility
    :members:
    :inherited-members:

.. autoclass:: File
    :members:
    :inherited-members:

.. autoclass:: Resources
    :members:
    :inherited-members:


Application Responses
---------------------

.. autoclass:: skein.model.ApplicationState
    :members:
    :inherited-members:

.. autoclass:: skein.model.FinalStatus
    :members:
    :inherited-members:

.. autoclass:: skein.model.ApplicationReport
    :members:
    :inherited-members:

.. autoclass:: skein.model.ResourceUsageReport
    :members:
    :inherited-members:


Exceptions
----------

.. autoexception:: SkeinError
    :show-inheritance:

.. autoexception:: SkeinConfigurationError
    :show-inheritance:

.. autoexception:: ConnectionError
    :show-inheritance:

.. autoexception:: DaemonNotRunningError
    :show-inheritance:

.. autoexception:: ApplicationNotRunningError
    :show-inheritance:

.. autoexception:: DaemonError
    :show-inheritance:

.. autoexception:: ApplicationError
    :show-inheritance:
