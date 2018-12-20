API Docs
========

.. currentmodule:: skein


Client
-------

.. autoclass:: Client
    :members:
    :inherited-members:
    :exclude-members: start_global_daemon, stop_global_daemon


Application Client
------------------

.. autoclass:: ApplicationClient
    :members:
    :inherited-members:


Runtime Properties
------------------

.. autodata:: properties


Key Value Store
---------------

.. autoclass:: skein.kv.KeyValueStore
    :members:
    :inherited-members:

.. autoclass:: skein.kv.ValueOwnerPair

.. autoclass:: skein.kv.count
    :members:

.. autoclass:: skein.kv.count
    :members:

.. autoclass:: skein.kv.list_keys
    :members:

.. autoclass:: skein.kv.exists
    :members:

.. autoclass:: skein.kv.missing
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

.. autoclass:: skein.kv.TransactionResult

.. autoclass:: skein.kv.value
    :members:

.. autoclass:: skein.kv.owner
    :members:

.. autoclass:: skein.kv.comparison
    :members:

.. autofunction:: skein.kv.is_condition

.. autofunction:: skein.kv.is_operation

.. autoclass:: skein.kv.EventType
    :members:

.. autoclass:: skein.kv.Event

.. autoclass:: skein.kv.EventFilter

.. autoclass:: skein.kv.EventQueue
    :members:


Web UI
------

.. autoclass:: skein.ui.WebUI
    :members:
    :member-order: bysource

.. autoclass:: skein.ui.ProxiedPage
    :members:


Application Specification
-------------------------

.. autoclass:: ApplicationSpec
    :members:
    :inherited-members:

.. autoclass:: Master
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

.. autoclass:: ACLs
    :members:
    :inherited-members:

.. autoclass:: Security
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

.. autoexception:: ConnectionError
    :show-inheritance:

.. autoexception:: DriverNotRunningError
    :show-inheritance:

.. autoexception:: ApplicationNotRunningError
    :show-inheritance:

.. autoexception:: DriverError
    :show-inheritance:

.. autoexception:: ApplicationError
    :show-inheritance:
