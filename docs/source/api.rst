API
===

.. currentmodule:: skein


Global configuration and state
------------------------------

.. autofunction:: start_global_daemon
.. autofunction:: stop_global_daemon
.. autoclass:: Security
    :members:


Client
-------

.. autoclass:: Client
    :members:
    :inherited-members:


Application
-----------

.. autoclass:: Application
    :members:
    :inherited-members:


Application Client
------------------

.. autoclass:: ApplicationClient
    :members:
    :inherited-members:


Job Specification
-----------------

.. autoclass:: Job
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
