Debugging
=========

Debugging YARN applications can be tricky, as you often don't have access to
the remote nodes to debug failures. This page provides several tips for what to
do when you encounter failures.


Accessing the Skein Web UI
--------------------------

For running applications, the Skein `Web UI <web-ui.html>`__ can provide useful
information including:

- What services are currently running in the application
- Status of all current and past containers
- Live links to logs for each service (these are especially useful)
- Key-value pairs in the `Key-Value Store <key-value-store.html>`__

For more information, see the `Web UI docs <web-ui.html>`__.


Accessing the Application Logs
------------------------------

When an application finishes, its logs are (usually) aggregated and made
available. They can be accessed using the ``yarn logs`` cli command.

.. code-block:: console

    $ yarn logs -applicationId <Application ID>

The logs contain the ``stdout`` and ``stderr`` for each service, as well as the
application master. This is a good first place to look when encountering an
unexpected failure or bug.


Useful Things to Log
--------------------

Since you often don't have access to the worker nodes, it can be useful to log
the full container environment before executing any application specific
commands. This includes all localized paths, and all environment variables.
Since ``skein`` accepts multiple commands for each service, this is easy to do.

.. code-block:: console

    services:
      my_service:
        commands:
          # List all local files, including file types
          - ls -l
          # List all environment variables and their values
          - env
          # Application specific commands...


It's also useful to log application logic as your application progresses. This
could as simple as periodic ``print`` statements, or using the standard
library's `logging module <https://docs.python.org/3/library/logging.html>`_.


Start a Remote IPython Kernel on the Container
----------------------------------------------

As a last resort, it can be useful to Skein's remote `IPython
<https://ipython.org/>`_ kernel recipe to start an IPython kernel on the
failing container, and connect to the kernel to debug locally. Refer to the
:doc:`recipe documentation <recipes-ipython-kernel>` for more information.
