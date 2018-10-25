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

.. code-block:: none

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


Configuring Logging in the Application Master
---------------------------------------------

Skein's `Application Master
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html>`__
uses `Log4j <http://logging.apache.org/log4j/1.2/>`__ for logging.
When debugging issues in
Skein itself, it can be useful to increase the log level to provide more
information. This can be accomplished two different ways:

- Change the logging level with the ``log_level`` field (``debug`` is a good option).
- Override the defaul log configuration by specifying a custom
  ``log4j.properties`` file in the `specification <specification.html>`__. This
  allows you to increase the logging level for component libraries as well. See
  the `Log4j documentation <https://logging.apache.org/log4j/1.2/>`__ for more
  information on configuration files.

.. code-block:: none

    master:
      # Change the log level to debug
      log_level: debug
      # OR provide a custom log configuration file
      log_config: path/to/my/log4j.properties


Configuring Logging in the Client Daemon
----------------------------------------

The :class:`skein.Client` uses a Java daemon process to communicate with
services like YARN and HDFS. If you find issues in communicating with these
services (submitting, querying, or killing applications), it may be useful to
increase logging verbosity for the skein daemon process (``debug`` is a good
option). There are a few ways to do this:

- Using the ``log_level`` keyword when creating a :class:`skein.Client`.

- Setting the ``SKEIN_LOG_LEVEL`` environment variable (e.g.
  ``SKEIN_LOG_LEVEL=DEBUG``).

- Using the ``--log-level`` flag when starting a persistent daemon using the
  `CLI <cli.html>`__ (e.g.  ``skein daemon start --log-level debug``).

Additionally, you may want to log to a file instead of to the terminal. There
are also a few ways to do this:

- Using the ``log`` keyword when creating a :class:`skein.Client`.

- Using the ``--log`` flag when starting a persistent daemon using the `CLI
  <cli.html>`__.

**Example**

.. code-block:: python

    # Create a client, logging to `daemon.log` with "debug" log level
    import skein
    client = skein.Client(log_level='debug', log='daemon.log')


Start a Remote IPython Kernel on the Container
----------------------------------------------

As a last resort, it can be useful to Skein's remote `IPython
<https://ipython.org/>`_ kernel recipe to start an IPython kernel on the
failing container, and connect to the kernel to debug locally. Refer to the
:doc:`recipe documentation <recipes-ipython-kernel>` for more information.
