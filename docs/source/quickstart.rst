Quickstart
==========

First make sure that `Skein is installed <index.html#installation>`__.

Skein is intended to be used either as a commandline application or
programmatically using the Python API. This document provides a brief example
using the commandline -- for more information please see the `API <api.html>`__
or `CLI <cli.html>`__ documentation.


.. _quickstart-kinit:

Kinit (optional)
----------------

If your system is configured to use Kerberos for authentication, you need to
make sure you have an active ticket-granting-ticket before continuing:

.. code::

    $ kinit


.. _quickstart-skein-daemon:


Start the Skein Daemon (optional)
---------------------------------

To communicate with the YARN Resource manager, Skein uses a background daemon
process written in Java. Since this process can be slow to start, sometimes it
can be nice to start it once and have it persist through all CLI calls. This
may look like:

1. Start the Skein daemon
2. Run yarn application/applications
3. Shut down the Skein daemon

To do this from the command line, use `skein daemon start
<cli.html#skein-daemon-start>`__:

.. code::

    $ skein daemon start
    localhost:12345

Note that if you don't start the daemon process, one will be started for you,
but not persisted between calls.


Write an Application Specification
----------------------------------

Skein applications are written declaratively as specifications. These can be
provided as YAML or JSON files, or created programmatically using the
`specification api <api.html#application-specification>`__. For more
information, see the `specification docs <specification.html>`__.

Here we create a simple "Hello World" application as a YAML file:

.. code-block:: yaml

    name: hello_world
    queue: default

    services:
      my_service:
        resources:
          vcores: 1
          memory: 128
        files:
          payload.txt: payload.txt
        commands:
          - echo "Sleeping for 60 seconds"
          - sleep 60
          - cat payload.txt
          - echo "Stopping service"

Where the contents of ``payload.txt`` is:

.. code-block:: text

    Hello World!

Walking through this specification:

- The application name is specified as ``hello_world``, and will be deployed in
  the YARN queue ``default``.

  .. code-block:: yaml

     name: hello_world
     queue: default

- The application starts a single service ``my_service`` on a container using 1
  virtual core and 128 MB of memory.

  .. code-block:: yaml

     services:
       my_service:
         resources:
           vcores: 1
           memory: 128


- The file ``payload.txt`` is distributed with the application, and is named
  ``payload.txt`` both locally and on the running container.

  .. code-block:: yaml

     files:
       payload.txt: payload.txt

- The service runs a few Shell commands. These will be run in order, stopping
  on the first failure, and all outputs logged in the container logs.

  .. code-block:: yaml

     commands:
       - echo "Sleeping for 60 seconds"
       - sleep 60
       - cat payload.txt
       - echo "Stopping service"


Submit the Application
----------------------

Applications are submitted to be run on the cluster using the `skein
application submit <cli.html#skein-application-submit>`__ command:

.. code::

    $ skein application submit hello_world.yaml
    application_1526497750451_0009

This uploads any necessary files to HDFS, and submits the application to the
YARN scheduler. Depending on current cluster usage this could start immediately
or at a later time. The command outputs the Application ID, which is needed for
subsequent commands.


Query existing applications
---------------------------

As YARN processes applications, they work through several states, enumerated by
:class:`~skein.model.ApplicationState`. The status of all Skein applications
can be queried using the `skein application ls
<cli.html#skein-application-ls>`__ command. By default this shows all
applications that are either ``SUBMITTED``, ``ACCEPTED``, or ``RUNNING``.

.. code::

    $ skein application ls
    APPLICATION_ID                    NAME           STATE      STATUS       CONTAINERS    VCORES    MEMORY
    application_1526497750451_0009    hello_world    RUNNING    UNDEFINED    2             2         640

You can also filter by application state. Here we show all ``KILLED`` and ``FAILED`` applications:

.. code::

    $ skein application ls -s KILLED -s FAILED
    APPLICATION_ID                    NAME           STATE     STATUS    CONTAINERS    VCORES    MEMORY
    application_1526497750451_0002    hello_world    KILLED    KILLED    0             0         0
    application_1526497750451_0004    hello_world    KILLED    KILLED    0             0         0
    application_1526497750451_0005    hello_world    FAILED    FAILED    0             0         0

To get the status of a specific application, use the `skein application status
<cli.html#skein-application-status>`__ command:

.. code::

    $ skein application status application_1526497750451_0009
    APPLICATION_ID                    NAME           STATE      STATUS       CONTAINERS    VCORES    MEMORY
    application_1526497750451_0009    hello_world    RUNNING    UNDEFINED    2             2         640


Kill a running application
--------------------------

By default, applications shutdown once all of their services have exited *or*
any service exits with a non-zero exit code. To explicitly kill an application,
use the `skein application kill <cli.html#skein-application-kill>`__ command:

.. code::

    $ skein application kill application_1526497750451_0009

    # See that the application was killed
    $ skein application status application_1526497750451_0009
    APPLICATION_ID                    NAME           STATE     STATUS    CONTAINERS    VCORES    MEMORY
    application_1526497750451_0009    hello_world    KILLED    KILLED    0             0         0


Stop the Skein Daemon (optional)
--------------------------------

If you started the Daemon process (see `Start the Skein Daemon (optional)`_
above), you'll probably want to shut it down when you're done.  This isn't
strictly necessary (the daemon can run for long periods), but helps keep
resource usage on the edge node low.

To do this from the command line, use `skein daemon stop
<cli.html#skein-daemon-stop>`__.

.. code::

    $ skein daemon stop
