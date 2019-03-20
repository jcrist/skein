Specification
=============

Skein uses a declarative api for creating applications. The application may be
specificed as a YAML or JSON document, or via the `Python API
<api.html#application-specification>`__.  Here we describe the pieces of an
application specification in detail.

.. contents:: :local:


.. currentmodule:: skein


Specification Components
------------------------

Top-Level Fields
^^^^^^^^^^^^^^^^

At the top-level, a specification starts with an :class:`ApplicationSpec`.
This takes the following fields:

``name``
~~~~~~~~

The name of the application. Optional, defaults to ``skein``.

``queue``
~~~~~~~~~

The queue to submit the application to. Optional, defaults to ``default``.

``user``
~~~~~~~~

The username to submit the application as. Requires that the current user
have permission to proxy as this username. Optional, defaults to the
current user's username. In most cases the default is what you want.

``node_label``
~~~~~~~~~~~~~~

The `node_label
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html>`__
to request for all containers in this application. Services can override this
value by setting ``node_label`` on the service directly. Default is no label.

``max_attempts``
~~~~~~~~~~~~~~~~

The maximum number of submission attempts before marking the application as
failed. Note that this only considers failures of the Application Master
during startup. Optional, default is 1 (recommended).

``tags``
~~~~~~~~

A list of strings to use as tags for this application. Optional.

**Example**

.. code-block:: none

  tags:
    - my-tag
    - my-other-tag

``file_systems``
~~~~~~~~~~~~~~~~

A list of Hadoop file systems to acquire delegation tokens for. A token is
always acquired for the default filesystem (``fs.defaultFS`` in
``core-site.xml``). In many cases the default is sufficient. Optional.

**Example**

.. code-block:: none

  file_systems:
    - hdfs://nn1.com:8032
    - hdfs://nn2.com:8032
    - webhdfs://nn3.com:50070


.. _specification-acls:

``acls``
~~~~~~~~

Configures the application-level Access Control Lists (ACLs). Optional,
defaults to no ACLs.

The following access types are supported:

- ``VIEW`` : view application details
- ``MODIFY`` : modify the application via YARN (e.g. killing the application)
- ``UI`` : access the application Web UI

The ``VIEW`` and ``MODIFY`` access types are handled by YARN directly;
permissions for these can be set by users and/or groups. Authorizing ``UI``
access is handled by Skein internally, and only user-level access control is
supported.

The application owner (the user who submitted the application) will always
have permission for all access types.

By default, ACLs are disabled - to enable, set ``enable: True``. If enabled,
access is restricted only to the application owner by default - add
users/groups to the access types you wish to expand to other users. You can
use the wildcard character ``"*"`` to enable access for all users. Here we
give view access to all users:

Supported subfields are:

- ``enable``: whether to enable ACLs for this application. Default is ``False``.
- ``view_users``: users to give ``VIEW`` access. Default is ``[]``.
- ``view_groups``: groups to give ``VIEW`` access. Default is ``[]``.
- ``modify_users``: users to give ``MODIFY`` access. Default is ``[]``.
- ``modify_groups``: groups to give ``MODIFY`` access. Default is ``[]``.
- ``ui_users``: users to give ``UI`` access. Default is ``[]``.

**Example**

.. code-block:: none

  acls:
    enable: True    # Enable ACLs. Without this ACLs will be ignored.

    ui_users:
      - "*"           # Give all users access to the Web UI

    view_users:
      - nancy         # Give nancy view access

    # The application owner always has access to all access types. Since
    # `modify_users`/`modify_groups` are unchanged, only the owner has modify
    # access.

For more information on ACLs see:

- Cloudera's `documentation on YARN ACLs <https://www.cloudera.com/documentation/enterprise/6/6.0/topics/cm_mc_yarn_service1.html>`__
- The :class:`ACLs` docstring

``master``
~~~~~~~~~~

A :class:`Master` object, configuring the Application Master. Optional, see
specification-master_ for more information.

``services``
~~~~~~~~~~~~

A dict of service-name to :class:`Service`. Optional, see Service_ for more
information.

**Example**

.. code-block:: none

    services:
      my_service:
        ...

.. _specification-master:

Master
^^^^^^

All applications start with a single process called an *Application Master*.
This process is responsible for starting and managing any additional containers
during the lifetime of the application.

In Skein, the Application Master is the Java process that interprets the
specification, and manages any additional services that specification contains.
It also allows for *optionally* running a single user-defined process, referred
to in Skein as the *application driver*. If an application driver is specified,
the application will terminate when that process exits, regardless if other
services have yet to complete.

The :class:`Master` object allows for configuring the Application Master Java
process, as well as the optional user-defined application driver. Supported
subfields are:

.. _specification-resources:

``resources``
~~~~~~~~~~~~~

The memory and cpu requirements for the Application Master. Takes the following
fields:

- ``memory``

  The amount of memory to request. Can be either a string with units (e.g. ``"5
  GiB"``), or numeric. If numeric, specifies the amount of memory in *MiB*.
  Note that the units are in mebibytes (MiB) *not* megabytes (MB) - the former
  being binary based (1024 MiB in a GiB), the latter being decimal based (1000
  MB in a GB). See `here <https://en.wikipedia.org/wiki/Mebibyte>`__ for more
  information on this distinction.

  Requests smaller than the minimum allocation will receive the minimum
  allocation (1024 MiB by default). Requests larger than the maximum allocation
  will error on application submission.

- ``vcores``

  The number of virtual cores to request. Depending on your system
  configuration one virtual core may map to a single actual core, or a fraction
  of a core. Requests larger than the maximum allocation will error on
  application submission.

**Example**

.. code-block:: none

    master:
      resources:
        memory: 2 GiB
        vcores: 2

``script``
~~~~~~~~~~~~

A bash script to run the *application driver*. Optional.

**Example**

.. code-block:: none

    master:
      script: |
        echo "Run this first"
        echo "Run this next"


.. _specification-files:

``files``
~~~~~~~~~

Any files or archives needed to distribute to this container. A mapping of
destination relative paths to :class:`File` or :class:`str` objects describing
the sources for these paths. :class:`File` objects are described in more detail
below.  Each :class:`File` object takes the following fields:

- ``source``

  The path to the file/archive. If no scheme is specified, the path is assumed
  to be on the local filesystem (``file://`` scheme). Relative paths are
  supported, and are taken relative to the location of the specification file.

- ``type``

  The type of file to distribute -- either ``archive`` or ``file``.  Archive's
  are automatically extracted by yarn into a directory with the same name as
  their destination (only ``.zip``, ``.tar.gz``, and ``.tgz`` supported).
  Optional; by default the type is inferred from the file extension.

- ``visibility``

  The resource visibility. Describes how resources are shared between
  applications. Valid values are:

  - ``application`` -- Shared among containers of the same application on the node.
  - ``private`` -- Shared among all applications of the same user on the node.
  - ``public`` -- Shared by all users on the node.

  Optional, default is ``application``. In most cases the default is what you
  want.

- ``size``

  The resource size in bytes. Optional; if not provided will be determined by
  the file system. In most cases the default is what you want.

- ``timestamp``

  The time the resource was last modified. Optional; if not provided will be
  determined by the file system. In most cases the default is what you want.

As a shorthand, values may be the source path instead of a :class:`File`
object.

For more information see :doc:`distributing-files`.

**Example**

.. code-block:: none

    master:
      files:
        # /local/path/to/file.zip will be uploaded to hdfs, and extracted
        # into the directory path_on_container
        path_on_container:
          source: /local/path/to/file.zip
          type: archive

        # Can also specify only the source path - missing fields are inferred
        script_path.py: /path/to/script.py

        # Files on remote filesystems can be used by specifying the scheme.
        script2_path.py: hdfs:///remote/path/to/script2.py

``env``
~~~~~~~

A mapping of environment variables to set in this container. Optional.

**Example**

.. code-block:: none

    master:
      env:
        ENV1: VAL1
        ENV2: VAL2

``log_level``
~~~~~~~~~~~~~

The Application Master log level. Possible values are (from most to least
verbose): ``all``, ``trace``, ``debug``, ``info``, ``warn``, ``error``,
``fatal`` or ``off``. Note that this sets the ``skein.log.level`` system
property, which is used in the default ``log4j.properties`` file - if you
provide your own ``log4j.properties`` file this field may have no effect.
Optional, the default is ``info``.

**Example**

.. code-block:: none

  master:
    log_level: debug

``log_config``
~~~~~~~~~~~~~~

A path to a custom ``log4j.properties`` file. Could be local or on a remote
filesystem. If not provided, a default logging configuration is used. See the
`Log4j documentation <https://logging.apache.org/log4j/1.2/>`__ for more
information. Optional.

**Example**

.. code-block:: none

  master:
    log_config: path/to/my/log4j.properties

``security``
~~~~~~~~~~~~

Security configuration for the Application Master. By default the application
master will use the same security credentials as the driver that launched it.
To override, provide a mapping of specifying the locations of ``cert_file`` and
``key_file``. See the :class:`Security` docstring for more information.
Optional, the default is usually sufficient.

**Example**

.. code-block:: none

  master:
    security:
      cert_file: path/to/my/cert_file.crt
      key_file: path/to/my/key_file.pem


Service
^^^^^^^

Any additional containers needed by an application can be described a
:class:`Service`.  Services specify how to launch an executable, as well as how
that executable should be managed over the course of the application. A service
may also have multiple instances, each running in their own YARN container. A
service description takes the following fields:

``resources``
~~~~~~~~~~~~~

The memory and cpu requirements for a single instance of the service. Same as
the ``resources`` field in ``master`` :ref:`described above
<specification-resources>`.

**Example**

.. code-block:: none

    services:
      my_service:
        resources:
          memory: 2 GiB
          vcores: 2

``script``
~~~~~~~~~~~~

A bash script to run the service. Required.

**Example**

.. code-block:: none

    services:
      my_service:
        script: |
          echo "Run this first"
          echo "Run this next"

``files``
~~~~~~~~~

Any files or archives needed to run the service. Same as the ``files`` field in
``master`` :ref:`described above <specification-files>`.

**Example**

.. code-block:: none

    services:
      my_service:
        files:
          # /local/path/to/file.zip will be uploaded to hdfs, and extracted
          # into the directory path_on_container
          path_on_container:
            source: /local/path/to/file.zip
            type: archive

          # Can also specify only the source path - missing fields are inferred
          script_path.py: /path/to/script.py

          # Files on remote filesystems can be used by specifying the scheme.
          script2_path.py: hdfs:///remote/path/to/script2.py

``env``
~~~~~~~

A mapping of environment variables needed to run the service. Optional.

**Example**

.. code-block:: none

    services:
      my_service:
        env:
          ENV1: VAL1
          ENV2: VAL2

``instances``
~~~~~~~~~~~~~

The number of instances to create on startup. Must be >= 0. After startup
additional instances may be created by the :class:`ApplicationClient`.
Optional, default is 1.

**Example**

.. code-block:: none

    services:
      my_service:
        instances: 4  # Start 4 instances

``depends``
~~~~~~~~~~~

A list of service names that this service depends on. The service will only be
started after all its dependencies have been started. Optional.

**Example**

.. code-block:: none

    services:
      starts_first:
        ...
      starts_second:
        depends:
          - starts_first

``max_restarts``
~~~~~~~~~~~~~~~~

The maximum number of restarts allowed for this service. Must be >= -1. On
failure, a container will be restarted if the total number of restarts for its
service is < ``max_restarts``. Once this limit is exceeded, the service is
marked as failed and the application will be terminated. Set to -1 to always
restart, or 0 to never restart. Optional, default is 0.

**Example**

.. code-block:: none

    services:
      my_service1:
        max_restarts: -1  # always restart
        ...

      my_service2:
        max_restarts: 0   # never restart
        ...

      my_service3:
        max_restarts: 3   # restart a maximum of 3 times
        ...

``allow_failures``
~~~~~~~~~~~~~~~~~~

If False (default), the whole application will shutdown if the number of
failures for this service exceeds ``max_restarts``. Set to True to keep the
application running even if the number of failures exceeds this limit.
Optional, default is False.

**Example**

.. code-block:: none

    services:
      my_service1:
        max_restarts: 0         # Never restart
        allow_failures: True    # Don't terminate the application on failure
        ...

      my_service2:
        max_restarts: 3         # Restart a maximum of 3 times
        allow_failures: False   # If more than 3 failures, terminate the application
        ...

``node_label``
~~~~~~~~~~~~~~

The `node_label
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeLabel.html>`__
to request for all containers in this service. If not set, defaults to the
application-level ``node_label`` (if set).

**Example**

.. code-block:: none

    node_label: mylabel

    services:
      my_service1:
        node_label: GPU  # This service will be allocated on "GPU" nodes only.
        ...

      my_service2:
        # node_label is not set, the application label "mylabel" will be used.
        ...

``nodes``
~~~~~~~~~

A list of node host names to restrict containers for this service to. Optional,
defaults to no node restrictions.

``racks``
~~~~~~~~~

A list of rack names to restrict containers for this service to. The racks
corresponding to any nodes requested will be automatically added to this list.
Optional, defaults to no rack restrictions.

``relax_locality``
~~~~~~~~~~~~~~~~~~

Whether to interpret the ``nodes`` and ``racks`` specifications as locality
*suggestions* whether than *requirements*. If True, containers for this request
may be assigned on hosts and racks other than the ones explicitly requested. If
False, those restrictions are strictly enforced. Optional, default is False.

**Example**

.. code-block:: none

    services:
      my_service1:
        # This service *must* run on either worker1 or worker2
        relax_locality: false
        nodes:
          - worker1
          - worker2

      my_service2:
        # This service is *suggested* to run on either worker1 or worker2,
        # but may run on any node
        relax_locality: true
        nodes:
          - worker1
          - worker2

Example
-------

An example specification file. This starts a `jupyter <http://jupyter.org/>`__
notebook and a 4 node `dask.distributed <http://dask.pydata.org/>`__ cluster.
The example uses `conda-pack <https://github.com/jcrist/conda-pack/>`__ to
package and distribute `conda environments <http://conda.pydata.org/>`__, but
applications are free to package files any way they see fit.

.. code-block:: none

    name: dask-with-jupyter
    queue: default

    master:
      resources:
        memory: 2 GiB
        vcores: 1
      files:
        conda_env: env.tar.gz
        data.csv: hdfs:///path/to/some/data.csv
      script: |
        source conda_env/bin/activate
        start-jupyter-notebook-and-register-address  # pseudocode

    services:
      dask.scheduler:
        resources:
          memory: 2 GiB
          vcores: 1
        files:
          conda_env: env.tar.gz
        script: |
          source conda_env/bin/activate
          start-dask-scheduler-and-register-address  # pseudocode

      dask.worker:
        instances: 4
        resources:
          memory: 4 GiB
          vcores: 4
        max_restarts: 8  # Restart workers a maximum of 8 times
        files:
          conda_env: env.tar.gz
        depends:
          - dask.scheduler  # Ensure scheduler is started before workers
        script: |
          source conda_env/bin/activate
          get-dask-scheduler-address-and-start-worker  # pseudocode


Python API Example
------------------

The above YAML file format is also composable using the `Python API
<api.html#application-specification>`__. The python classes
(:class:`ApplicationSpec`, :class:`Service`, etc...) map 1:1 to the YAML format
described above. They can be read from a file, or created directly:

.. code-block:: python

    import skein

    # Create from a yaml file
    spec = skein.ApplicationSpec.from_file('spec.yaml')

    # Create directly
    jupyter = skein.Master(resources=skein.Resources(memory='2 GiB', vcores=1),
                           files={'conda_env': 'env.tar.gz',
                                  'data.csv': 'hdfs:///path/to/some/data.csv'},
                           script=('source conda_env/bin/activate\n'
                                   'start-jupyter-notebook-and-register-address'))

    scheduler = skein.Service(resources=skein.Resources(memory='2 GiB', vcores=1),
                              files={'conda_env': 'env.tar.gz'},
                              script=('source conda_env/bin/activate\n'
                                      'start-dask-scheduler-and-register-address'))

    worker = skein.Service(instances=4,
                           max_restarts=8,
                           resources=skein.Resources(memory='4 GiB', vcores=4),
                           files={'conda_env': 'env.tar.gz'},
                           depends=['dask.scheduler'],
                           script=('source conda_env/bin/activate\n'
                                   'get-dask-scheduler-address-and-start-worker'))

    spec = skein.ApplicationSpec(name="dask-with-jupyter",
                                 queue="default",
                                 master=jupyter,
                                 services={'dask.scheduler': scheduler,
                                           'dask.worker': worker})
