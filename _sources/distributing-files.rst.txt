Distributing Files
==================

Most applications require some number of files (executables, configuration,
scripts, etc...) to run. When deploying these applications on YARN you have
two options depending on the kind of access you want to assume:

1. **IT Permissions**: Install the files on every node in the cluster. This
   usually requires a cluster administrator, and is often only done for things
   like commonly used libraries. For example, you might install Python and
   common libraries on every node, but not your application code.

2. **User Permissions**: Distribute the files to the executing containers as
   part of the application.  This is the common approach, and does not require
   administrator privileges.

Both options have their place, and can be mixed in a single application. Below
we discuss the second option in detail.


Specifying Files for a Service
------------------------------

Files for distribution are specified per-service using the :ref:`files field of
the specification <specification-files>`. This field takes a mapping of
destination relative paths to :class:`File` or :class:`str` objects describing
the sources for these paths. Each :class:`File` object takes the following
fields:

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

**Example**

.. code-block:: yaml

    services:
      my_service:
        files:
          # /local/path/to/file.zip will be uploaded to HDFS, and extracted
          # into the directory path_on_container
          path_on_container:
            source: /local/path/to/file.zip
            type: archive

          # Can also specify only the source path - missing fields are inferred
          script_path.py: /path/to/script.py

          # Files on remote filesystems can be used by specifying the scheme.
          # If the specified filesystem matches the default filesystem
          # (typically HDFS), uploading will avoiding, allowing for faster
          # application startup.
          script2_path.py: hdfs:///remote/path/to/script2.py


The File Distribution Process
-----------------------------

When an application launches, the following process occurs:

1. An application staging directory is created in the user's home directory on
   the default filesystem (typically HDFS). The path for this is the format
   ``~/.skein/{application id}``.

2. The missing fields for each file in the application specification are
   inferred. All files that are not already on the default filesystem are
   copied over. Note that if a file is used by more than once service, it is
   only copied over once for the application.

3. The application is started.

4. When a container is started, YARN resource localization is applied for every
   file specified by that service. During this process files are copied to the
   container and any archives are extracted to the destination directories.
   Based on the file ``visibility`` setting, this may be done once per node per
   application, or once per node (with an LRU cache for clearing old files).
   For more information on this process see `this blogpost from Hortonworks
   <https://hortonworks.com/blog/resource-localization-in-yarn-deep-dive/>`__.

5. When the application completes, the staging directory is deleted.


Distribute Python Environments With Conda-Pack
----------------------------------------------

When deploying Python applications, one needs to figure out how to distribute
any library dependencies. If Python and all required libraries are already
installed on every node (option 1 above), then you can use the local Python and
avoid this problem completely. Otherwise, one way that users can distribute
libraries themselves is to use the `conda package manager
<https://conda.io/docs/>`__ to create a Python environment, and `conda-pack
<https://conda.github.io/conda-pack/>`__ to package that environment for
distribution with YARN (or other services).

``conda-pack`` is a tool for taking a conda environment and creating an archive
of it in a way that (most) absolute paths in any libraries or scripts are
altered to be relocatable. This archive then can be distributed with your
application, and will be automatically extracted during `YARN resource
localization
<https://hortonworks.com/blog/resource-localization-in-yarn-deep-dive/>`__.


Packaging the Environment
~~~~~~~~~~~~~~~~~~~~~~~~~

Here we create an example environment using ``conda``, and then use
``conda-pack`` to package the environment into a ``tar.gz`` file named
``environment.tar.gz``. This is what will be distributed with our application.

.. code-block:: console

    # Create a new conda environment (output trimmed for brevity)
    $ conda create -n example
    ...

    # Activate the environment
    $ conda activate example

    # Install the needed packages (output trimmed for brevity)
    $ conda install conda-pack skein numpy scikit-learn numba -c conda-forge
    ...

    # Package the environment into environment.tar.gz
    $ conda pack -o environment.tar.gz
    Collecting packages...
    Packing environment at '/home/jcrist/miniconda/envs/example' to 'environment.tar.gz'
    [########################################] | 100% Completed | 24.2s


Using the Packaged Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use the packaged environment in a service specification, you need to include
the archive in ``files``, and activate the environment in the ``commands``
list.

.. code-block:: yaml

    services:
      my_service:
        files:
          # The environment archive will be uploaded to HDFS, and extracted
          # into a directory named ``environment`` in each container.
          environment: environment.tar.gz

        commands:
          # Activate the environment
          - source environment/bin/activate

          # Run commands inside the environment. All executables or imported
          # python libraries will be from within the packaged environment.
          - my-cool-application
