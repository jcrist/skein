Remote IPython Kernel
=====================

The :mod:`skein.recipes.ipython_kernel` module provides a command-line recipe
for starting a remote IPython kernel on a YARN container. The intended use is
to execute the module in a service, using the command:

.. code-block:: console

    $ python -m skein.recipes.ipython_kernel

The executing Python environment must contain the following dependencies to
work properly:

- ``skein``
- ``ipykernel``

After launching the service, the kernel connection information will be stored
in the `key-value store <key-value-store.html>`__ under the key
``'ipython.kernel.info'``. This key name is configurable with the command-line
flag ``--kernel-info-key``.


Example
-------

Here we provide a complete walkthrough of launching and connecting to a remote
IPython kernel. This example assumes you're logged into and running on an edge
node.


Kinit (optional)
^^^^^^^^^^^^^^^^

See :ref:`quickstart-kinit`.


Start the Skein Daemon (optional)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

See :ref:`quickstart-skein-daemon`.


Package the Python Environment
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To distribute Python environments we'll make use of `conda-pack
<https://conda.github.io/conda-pack/>`_, a tool for packaging and distributing
conda environments. As mentioned above, we need to make sure we have the
following packages installed in the remote environment:

- ``skein``
- ``ipykernel``

we'll also install ``numpy`` to have an example library for doing some
computation, and ``jupyter_console`` to have a way to connect to the remote
kernel (note that this is only needed on the client machine, but we'll install
it on both for simplicity).

.. code-block:: console

    # Create a new demo environment (output trimmed for brevity)
    $ conda create -n ipython-demo
    ...

    # Activate the environment
    $ conda activate ipython-demo

    # Install the needed packages (output trimmed for brevity)
    $ conda install conda-pack skein ipykernel numpy jupyter_console -c conda-forge
    ...

    # Package the environment into environment.tar.gz
    $ conda pack -o environment.tar.gz
    Collecting packages...
    Packing environment at '/home/jcrist/miniconda/envs/ipython-demo' to 'environment.tar.gz'
    [########################################] | 100% Completed | 35.3s


Write the Application Specification
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Next we need to write the application specification. For more information see
the `specification docs <specification.html>`__.

.. code-block:: yaml

    # stored in ipython-demo.yaml

    name: ipython-demo

    services:
      ipython:
        resources:
          memory: 1024
          vcores: 1
        files:
          # Distribute the bundled environment as part of the application.
          # This will be automatically extracted by YARN to the directory
          # ``environment`` during resource localization.
          environment: environment.tar.gz
        commands:
          # Activate our environment
          - source environment/bin/activate
          # Start the remote ipython kernel
          - python -m skein.recipes.ipython_kernel


Start the Remote IPython Kernel
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now we have everything needed to start the remote IPython kernel. The following
bash command starts the application and stores the application id in the
environment variable ``APPID``.

.. code-block:: console

    $ APPID=`skein application submit ipython-demo.yaml`


Retrieve the Kernel Information
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To connect to a remote kernel, Jupyter requires information usually stored in a
``kernel.json`` file. As mentioned above, the recipe provided in
:mod:`skein.recipes.ipython_kernel` stores this information in the key
``'ipython.kernel.info'``. We can retrieve this information and store it in a
file using the following bash command:

.. code-block:: console

    $ skein kv get $APPID --key ipython.kernel.info --wait > kernel.json


Connect to the Remote IPython Kernel
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Using ``jupyter console`` and the ``kernel.json`` file, we can connect to the
remote kernel.

.. code-block:: console

    $ jupyter console --existing kernel.json
    Jupyter console 5.2.0

    Python 3.6.6 | packaged by conda-forge | (default, Jul 26 2018, 09:53:17)
    Type 'copyright', 'credits' or 'license' for more information
    IPython 6.5.0 -- An enhanced Interactive Python. Type '?' for help.


    In [1]: import numpy as np  # can import distributed libraries

    In [2]: np.sum([1, 2, 3])
    Out[2]: 6

    In [3]: # ls shows the files on the remote container, not the local files

    In [4]: ls
    container_tokens                        environment@
    default_container_executor_session.sh*  launch_container.sh*
    default_container_executor.sh*          tmp/

    In [5]: # exit shuts down the application

    In [6]: exit
    Shutting down kernel


Confirm that the Application Completed
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We can check that application shutdown properly using ``skein application status``

.. code-block:: console

    $ skein application status $APPID
    APPLICATION_ID                    NAME            STATE       STATUS       CONTAINERS    VCORES    MEMORY    RUNTIME
    application_1533143063639_0017    ipython-demo    FINISHED    SUCCEEDED    0             0         0         2m
