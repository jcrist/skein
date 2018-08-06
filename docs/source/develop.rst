Development
===========

This page provides information on how to build, test, and develop ``Skein``.


Building Skein
--------------

Clone Repository
~~~~~~~~~~~~~~~~

Clone the skein git repository:

.. code-block:: console

    $ git clone https://github.com/jcrist/skein.git


Install Dependencies (Conda)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We recommend using the Conda_ package manager to setup your development
environment. Here we setup a conda environment to contain the build
dependencies.

.. code-block:: console

    # Create a new conda environment
    $ conda create -n skein

    # Activate environment
    $ conda activate skein

    # Install dependencies
    $ conda install -c conda-forge grpcio protobuf cryptography pyyaml

    # Install grpcio-tools (not on conda-forge currently)
    $ pip install grpcio-tools


Besides the above dependencies, you'll also need Maven_. You can install Maven
using your system package manager, the maven website, or use Conda:

.. code-block:: console

    $ conda install -c conda-forge maven


Install Dependencies (Pip)
~~~~~~~~~~~~~~~~~~~~~~~~~~

You can also setup the development environment using ``pip``.

.. code-block:: console

    $ pip install grpcio protobuf cryptography pyyaml grpcio-tools


Besides the above dependencies, you'll also need Maven_. You can install Maven
using your system package manager or via the maven website.


Build and Install Skein
~~~~~~~~~~~~~~~~~~~~~~~

You can build and install Skein as an editable package or a regular install.

.. code-block:: console

    # Build and install skein as an editable package
    $ python setup.py develop

    # or, build and install as a regular package
    $ python setup.py install


Running the Tests
-----------------

The test suite is designed to run in a specific hadoop setup, provided by the
`hadoop-test-cluster`_ package. This is a CLI tool for setting up a Hadoop
cluster using `docker compose`_. This requires ``docker compose`` be installed,
and the docker daemon already be running. Please follow the install
instructions for your system `here
<https://docs.docker.com/compose/install/>`__.


Install `hadoop-test-cluster`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can install ``hadoop-test-cluster`` using ``pip``. This assumes you already
have ``docker`` and ``docker-compose`` already installed.

The `hadoop-test-cluster`_ repository readme has documentation on usage - below
we provide a few commands needed for using the cluster to run the tests.

.. code-block:: console

    $ pip install hadoop-test-cluster


Startup the Test Cluster
~~~~~~~~~~~~~~~~~~~~~~~~

This command starts up a tiny Hadoop cluster with ``simple`` security, and
mounts the current directory as ``~/skein`` on every node. To create a cluster
with ``kerberos`` security enabled, use ``--image kerberos`` instead.

.. code-block:: console

    $ htcluster startup --image base --mount .:skein


Login to the Edge Node
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ htcluster login


Setup the Development Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The docker image already has Conda_ installed. After startup, you only need to
install the runtime and test dependencies (see `Install Dependencies (Conda)`_).
Alternatively, Maven_ is also already installed on the docker image, so you can
skip the instructions for building Skein locally above and do everything on the
docker image.

You also need ``pytest`` to run the tests, and ``flake8`` to run the lint
checks.

.. code-block:: console

    $ conda install -c conda-forge pytest flake8


Build and Install Skein
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: console

    $ python setup.py develop


Run the Tests
~~~~~~~~~~~~~

.. code-block:: console

    $ pytest skein


Run the Linter
~~~~~~~~~~~~~~

.. code-block:: console

    $ flake8 skein


Shutdown the Cluster
~~~~~~~~~~~~~~~~~~~~

When you are done developing, you can shutdown the cluster using the following

.. code-block:: console

    $ htcluster shutdown


Building the Documentation
--------------------------

Skein uses Sphinx_ for documentation. The source files are located in
``skein/docs/source``. To build the documentation locally, first install the
documentation build requirements

.. code-block:: console

    $ pip install sphinx numpydoc sphinxcontrib.autoprogram

Then build the documentation with ``make``

.. code-block:: console

    # Running from the skein/docs folder
    $ make html

The resulting HTML files end up in the ``build/html`` directory.

Submitting a Documentation-Only Pull Request
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your pull-request only contains documentation changes, you can tell
Travis-CI to skip running the tests (and speed-up our CI process) by including
the string ``"skip-tests"`` somewhere in your commit message. For example:

.. code-block:: text

    Note how to skip tests on travis-ci [skip-tests]

    Add a note to the develop.rst docs on how to skip running the tests in
    travis.
    # Please enter the commit message for your changes. Lines starting
    # with '#' will be ignored, and an empty message aborts the commit.
    # On branch conditional-docs-build
    # Changes to be committed:
    #    modified:   docs/source/develop.rst


.. _Conda: https://conda.io/docs/
.. _Maven: http://maven.apache.org/
.. _Sphinx: http://www.sphinx-doc.org/
.. _docker compose: https://docs.docker.com/compose/
.. _hadoop-test-cluster: https://github.com/jcrist/hadoop-test-cluster
