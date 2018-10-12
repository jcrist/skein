Echo Server Example
===================

This example demonstrates deploying and interacting with a simple Python echo
server. The server is deployed on a random port, and the `Key-Value Store
<https://jcrist.github.io/skein/key-value-store.html>`__ is used to expose the
address to the client. The number of servers is then scaled dynamically.

This example code is based off `this example
<https://docs.python.org/3/library/asyncio-stream.html#asyncio-tcp-echo-client-streams>`__
from the asyncio documentation.

A blogpost demonstrating this example in more detail `can be read here
<http://jcrist.github.io/introducing-skein.html>`__.


Packaging
---------

This example requires Python 3+ and ``skein``. To distribute the dependencies,
we can make use of either `conda-pack <https://conda.github.io/conda-pack/>`__
or `venv-pack <https://jcrist.github.io/venv-pack/>`__. See `distributing
python environments
<https://jcrist.github.io/skein/distributing-files.html#distributing-python-environments>`__
for more information.


Using Conda Environments
~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: console

    # Create the environment
    $ conda create -n echo-server -c conda-forge -y skein conda-pack

    # Activate the environment
    $ conda activate echo-server

    # Package the environment
    $ conda pack -o environment.tar.gz


Using Virtual Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: console

    # Create the environment
    $ python -m venv echo-server

    # Activate the environment
    $ source echo-server/bin/activate

    # Install the required packages
    $ pip install skein venv-pack

    # Package the environment
    $ venv-pack -o environment.tar.gz


Running
-------

To run, make sure you've packaged an environment with the required
dependencies, and have ``skein`` installed in your current environment. Then
run:

.. code:: console

    # Start the application
    $ APPID=`skein application submit spec.yaml`

The client can then be run using

.. code:: console

    $ python client.py $APPID
    Connecting to server at 172.18.0.4:41846
    Sent: 'Hello World!'
    Received: 'Hello World!'

You can scale up the number of echo servers by running

.. code:: console

    # Scale to 4 server instances
    $ skein container scale $APPID --service server --number 4

    # List all ``server`` containers for this application
    $ skein container ls $APPID --service server
    SERVICE    ID          STATE      RUNTIME
    server     server_0    RUNNING    2m
    server     server_1    RUNNING    4s
    server     server_2    RUNNING    3s
    server     server_3    RUNNING    2s

Running the echo client again:

.. code:: console

    $ python client.py $APPID
    python client.py $APPID
    Connecting to server at 172.18.0.4:41846
    Sent: 'Hello World!'
    Received: 'Hello World!'
    Connecting to server at 172.18.0.4:42547
    Sent: 'Hello World!'
    Received: 'Hello World!'
    Connecting to server at 172.18.0.4:37295
    Sent: 'Hello World!'
    Received: 'Hello World!'
    Connecting to server at 172.18.0.4:45087
    Sent: 'Hello World!'
    Received: 'Hello World!'


The echo servers will run until they're manually shutdown. To shutdown the
application run:

.. code:: console

    $ skein application shutdown $APPID
