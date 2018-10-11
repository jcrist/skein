Pricing Dashboard Example
=========================

This example shows registering a webpage through the Skein Web UI.

It uses `bokeh <http://bokeh.pydata.org/en/latest/>`__ to create a streaming
dashboard of simulated market data.  It's based on `this example
<https://github.com/bokeh/bokeh/blob/master/examples/app/ohlc/README.md>`__
from the bokeh documentation.

Packaging
---------

This example requires ``bokeh``, ``tornado``, and ``numpy``, in addition to
``skein``. To distribute the dependencies, we can make use of either
`conda-pack <https://conda.github.io/conda-pack/>`__ or `venv-pack
<https://jcrist.github.io/venv-pack/>`__. See `distributing python environments
<https://jcrist.github.io/skein/distributing-files.html#distributing-python-environments>`__
for more information.

Using Conda Environments
~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: console

    # Create the environment
    $ conda create -n dashboard -c conda-forge -y skein bokeh tornado numpy conda-pack

    # Activate the environment
    $ conda activate dashboard

    # Package the environment
    $ conda pack -o environment.tar.gz


Using Virtual Environments
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: console

    # Create the environment
    $ python -m venv dashboard

    # Activate the environment
    $ source dashboard/bin/activate

    # Install the required packages
    $ pip install skein bokeh tornado numpy venv-pack

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


The pricing dashboard will then be accessible through the Skein Web UI, under
the "Service Pages" dropdown.

.. image:: ../../docs/source/_images/pricing-dashboard.gif


The dashboard will run until shutdown. To shutdown the application run:

.. code:: console

    $ skein application shutdown $APPID
