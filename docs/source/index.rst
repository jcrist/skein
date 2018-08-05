Skein
=====

.. image:: /_images/yarn.jpg
    :width: 90 %
    :align: center
    :alt: skeins of yarn

..

    `skein <https://en.wiktionary.org/wiki/skein>`_ /skān/ *noun*

    1. *A quantity of yarn, thread, or the like, put up together, after it is taken from the reel.*


Skein is a simple library and cli for deploying applications on `Apache YARN
<https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`_.
While YARN has many features that make it powerful for developing resilient
applications, these features also can make it difficult for developers to use.

Skein aims to be easy to use, providing just as much YARN as your project
needs.

Installation
------------

**Install with Conda:**

.. code::

    conda install -c conda-forge skein

**Install with Pip:**

.. code::

    pip install skein

**Install from source:**

Skein is `available on github <https://github.com/jcrist/skein>`_ and can
always be installed from source. Note that this requires `Apache Maven
<https://maven.apache.org/>`_ to build.


.. code::

    git clone https://github.com/jcrist/skein.git
    cd skein
    pip install .


User Documentation
------------------

.. toctree::
    :maxdepth: 2

    quickstart.rst
    specification.rst
    key-value-store.rst
    recipes.rst
    api.rst
    cli.rst
