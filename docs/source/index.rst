Skein
=====

.. image:: /_images/yarn.jpg
    :width: 90 %
    :align: center
    :alt: skeins of yarn

..

    `skein <https://en.wiktionary.org/wiki/skein>`_ /skān/ *noun*

    1. *A quantity of yarn, thread, or the like, put up together, after it is taken from the reel.*


Skein is a simple library and command-line interface (CLI) for deploying
applications on `Apache YARN
<https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html>`_.
While YARN has many features that make it powerful for developing resilient
applications, these features also can make it difficult for developers to use.

Skein is easy to use, providing just as much YARN as your project needs.

Highlights
-----------

- No Java required! Compose and deploy applications using either the native
  `Python API <api.html>`__ or the `CLI <cli.html>`__.
- A declarative, language independent `specification <specification.html>`__
  allowing applications to be deployed in any language.
- Support for dynamic applications. Containers can be started and stopped at
  runtime, allowing for services to scale to your needs.
- An internal :doc:`key-value-store` for coordinating state between application
  containers.
- An extensible :doc:`web-ui` for tracking and interacting with applications.
- A `rigorous test suite <https://travis-ci.org/jcrist/skein>`__. Skein is
  tested on multiple YARN configurations to ensure wide support.

Installation
------------

Skein doesn't require elevated user privileges or cluster-wide configuration to
use. Simply install it on an edge node and get started.

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
    distributing-files.rst
    key-value-store.rst
    web-ui.rst
    recipes.rst
    debugging.rst
    api.rst
    cli.rst
    changelog.rst

Developer Documentation
-----------------------

.. toctree::
    :maxdepth: 2

    develop.rst
