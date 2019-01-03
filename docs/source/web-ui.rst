Web UI
======

.. currentmodule:: skein

Skein comes with an informative Web UI. This displays information on:

- What services are currently running in the application
- Status of all current and past containers
- Live links to logs for each service
- Key-value pairs in the `Key-Value Store <key-value-store.html>`__

As well as links to user registered pages.

.. contents:: :local:

The Skein Web UI is usually accessed through the YARN Resource Manager Web UI.

.. image:: /_images/yarn-resourcemanager-webui-link.png
    :width: 90 %
    :class: light-bordered-image
    :align: center
    :alt: Link to the Skein Web UI from the YARN Resource Manager Web UI.

The address can also be obtained programatically through the
:class:`skein.ui.WebUI` object (found at :attr:`ApplicationClient.ui`).

.. code-block:: python

  >>> app.ui.address
  'http://master.example.com:8088/proxy/application_1539033277709_0001/'


The Overview Page
-----------------

The Overview page presents a summary view of the application and its contained
services. Information contained here includes:

- The application name, ID, and user
- Links to the Application Master and Application Driver logs (if relevant)
- Total memory and cores allocated for this application
- Total runtime
- Application progress (if set by the application)
- A summary of the application services

.. image:: /_images/webui-overview-closed.png
    :width: 90 %
    :class: light-bordered-image
    :align: center
    :alt: Overview page, accordion closed

Here we have two services - ``my-first-service`` and ``my-second-service``. The
icons in each row indicate for each service (in order):

- How many containers are currently running
- How many containers succeeded (exit code of 0)
- How many containers were killed (stopped by the user)
- How many containers have failed (non-zero exit code)

In this case, ``my-first-service`` has 2 containers running, 1 container
succeeded, 1 container killed, and 1 container failed.

Opening the accordion for the service displays three tables, summarizing all
pending, running, and completed containers.

.. image:: /_images/webui-overview-open.png
    :width: 90 %
    :class: light-bordered-image
    :align: center
    :alt: Overview page, accordion open

If applicable, links to the live logs for each container are provided, as well
as their current status and total runtime.


The Key/Value Page
------------------

The Key/Value page contains information on all key-value pairs currently set in
the `Key-Value Store <key-value-store.html>`__. This can be useful for
debugging applications. Note that if a value is not UTF-8 encodable it is
displayed as ``<binary value>``.

.. image:: /_images/webui-key-value.png
    :width: 90 %
    :class: light-bordered-image
    :align: center
    :alt: Key/Value page


Custom Pages
------------

Applications can register additional pages in the Web UI, which are available
through the "Service Pages" dropdown. This can be useful for providing
application specific dashboards.

Custom pages can be added with :func:`WebUI.add_page`, removed with
:func:`WebUI.remove_page`, and queried with :func:`WebUI.get_pages`.

.. currentmodule:: skein.ui
.. autosummary::

  WebUI.add_page
  WebUI.remove_page
  WebUI.get_pages

.. code-block:: python

  # Adds a page at <address_to_web_ui>/pages/custom
  # The link in the dropdown will be "My Custom Page"
  app.ui.add_page('custom', my_custom_page_address, link_name='My Custom Page')

  # Get a map of all registered pages
  app.ui.get_pages()

  # Remove the page with the route `custom`
  app.ui.remove_page('custom')

Note that due to limitations in the `YARN web application proxy
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/WebApplicationProxy.html>`__,
only ``GET`` requests can be successfully proxied through in most deployments
(support for ``PUT`` requests was added in Hadoop 3). This means that modern
web components like websockets won't work, nor will many RESTful web apps. Even
so, useful web pages can still be supported.


Bokeh Dashboard Example
~~~~~~~~~~~~~~~~~~~~~~~

An example dashboard with live updates using `bokeh
<https://bokeh.pydata.org/en/latest/>`__ is available `here
<https://github.com/jcrist/skein/tree/master/examples/pricing_dashboard>`__.

A few high level notes:

- Since the YARN web application proxy doesn't support websockets, we use an
  `AjaxDataSource
  <https://bokeh.pydata.org/en/latest/docs/reference/models/sources.html#bokeh.models.sources.AjaxDataSource>`__.
  This is less efficient than a ``ColumnDataSource`` (which uses websockets),
  as it requires polling the server to get updates. For small amounts of data
  though this is sufficient and fine.

- We choose a dynamic port, as we can't know for sure what ports will be
  available on the deployed container.

- The only Skein-specific bit is where `the page is registered with the
  application
  <https://github.com/jcrist/skein/blob/master/examples/pricing_dashboard/dashboard.py#L242-L246>`__:

  .. code-block:: python

    # Register the page with the Skein Web UI.
    # This is the only Skein-specific bit
    app = skein.ApplicationClient.from_current()
    app.ui.add_page('price-dashboard', address, link_name='Price Dashboard')


The deployed page looks like:

.. image:: /_images/pricing-dashboard.gif
    :width: 90 %
    :class: light-bordered-image
    :align: center
    :alt: An example bokeh dashboard deployed using Skein


Security
--------

YARN handles authentication but not authorization for application web pages. As
such, users accessing the pages must successfully authenticate with YARN
(either using kerberos or "simple" authentication, depending on cluster
configuration). However, by default any successfully authenticated user will
have access to the Skein Web UI.

If this is undesired, you'll need to configure security using :ref:`Access
Control Lists (ACLs) <specification-acls>`. The simplest version disables
access to the Web UI for everyone but the application owner:


.. code-block:: none

  acls:
    enable: true

  ...

All requests to the Web UI are authenticated by YARN and authorized by Skein
against the specified ACLs. This goes both for requests made from outside the
cluster (through the YARN web ui) and inside the cluster with direct access to
the Web UI (this is due to YARN's provided `security filter
<https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YarnApplicationSecurity.html#Securing_YARN_Application_Web_UIs_and_REST_APIs>`__).

.. warning::

   While the *Skein* Web UI is secured against both external and internal
   access by the YARN security filter, `Custom Pages`_ are currently only
   secured against access from outside the cluster (i.e. through the YARN Web
   UI). Users with SSH access to the cluster and knowledge of the internal
   address/ports used for custom web pages will currently have unrestricted
   access to these custom pages.

   This is fixable, but deemed lower priority, as it's only an issue in the
   case of malicious users that already have direct access to the cluster. If
   this is a problem for you, please let us know by `filing an issue
   <https://github.com/jcrist/skein/issues>`__.
