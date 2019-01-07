Changelog
=========

Upcoming Release
----------------

Version 0.5.0 (January 7, 2019)
-------------------------------

- Support login via keytab, allowing for long-running services (:pr:`115`,
  :issue:`103`)
- Rename ``daemon`` to ``driver`` everywhere, deprecating old methods/classes
  (:pr:`116`)
- Allow forwarding java options to the Skein ``driver`` (:pr:`117`)
- Fix bugs preventing the Skein driver from running inside a YARN container
  (:pr:`119`)
- Add support for running a single user process on the same node as the
  Application Master, allowing for faster application startup for
  single-container services (:pr:`120`, :issue:`118`)
- Ensure application directory is cleaned up, even during application master
  failure or if killed by other tools (:pr:`122`)
- Fix support for application retries (:pr:`122`)
- Deprecate ``commands`` field in favor of ``script`` (:pr:`125`, :issue:`121`)
- Add ``--force`` option to ``skein driver stop`` (:pr:`126`, :issue:`124`)
- Update Web UI to display new features (progress reports, application
  master/driver logs, etc...) (:pr:`127`, :pr:`95`, :issue:`123`)
- Obtain a resource manager delegation token for all applications. Allows for
  applications to submit additional applications as needed (:pr:`127`)

Version 0.4.1 (December 7, 2018)
--------------------------------

- Reduce size of Skein's JAR by selectively culling unnecessary resources (:pr:`109`)
- Use Protobuf Lite to further reduce JAR size (:pr:`111`)
- Normalize application specification consistently between ``Client.submit``
  and ``Client.submit_and_connect`` (:pr:`114`, :issue:`110`).

Version 0.4.0 (December 5, 2018)
--------------------------------

- Add support for proxying user credentials, allowing submitted applications to
  run as a different user than the submitter (:pr:`101`)
- Support running on MapR provided clusters (:pr:`105`)
- Allow TLS credentials to be configured per-application (:pr:`107`)
- Silence extraneous gRPC logged warning ("Fork support only compatible with
  epoll1 and poll polling strategies").This warning doesn't apply to our use
  case, and will not be raised in a future release of gRPC. (:pr:`107`)
- Upgrade gRPC version to 1.16 (:pr:`107`)
- Silence deprecation warnings in Python 3.7 (:pr:`108`)

Version 0.3.1 (October 29, 2018)
--------------------------------

- Use ``NM_HOST`` to determine hostname, fixes connection issues on systems
  with alternate network interfaces (e.g. infiniband) (:pr:`97`)
- Fix accidental reliance on ``JAVA_HOME`` being defined (:pr:`100`)

Version 0.3.0 (October 26, 2018)
--------------------------------

- Add support for YARN node label expressions (:pr:`44`)
- Allow memory requirements to be specified with human-readable units
  (:pr:`87`, :issue:`86`)
- Add support for YARN node and rack requirements/suggestions (:pr:`90`,
  :issue:`89`)
- Allow setting diagnostics message on user-requested shutdown (:pr:`92`)
- Add ability to set application progress (:pr:`93`, :issue:`88`)
- Error nicely if user forgets to kinit (:pr:`94`)
- Improve logging messages in client daemon (:pr:`95`)
- Support configurable logging for client daemon (:pr:`96`)

Version 0.2.0 (October 11, 2018)
--------------------------------

- Add support for specifying additional filesystems, needed for ViewFs
  (:pr:`58`)
- Add a Web UI for viewing application status and logs (:pr:`68`, :pr:`42`,
  :issue:`34`)
- Cleanup staging directory for killed applications (:pr:`71`, :issue:`69`)
- Support application-level Access Control Lists (ACLs) (:pr:`78`, :issue:`74`)
- Add support for user-defined pages in the Web UI (:pr:`72`)
- Remove unneeded resources to reduce JAR size (:pr:`83`)
- Support custom logging configurations (:pr:`84`, :issue:`79`)
- Improve logging messages and granularity (:pr:`84`, :issue:`79`)
- Add ``exit_message`` field for completed containers to aid in debugging
  (:pr:`84`, :pr:`66`)

Version 0.1.1 (August 6, 2018)
------------------------------

- Fix bug in key ownership model (:pr:`53`)
- Cleanup and document ``skein.recipes`` (:pr:`50`)

Version 0.1.0 (August 1, 2018)
------------------------------

- Initial Public Release
- Clean rewrite of Key-Value Store to support more powerful operations
  (:pr:`40`, :issue:`33`, :issue:`35`)
- Expose container node addresses in Python API (:pr:`39`, :issue:`38`)
- Expose application and container ids, as well as resource limits in running
  containers via environment variables (:pr:`37`, :issue:`32`).
- Cleanup internals and rework public API (:pr:`36`)
- Correctly handle multiple values in ``LOCAL_DIRS`` (:pr:`31`)

Version 0.0.4 (July 3, 2018)
----------------------------

- Initial Alpha Release
