Changelog
=========

Upcoming Release
----------------

Version 0.8.2 (Apr 4, 2022)
---------------------------

- Fix corrupt CLASSPATH env by removing trailing newline (:pr:`241`)
- Fix setup.py failure caused by PyPI yanked package: grpcio=1.45.0 (:pr:`242`)
- Add inline comments to pom.xml to help the java novice debug (:pr:`243`)

Version 0.8.1 (Mar 2, 2021)
---------------------------

- Fix resource check for GPU support (:issue:`220`, :pr:`224`)

Version 0.8.0 (August 28, 2019)
-------------------------------

- Dropped support for Python 2 (:issue:`180`, :pr:`186`)
- Add method to programmatically get logs from completed applications (:pr:`185`)
- Upgrade javascript dependencies (:issue:`184`, :pr:`187`)

Version 0.7.4 (June 28, 2019)
-----------------------------

- Close gRPC channels automatically on client shutdown (:pr:`179`)
- Silence client shutdown logging that was confusing to new users

Version 0.7.3 (May 6, 2019)
---------------------------

- Add ``ApplicationClient.add_container``, providing the ability to add a
  container to a service with optional service configuration overrides
  (:pr:`174`)

Version 0.7.2 (May 2, 2019)
---------------------------

- Fix bug in shutdown logic preventing shutdown on certain container failures
  (:issue:`172`, :pr:`173`)

Version 0.7.1 (April 25, 2019)
------------------------------

- Re-enable fork support when grpcio >= 1.18.0 (:issue:`167`, :pr:`168`)
- Make ``Client`` objects picklable (:pr:`169`)
- Support GPU and FPGA resources in Hadoop 3.1 (:issue:`154`, :pr:`171`)

Version 0.7.0 (April 22, 2019)
------------------------------

- Add ``Client.get_nodes`` for querying status of YARN cluster nodes
  (:issue:`155`, :pr:`156`)
- Expose information on YARN queues through ``Client.get_queue``,
  ``Client.get_child_queues``, and ``Client.get_all_queues`` (:pr:`159`)
- Improve error in build script if maven isn't installed (:issue:`158`,
  :pr:`160`)
- Add support for moving applications between queues (:pr:`161`)
- Support more filters in ``Client.get_applications`` (:issue:`133`, :pr:`162`)
- Log skein version in both ``Driver`` and ``ApplicationMaster`` Java processes
  (:pr:`163`)
- Enable testing on Hadoop 3/CDH 6 (:issue:`153`, :pr:`164`)
- Explicitly use ipv4 for Python <-> Java connection, removing potential for
  each binding to different interfaces (:issue:`165`, :pr:`166`)

Version 0.6.1 (April 3, 2019)
-----------------------------

- Add support to ``ApplicationClient.scale`` for scaling by a delta in
  instances rather than a total instance count (:pr:`150`)
- Fix bug in finding container directory (:pr:`151`)

Version 0.6.0 (March 21, 2019)
------------------------------

- More robustly handle starting/stopping the global driver in the case of
  previous driver failure (:pr:`141`, :issue:`140`)
- Add ``allow_failures`` field to services (:pr:`145`)
- Better error messages for improperly specified ``files`` (:pr:`146`,
  :issue:`139`)
- Expose the absolute path to the current container working directory as
  ``skein.properties.container_dir`` (:pr:`147`, :issue:`138`)
- Prevent a race condition when creating new global credentials (:pr:`148`,
  :issue:`144`)
- Remove functionality previously deprecated in version ``0.5.0`` (:pr:`149`)

Version 0.5.1 (January 21, 2019)
--------------------------------

- Fix bug preventing launching containers with vcores > 1 on some YARN
  configurations (:pr:`131`)
- Ensure application staging directory is cleaned up if submission fails
  (:pr:`132`)
- Refactored error handling in application master, improving error logs during
  application master failure (:pr:`134`)
- Decrease heartbeat interval during steady-state operation, reducing
  communication load on the resource manager (:pr:`137`, :issue:`135`)

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
