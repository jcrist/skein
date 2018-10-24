Changelog
=========

Upcoming Release
----------------

- Add support for YARN node label expressions (:pr:`44`)
- Allow memory requirements to be specified with human-readable units
  (:pr:`87`, :issue:`86`)
- Add support for YARN node and rack requirements/suggestions (:pr:`90`,
  :issue:`89`)

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
