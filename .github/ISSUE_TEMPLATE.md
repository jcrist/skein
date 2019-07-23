Thank you for reporting an issue.

If your issue relates to ``dask-yarn``, please open an issue in that
repository's [issue tracker](https://github.com/dask/dask-yarn/issues) instead.

When reporting an issue, please include the following:

- **A description of the bug**

    A clear description of what went wrong, and what you expected to happen.

- **Steps to reproduce** 

    Where possible, please include steps to reproduce the issue. For an example
    of what this means, please see [this blog
    post](https://blog.dask.org/2018/02/28/minimal-bug-reports).

- **Relevant logs/tracebacks**

    If an error occurred, please include the **full** traceback and code that
    produced it. In the case of an application failure, please also include the
    application logs. These can be retrieved via 

    ```
    $ yarn logs -applicationId <APPLICATION ID>
    ```

    You may want to glance through the logs to ensure things have been
    anonymized where appropriate.

- **Version information**

    Please include version information for the following:

    - Python version
    - Hadoop version, and distribution (e.g. CDH) if applicable
    - Skein version
