from __future__ import print_function, division, absolute_import

import json
import os
import select
import signal
import socket
import struct
import subprocess
from contextlib import closing

import grpc

from . import proto
from .compatibility import PY2, makedirs, isidentifier, Mapping
from .exceptions import (context, FileNotFoundError, ConnectionError,
                         TimeoutError, ApplicationNotRunningError,
                         ApplicationError, DaemonNotRunningError, DaemonError)
from .kv import KeyValueStore
from .ui import WebUI
from .model import (Security, ApplicationSpec, ApplicationReport,
                    ApplicationState, ContainerState, Container,
                    FinalStatus, Resources, container_instance_from_string,
                    LogLevel)
from .utils import cached_property, grpc_fork_support_disabled


__all__ = ('Client', 'ApplicationClient', 'properties')


_SKEIN_DIR = os.path.abspath(os.path.dirname(os.path.relpath(__file__)))
_SKEIN_JAR = os.path.join(_SKEIN_DIR, 'java', 'skein.jar')


class Properties(Mapping):
    """Skein runtime properties.

    This class implements an immutable mapping type, exposing properties
    determined at import time.

    Attributes
    ----------
    application_id : str or None
        The current application id. None if not running in a container.
    appmaster_address : str or None
        The address of the current application's appmaster. None if not running
        in a container.
    config_dir : str
        The path to the configuration directory.
    container_id : str or None
        The current skein container id (of the form
        ``'{service}_{instance}'``). None if not running in a container.
    container_resources : Resources or None
        The resources allocated to the current container. None if not in a
        container.
    yarn_container_id : str or None
        The current YARN container id. None if not running in a container.
    """
    def __init__(self):
        config_dir = os.environ.get('SKEIN_CONFIG',
                                    os.path.join(os.path.expanduser('~'), '.skein'))
        application_id = os.environ.get('SKEIN_APPLICATION_ID')
        appmaster_address = os.environ.get('SKEIN_APPMASTER_ADDRESS')
        container_id = os.environ.get('SKEIN_CONTAINER_ID')
        yarn_container_id = os.environ.get('CONTAINER_ID')
        try:
            container_resources = Resources(
                int(os.environ.get('SKEIN_RESOURCE_MEMORY')),
                int(os.environ.get('SKEIN_RESOURCE_VCORES')))
        except (ValueError, TypeError):
            container_resources = None

        mapping = dict(application_id=application_id,
                       appmaster_address=appmaster_address,
                       config_dir=config_dir,
                       container_id=container_id,
                       container_resources=container_resources,
                       yarn_container_id=yarn_container_id)

        object.__setattr__(self, '_mapping', mapping)

    def __getitem__(self, key):
        return self._mapping[key]

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError("%r object has no attribute %r"
                                 % (type(self).__name__, key))

    def __setattr__(self, key, val):
        raise AttributeError("%r object has no attribute %r"
                             % (type(self).__name__, key))

    def __dir__(self):
        o = set(dir(type(self)))
        o.update(self.__dict__)
        o.update(c for c in self._mapping if isidentifier(c))
        return list(o)

    def __iter__(self):
        return iter(self._mapping)

    def __len__(self):
        return len(self._mapping)


properties = Properties()


def secure_channel(address, security):
    cert_bytes = security._get_bytes('cert')
    key_bytes = security._get_bytes('key')

    creds = grpc.ssl_channel_credentials(cert_bytes, key_bytes, cert_bytes)
    options = [('grpc.ssl_target_name_override', 'skein-internal'),
               ('grpc.default_authority', 'skein-internal')]
    return grpc.secure_channel(address, creds, options)


def _read_daemon():
    try:
        with open(os.path.join(properties.config_dir, 'daemon'), 'r') as fil:
            data = json.load(fil)
            address = data['address']
            pid = data['pid']
    except Exception:
        address = pid = None
    return address, pid


def _write_daemon(address, pid):
    # Ensure the config dir exists
    makedirs(properties.config_dir, exist_ok=True)
    # Write to the daemon file
    with open(os.path.join(properties.config_dir, 'daemon'), 'w') as fil:
        json.dump({'address': address, 'pid': pid}, fil)


def _start_daemon(security=None, set_global=False, log=None, log_level=None):
    if security is None:
        security = Security.from_default()

    if log_level is None:
        log_level = LogLevel(
            os.environ.get('SKEIN_LOG_LEVEL', LogLevel.INFO)
        )
    else:
        log_level = LogLevel(log_level)

    if not os.path.exists(_SKEIN_JAR):
        raise context.FileNotFoundError("Failed to find the skein jar file")

    # Compose the command to start the daemon server
    java_bin = ('%s/bin/java' % os.environ['JAVA_HOME']
                if 'JAVA_HOME' in os.environ
                else 'java')

    command = [java_bin,
               '-Dskein.log.level=%s' % log_level]

    # Configure location of native libs if directory exists
    if 'HADOOP_HOME' in os.environ:
        native_path = '%s/lib/native' % os.environ['HADOOP_HOME']
        if os.path.exists(native_path):
            command.append('-Djava.library.path=%s' % native_path)

    command.extend(['com.anaconda.skein.Daemon', _SKEIN_JAR])

    if set_global:
        command.append("--daemon")

    env = dict(os.environ)
    env['SKEIN_CERTIFICATE'] = security._get_bytes('cert')
    env['SKEIN_KEY'] = security._get_bytes('key')
    # Update the classpath in the environment
    classpath = (subprocess.check_output(['yarn', 'classpath', '--glob'])
                           .decode('utf-8'))
    env['CLASSPATH'] = '%s:%s' % (_SKEIN_JAR, classpath)

    callback = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    callback.bind(('localhost', 0))
    callback.listen(1)

    with closing(callback):
        _, callback_port = callback.getsockname()
        env['SKEIN_CALLBACK_PORT'] = str(callback_port)

        if PY2:
            popen_kwargs = dict(preexec_fn=os.setsid)
        else:
            popen_kwargs = dict(start_new_session=True)

        if log is None:
            outfil = None
        elif log is False:
            if PY2:
                outfil = open(os.devnull, 'w')
            else:
                outfil = subprocess.DEVNULL
        else:
            outfil = open(log, mode='w')
        infil = None if set_global else subprocess.PIPE

        proc = subprocess.Popen(command,
                                stdin=infil,
                                stdout=outfil,
                                stderr=outfil,
                                env=env,
                                **popen_kwargs)

        while proc.poll() is None:
            readable, _, _ = select.select([callback], [], [], 1)
            if callback in readable:
                connection = callback.accept()[0]
                with closing(connection):
                    stream = connection.makefile(mode="rb")
                    msg = stream.read(4)
                    if not msg:
                        raise DaemonError("Failed to read in client port")
                    port = struct.unpack("!i", msg)[0]
                    break
        else:
            raise DaemonError("Failed to start java process")

    address = 'localhost:%d' % port

    if set_global:
        Client.stop_global_daemon()
        _write_daemon(address, proc.pid)
        proc = None

    return address, proc


class _ClientBase(object):
    __slots__ = ('__weakref__',)

    def _call(self, method, req, timeout=None):
        try:
            return getattr(self._stub, method)(req, timeout=timeout)
        except grpc.RpcError as _exc:
            exc = _exc

        code = exc.code()
        if code == grpc.StatusCode.UNAVAILABLE:
            raise ConnectionError("Unable to connect to %s" % self._server_name)
        if code == grpc.StatusCode.DEADLINE_EXCEEDED:
            raise TimeoutError("Unable to connect to %s" % self._server_name)
        elif code == grpc.StatusCode.NOT_FOUND:
            raise context.KeyError(exc.details())
        elif code in (grpc.StatusCode.INVALID_ARGUMENT,
                      grpc.StatusCode.FAILED_PRECONDITION,
                      grpc.StatusCode.ALREADY_EXISTS):
            raise context.ValueError(exc.details())
        else:
            raise self._server_error(exc.details())


class Client(_ClientBase):
    """Connect to and schedule applications on the YARN cluster.

    Parameters
    ----------
    address : str, optional
        The address for the daemon. By default will create a new daemon
        process.  Pass in address explicitly to connect to a different daemon.
        To connect to the global daemon see ``Client.from_global_daemon``.
    security : Security, optional
        The security configuration to use to communicate with the daemon.
        Defaults to the global configuration.
    log : str, bool, or None, optional
        When starting a new daemon, sets the logging behavior for the daemon.
        Values may be a path for logs to be written to, ``None`` to log to
        stdout/stderr, or ``False`` to turn off logging completely. Default is
        ``None``.
    log_level : str or skein.model.LogLevel, optional
        The daemon log level. Sets the ``skein.log.level`` system property. One
        of {'ALL', 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL', 'OFF'}
        (from most to least verbose). Default is 'INFO'.

    Examples
    --------
    >>> with skein.Client() as client:
    ...     app_id = client.submit('spec.yaml')
    """
    __slots__ = ('address', 'security', '_stub', '_proc')
    _server_name = 'daemon'
    _server_error = DaemonError

    def __init__(self, address=None, security=None, log=None, log_level=None):
        if security is None:
            security = Security.from_default()

        if address is None:
            address, proc = _start_daemon(security=security, log=log,
                                          log_level=log_level)
        else:
            proc = None

        with grpc_fork_support_disabled():
            self._stub = proto.DaemonStub(secure_channel(address, security))
        self.address = address
        self.security = security
        self._proc = proc

        try:
            # Ping server to check connection
            self._call('ping', proto.Empty())
        except Exception:
            if proc is not None:
                proc.stdin.close()  # kill the daemon on error
                proc.wait()
            raise

    @classmethod
    def from_global_daemon(self):
        """Connect to the global daemon."""
        address, _ = _read_daemon()

        if address is None:
            raise DaemonNotRunningError("No daemon currently running")

        security = Security.from_default()
        return Client(address=address, security=security)

    @staticmethod
    def start_global_daemon(log=None, log_level=None):
        """Start the global daemon.

        No-op if the global daemon is already running.

        Parameters
        ----------
        log : str, bool, or None, optional
            Sets the logging behavior for the daemon. Values may be a path for
            logs to be written to, ``None`` to log to stdout/stderr, or
            ``False`` to turn off logging completely. Default is ``None``.
        log_level : str or skein.model.LogLevel, optional
            The daemon log level. Sets the ``skein.log.level`` system property.
            One of {'ALL', 'TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL',
            'OFF'} (from most to least verbose). Default is 'INFO'.

        Returns
        -------
        address : str
            The address of the daemon
        """
        try:
            client = Client.from_global_daemon()
        except DaemonNotRunningError:
            pass
        else:
            return client.address
        address, _ = _start_daemon(set_global=True, log=log,
                                   log_level=log_level)
        return address

    @staticmethod
    def stop_global_daemon():
        """Stops the global daemon if running.

        No-op if no global daemon is running."""
        address, pid = _read_daemon()
        if address is None:
            return

        try:
            Client(address=address)
        except DaemonNotRunningError:
            pass
        else:
            os.kill(pid, signal.SIGTERM)

        try:
            os.remove(os.path.join(properties.config_dir, 'daemon'))
        except OSError:
            pass

    def __repr__(self):
        return 'Client<%s>' % self.address

    def close(self):
        """Closes the java daemon if started by this client. No-op otherwise."""
        if self._proc is not None:
            self._proc.stdin.close()
            self._proc.wait()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        self.close()

    def submit(self, spec):
        """Submit a new skein application.

        Parameters
        ----------
        spec : ApplicationSpec, str, or dict
            A description of the application to run. Can be an
            ``ApplicationSpec`` object, a path to a yaml/json file, or a
            dictionary description of an application specification.

        Returns
        -------
        app_id : str
            The id of the submitted application.
        """
        if isinstance(spec, str):
            spec = ApplicationSpec.from_file(spec)
        elif isinstance(spec, dict):
            spec = ApplicationSpec.from_dict(spec)
        elif not isinstance(spec, ApplicationSpec):
            raise context.TypeError("spec must be either an ApplicationSpec, "
                                    "path, or dict, got "
                                    "%s" % type(spec).__name__)
        resp = self._call('submit', spec.to_protobuf())
        return resp.id

    def submit_and_connect(self, spec):
        """Submit a new skein application, and wait to connect to it.

        If an error occurs before the application connects, the application is
        killed.

        Parameters
        ----------
        spec : ApplicationSpec, str, or dict
            A description of the application to run. Can be an
            ``ApplicationSpec`` object, a path to a yaml/json file, or a
            dictionary description of an application specification.

        Returns
        -------
        app_client : ApplicationClient
        """
        app_id = self.submit(spec)
        try:
            return self.connect(app_id, security=spec.master.security)
        except BaseException:
            self.kill_application(app_id)
            raise

    def connect(self, app_id, wait=True, security=None):
        """Connect to a running application.

        Parameters
        ----------
        app_id : str
            The id of the application.
        wait : bool, optional
            If true [default], blocks until the application starts. If False,
            will raise a ``ApplicationNotRunningError`` immediately if the
            application isn't running.
        security : Security, optional
            The security configuration to use to communicate with the
            application master. Defaults to the global configuration.

        Returns
        -------
        app_client : ApplicationClient

        Raises
        ------
        ApplicationNotRunningError
            If the application isn't running.
        """
        if wait:
            resp = self._call('waitForStart', proto.Application(id=app_id))
        else:
            resp = self._call('getStatus', proto.Application(id=app_id))
        report = ApplicationReport.from_protobuf(resp)
        if report.state is not ApplicationState.RUNNING:
            raise ApplicationNotRunningError(
                "%s is not running. Application state: "
                "%s" % (app_id, report.state))

        if security is None:
            security = self.security

        return ApplicationClient('%s:%d' % (report.host, report.port),
                                 app_id,
                                 security=security)

    def get_applications(self, states=None):
        """Get the status of current skein applications.

        Parameters
        ----------
        states : sequence of ApplicationState, optional
            If provided, applications will be filtered to these application
            states. Default is ``['SUBMITTED', 'ACCEPTED', 'RUNNING']``.

        Returns
        -------
        reports : list of ApplicationReport

        Examples
        --------
        Get all the finished and failed applications

        >>> client.get_applications(states=['FINISHED', 'FAILED'])
        [ApplicationReport<name='demo'>,
         ApplicationReport<name='dask'>,
         ApplicationReport<name='demo'>]
        """
        if states is not None:
            states = tuple(ApplicationState(s) for s in states)
        else:
            states = (ApplicationState.SUBMITTED,
                      ApplicationState.ACCEPTED,
                      ApplicationState.RUNNING)

        req = proto.ApplicationsRequest(states=[str(s) for s in states])
        resp = self._call('getApplications', req)
        return sorted((ApplicationReport.from_protobuf(r) for r in resp.reports),
                      key=lambda x: x.id)

    def application_report(self, app_id):
        """Get a report on the status of a skein application.

        Parameters
        ----------
        app_id : str
            The id of the application.

        Returns
        -------
        report : ApplicationReport

        Examples
        --------
        >>> client.application_report('application_1526134340424_0012')
        ApplicationReport<name='demo'>
        """
        resp = self._call('getStatus', proto.Application(id=app_id))
        return ApplicationReport.from_protobuf(resp)

    def kill_application(self, app_id, user=""):
        """Kill an application.

        Parameters
        ----------
        app_id : str
            The id of the application to kill.
        user : str, optional
            The user to kill the application as. Requires the current user to
            have permissions to proxy as ``user``. Default is the current user.
        """
        self._call('kill', proto.KillRequest(id=app_id, user=user))


class ApplicationClient(_ClientBase):
    """A client for the application master.

    Used to interact with a running application.

    Parameters
    ----------
    address : str
        The address of the application master.
    app_id : str
        The application id
    security : Security, optional
        The security configuration to use to communicate with the
        application master.  Defaults to the global configuration.
    """
    _server_name = 'application'
    _server_error = ApplicationError

    def __init__(self, address, app_id, security=None):
        self.address = address
        self.security = security or Security.from_default()
        self.id = app_id
        with grpc_fork_support_disabled():
            self._stub = proto.AppMasterStub(secure_channel(address, self.security))

    def __repr__(self):
        return 'ApplicationClient<%s, %s>' % (self.id, self.address)

    def shutdown(self, status='SUCCEEDED', diagnostics=None):
        """Shutdown the application.

        Stop all running containers and shutdown the application.

        Parameters
        ----------
        status : FinalStatus, optional
            The final application status. Default is 'SUCCEEDED'.
        diagnostics : str, optional
            The application exit message, usually used for diagnosing failures.
            Can be seen in the YARN Web UI for completed applications under
            "diagnostics", as well as the ``diagnostic`` field of
            ``ApplicationReport`` objects. If not provided, a default will be
            used.
        """
        req = proto.ShutdownRequest(final_status=str(FinalStatus(status)),
                                    diagnostics=diagnostics)
        self._call('shutdown', req)

    @cached_property
    def kv(self):
        """The Skein Key-Value store.

        Used by applications to coordinate configuration and global state.

        This implements the standard MutableMapping interface, along with the
        ability to "wait" for keys to be set. Keys are strings, with values as
        bytes.

        Examples
        --------
        >>> app_client.kv['foo'] = b'bar'
        >>> app_client.kv['foo']
        b'bar'
        >>> del app_client.kv['foo']
        >>> 'foo' in app_client.kv
        False

        Wait until the key is set, either by another service or by a user
        client. This is useful for inter-service synchronization.

        >>> app_client.kv.wait('mykey')
        """
        return KeyValueStore(self)

    @cached_property
    def ui(self):
        """The Skein Web UI.

        Used by applications to register additional web pages, and to get the
        addresses of these pages.
        """
        return WebUI(self)

    def get_specification(self):
        """Get the specification for the running application.

        Returns
        -------
        spec : ApplicationSpec
        """
        resp = self._call('getApplicationSpec', proto.Empty())
        return ApplicationSpec.from_protobuf(resp)

    def scale(self, service, instances):
        """Scale a service to a requested number of instances.

        Adds or removes containers to match the requested number of instances.
        When choosing which containers to remove, containers are removed in
        order of state (`WAITING`, `REQUESTED`, `RUNNING`) followed by age
        (oldest to newest).

        Parameters
        ----------
        service : str, optional
            The service to scale.
        instances : int
            The number of instances to scale to.

        Returns
        -------
        containers : list of Container
            A list of containers that were started or stopped.
        """
        if instances < 0:
            raise context.ValueError("instances must be >= 0")
        req = proto.ScaleRequest(service_name=service, instances=instances)
        resp = self._call('scale', req)
        return [Container.from_protobuf(c) for c in resp.containers]

    def set_progress(self, progress):
        """Update the progress for this application.

        For applications processing a fixed set of work it may be useful for
        diagnostics to set the progress as the application processes.

        Progress indicates job progression, and must be a float between 0 and
        1. By default the progress is set at 0.1 for its duration, which is a
        good default value for applications that don't know their progress,
        (e.g. interactive applications).

        Parameters
        ----------
        progress : float
            The application progress, must be a value between 0 and 1.
        """
        if not (0 <= progress <= 1.0):
            raise ValueError("progress must be between 0 and 1, got %.3f"
                             % progress)
        self._call('SetProgress', proto.SetProgressRequest(progress=progress))

    @classmethod
    def from_current(cls):
        """Create an application client from within a running container.

        Useful for connecting to the application master from a running
        container in a application.
        """
        address = properties.appmaster_address
        app_id = properties.application_id
        yarn_container_id = properties.yarn_container_id
        local_dirs = os.environ.get('LOCAL_DIRS')

        if any(p is None for p in [address, app_id, yarn_container_id,
                                   local_dirs]):
            raise context.ValueError("Not running inside a container")

        for local_dir in local_dirs.split(','):
            container_dir = os.path.join(local_dir, yarn_container_id)
            crt_path = os.path.join(container_dir, '.skein.crt')
            pem_path = os.path.join(container_dir, '.skein.pem')
            if os.path.exists(crt_path) and os.path.exists(pem_path):
                security = Security(cert_file=crt_path, key_file=pem_path)
                return cls(address, app_id, security=security)

        raise FileNotFoundError(
            "Failed to resolve .skein.{crt,pem} in 'LOCAL_DIRS'")

    def get_containers(self, services=None, states=None):
        """Get information on containers in this application.

        Parameters
        ----------
        services : sequence of str, optional
            If provided, containers will be filtered to these services.
            Default is all services.
        states : sequence of ContainerState, optional
            If provided, containers will be filtered by these container states.
            Default is ``['WAITING', 'REQUESTED', 'RUNNING']``.

        Returns
        -------
        containers : list of Container
        """
        if services is not None:
            services = set(services)
        if states is not None:
            states = [str(ContainerState(s)) for s in states]

        req = proto.ContainersRequest(services=services, states=states)
        resp = self._call('getContainers', req)
        return sorted((Container.from_protobuf(c) for c in resp.containers),
                      key=lambda x: (x.service_name, x.instance))

    def kill_container(self, id):
        """Kill a container.

        Parameters
        ----------
        id : str
            The id of the container to kill.
        """
        self._call('killContainer', container_instance_from_string(id))
