from __future__ import print_function, division, absolute_import

import datetime
import glob
import json
import os
import select
import signal
import socket
import struct
import subprocess
import warnings
from collections import namedtuple
from contextlib import closing

import grpc

from . import proto
from .compatibility import PY2
from .exceptions import (context, FileNotFoundError, SkeinConfigurationError,
                         ApplicationNotRunningError, ApplicationError,
                         DaemonNotRunningError, DaemonError)
from .model import Job, Service, ApplicationReport, ApplicationState


__all__ = ('Client', 'Application', 'ApplicationClient', 'Security',
           'start_global_daemon', 'stop_global_daemon')


ADDRESS_ENV_VAR = 'SKEIN_APPMASTER_ADDRESS'
CONFIG_DIR = os.path.join(os.path.expanduser('~'), '.skein')
DAEMON_PATH = os.path.join(CONFIG_DIR, 'daemon')


def _find_skein_jar():
    this_dir = os.path.dirname(os.path.relpath(__file__))
    jars = glob.glob(os.path.join(this_dir, 'java', 'skein-*.jar'))
    if not jars:
        raise context.FileNotFoundError("Failed to find the skein jar file")
    assert len(jars) == 1
    return jars[0]


class Security(namedtuple('Security', ['cert_path', 'key_path'])):
    """Security configuration.

    Parameters
    ----------
    cert_path : str
        Path to the certificate file in pem format.
    key_path : str
        Path to the key file in pem format.
    """
    def __new__(cls, cert_path=None, key_path=None):
        paths = [os.path.abspath(p) for p in (cert_path, key_path)]
        for path in paths:
            if not os.path.exists(path):
                raise FileNotFoundError(path)
        return super(Security, cls).__new__(cls, *paths)

    @classmethod
    def default(cls):
        """The default security configuration."""
        try:
            return cls.from_directory(CONFIG_DIR)
        except FileNotFoundError:
            pass
        raise SkeinConfigurationError(
            "Skein global configuration directory is not initialized. "
            "Please run ``skein init``.")

    @classmethod
    def from_directory(cls, directory):
        """Create a security object from a directory.

        Relies on standard names for each file (``skein.crt`` and
        ``skein.pem``)."""
        cert_path = os.path.join(directory, 'skein.crt')
        key_path = os.path.join(directory, 'skein.pem')
        return Security(cert_path, key_path)

    @classmethod
    def from_new_key_pair(cls, directory=None, force=False):
        """Create a Security object from a new certificate/key pair.

        This is equivalent to the cli command ``skein init`` with the option to
        specify an alternate directory *if needed*. Should only need to be
        called once per user upon install. Call again with ``force=True`` to
        generate new TLS keys and certificates.

        Parameters
        ----------
        directory : str, optional
            The directory to write the configuration to. Defaults to the global
            skein configuration directory at ``~/.skein/``.
        force : bool, optional
            If True, will overwrite existing configuration. Otherwise will
            error if already configured. Default is False.
        """
        from cryptography import x509
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import hashes
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from cryptography.x509.oid import NameOID

        directory = directory or CONFIG_DIR

        # Create directory if it doesn't exist
        os.makedirs(directory, exist_ok=True)

        key = rsa.generate_private_key(public_exponent=65537,
                                       key_size=2048,
                                       backend=default_backend())
        key_bytes = key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption())

        subject = issuer = x509.Name(
            [x509.NameAttribute(NameOID.COMMON_NAME, 'skein-internal')])
        now = datetime.datetime.utcnow()
        cert = (x509.CertificateBuilder()
                    .subject_name(subject)
                    .issuer_name(issuer)
                    .public_key(key.public_key())
                    .serial_number(x509.random_serial_number())
                    .not_valid_before(now)
                    .not_valid_after(now + datetime.timedelta(days=365))
                    .sign(key, hashes.SHA256(), default_backend()))

        cert_bytes = cert.public_bytes(serialization.Encoding.PEM)

        cert_path = os.path.join(directory, 'skein.crt')
        key_path = os.path.join(directory, 'skein.pem')

        for path, name in [(cert_path, 'skein.crt'), (key_path, 'skein.pem')]:
            if os.path.exists(path):
                if force:
                    os.unlink(path)
                else:
                    msg = ("%r file already exists, use `%s` to overwrite" %
                           (name, '--force' if context.is_cli else 'force'))
                    raise context.FileExistsError(msg)

        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        for path, data in [(cert_path, cert_bytes), (key_path, key_bytes)]:
            with os.fdopen(os.open(path, flags, 0o600), 'wb') as fil:
                fil.write(data)

        return cls(cert_path, key_path)


def secure_channel(address, security=None):
    security = security or Security.default()

    with open(security.cert_path, 'rb') as fil:
        cert = fil.read()

    with open(security.key_path, 'rb') as fil:
        key = fil.read()

    creds = grpc.ssl_channel_credentials(cert, key, cert)
    options = [('grpc.ssl_target_name_override', 'skein-internal'),
               ('grpc.default_authority', 'skein-internal')]
    return grpc.secure_channel(address, creds, options)


def _read_daemon():
    try:
        with open(DAEMON_PATH, 'r') as fil:
            data = json.load(fil)
            address = data['address']
            pid = data['pid']
    except Exception:
        address = pid = None
    return address, pid


def _write_daemon(address, pid):
    # Ensure the config dir exists
    os.makedirs(CONFIG_DIR, exist_ok=True)
    # Write to the daemon file
    with open(DAEMON_PATH, 'w') as fil:
        json.dump({'address': address, 'pid': pid}, fil)


def _start_daemon(security=None, set_global=False, log=None):
    security = security or Security.default()

    jar = _find_skein_jar()

    command = ["yarn", "jar", jar, jar, security.cert_path, security.key_path]
    if set_global:
        command.append("--daemon")

    callback = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    callback.bind(('localhost', 0))
    callback.listen(1)

    with closing(callback):
        _, callback_port = callback.getsockname()

        env = dict(os.environ)
        env.update({'SKEIN_CALLBACK_PORT': str(callback_port)})

        if PY2:
            popen_kwargs = dict(preexec_fn=os.setsid)
        else:
            popen_kwargs = dict(start_new_session=True)

        if log is None:
            outfil = None
        elif log is False:
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
        stop_global_daemon()
        _write_daemon(address, proc.pid)
        proc = None

    return address, proc


def start_global_daemon(log=None):
    """Start the global daemon.

    No-op if the global daemon is already running.

    Parameters
    ----------
    log : str, bool, or None, optional
        Sets the logging behavior for the daemon. Values may be a path for logs
        to be written to, ``None`` to log to stdout/stderr, or ``False`` to
        turn off logging completely. Default is ``None``.

    Returns
    -------
    address : str
        The address of the daemon
    """
    try:
        client = Client()
    except DaemonNotRunningError:
        pass
    else:
        return client.address
    address, _ = _start_daemon(set_global=True, log=log)
    return address


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
        os.remove(DAEMON_PATH)
    except OSError:
        pass


class _ClientBase(object):
    def _call(self, method, req):
        try:
            return getattr(self._stub, method)(req)
        except grpc.RpcError as _exc:
            exc = _exc

        code = exc.code()
        if code == grpc.StatusCode.UNAVAILABLE:
            raise ConnectionError("Unable to connect to %s" % self._server_name)
        elif code in (grpc.StatusCode.INVALID_ARGUMENT,
                      grpc.StatusCode.NOT_FOUND,
                      grpc.StatusCode.ALREADY_EXISTS):
            raise context.ValueError(exc.details())
        else:
            raise self._server_error(exc.details())


class Client(_ClientBase):
    """Connect to and schedule jobs on the YARN cluster.

    Parameters
    ----------
    address : str, optional
        The address for the daemon. By default will try to connect to the
        global daemon. Pass in address explicitly to connect to a different
        daemon. To create a new daemon, see ``skein.Client.temporary`` or
        ``skein.start_global_daemon``.
    security : Security, optional
        The security configuration to use to communicate with the daemon.
        Defaults to the global configuration.
    """
    _server_name = 'daemon'
    _server_error = DaemonError

    def __init__(self, address=None, security=None):
        if address is None:
            address, _ = _read_daemon()

            if address is None:
                raise DaemonNotRunningError("No daemon currently running")

        if security is None:
            security = Security.default()

        self._stub = proto.DaemonStub(secure_channel(address, security))
        self.address = address
        self.security = security
        self._proc = None

        # Ping server to check connection
        self._call('ping', proto.Empty())

    @classmethod
    def temporary(cls, security=None, log=None):
        """Create a client connected to a temporary daemon process.

        It's recommended to use the resulting client as a contextmanager or
        explicitly call ``.close()``. Otherwise the daemon won't be closed
        until the creating process exits.

        Parameters
        ----------
        security : Security, optional
            The security configuration to use to communicate with the daemon.
            Defaults to the global configuration.
        log : str, bool, or None, optional
            Sets the logging behavior for the daemon. Values may be a path for
            logs to be written to, ``None`` to log to stdout/stderr, or
            ``False`` to turn off logging completely. Default is ``None``.

        Returns
        -------
        client : Client

        Examples
        --------
        >>> with Client.temporary() as client:
        ...     print(client.status(app_id='application_1526134340424_0012'))
        ApplicationReport<name='demo'>
        """
        address, proc = _start_daemon(security=security, log=log)
        try:
            client = cls(address=address, security=security)
        except Exception:
            proc.stdin.close()  # kill the daemon on error
            proc.wait()
            raise
        client._proc = proc
        return client

    @property
    def is_temporary(self):
        """Whether this client is connected to a temporary daemon"""
        return self._proc is not None

    def __repr__(self):
        kind = 'temporary' if self.is_temporary else 'global'
        return 'Client<%s, %s>' % (self.address, kind)

    def close(self):
        """If connected to a temporary daemon, closes the daemon.

        No-op if connected to the global daemon"""
        if self._proc is not None:
            self._proc.stdin.close()
            self._proc.wait()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def submit(self, job_or_path):
        """Submit a new skein job.

        Parameters
        ----------
        job_or_path : Job or str
            The job to run. Can either be a ``Job`` object, or a path to a
            yaml/json file.

        Returns
        -------
        app : Application
        """
        if isinstance(job_or_path, str):
            job = Job.from_file(job_or_path)
        else:
            job = job_or_path
        resp = self._call('submit', job.to_protobuf())
        return Application(self, resp.id)

    def connect(self, app_id, wait=True):
        """Connect to a running application.

        Parameters
        ----------
        app_id : str
            The id of the application.
        wait : bool, optional
            If true [default], blocks until the application starts. If False,
            will raise a ``ApplicationNotRunningError`` immediately if the
            application isn't running.

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

        return ApplicationClient('%s:%d' % (report.host, report.port),
                                 security=self.security)

    def applications(self, states=None):
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

        >>> client.status(states=['FINISHED', 'FAILED'])
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
        return [ApplicationReport.from_protobuf(r) for r in resp.reports]

    def status(self, app_id):
        """Get the status of a skein application.

        Parameters
        ----------
        app_id : str
            A single application id to check the status of.

        Returns
        -------
        report : ApplicationReport

        Examples
        --------
        Get the status of a single application

        >>> client.status(app_id='application_1526134340424_0012')
        ApplicationReport<name='demo'>
        """
        resp = self._call('getStatus', proto.Application(id=app_id))
        return ApplicationReport.from_protobuf(resp)

    def kill(self, app_id):
        """Kill an application.

        Parameters
        ----------
        app_id : str
            The id of the application to kill.
        """
        self._call('kill', proto.Application(id=app_id))


class ApplicationClient(_ClientBase):
    """A client for the application master.

    Used to interact with a running application.

    Parameters
    ----------
    address : str
        The address of the application master.
    security : Security, optional
        The security configuration to use to communicate with the daemon.
        Defaults to the global configuration.
    """
    _server_name = 'application'
    _server_error = ApplicationError

    def __init__(self, address, security=None):
        self.address = address
        self.security = security
        self._stub = proto.MasterStub(secure_channel(address, security))

    def __repr__(self):
        return 'ApplicationClient<%s>' % self.address

    def get_key(self, key=None, wait=False):
        """Get a key from the keystore.

        Parameters
        ----------
        key : str, optional
            The key to get. If not provided, returns the whole keystore.
        wait : bool, optional
            If true, will block until the key is set. Default is False.
        """
        if key is None:
            resp = self._call('keystore', proto.Empty())
            return dict(resp.items)

        req = proto.GetKeyRequest(key=key, wait=wait)
        resp = self._call('keystoreGet', req)
        return resp.val

    def set_key(self, key, value):
        """Set a key in the keystore.

        Parameters
        ----------
        key : str
            The key to set.
        value : str
            The value to set.
        """
        if not len(key):
            raise context.ValueError("key length must be > 0")

        self._call('keystoreSet', proto.SetKeyRequest(key=key, val=value))

    def describe(self, service=None):
        """Information about the running job.

        Parameters
        ----------
        service : str, optional
            If provided, returns information on that service.

        Returns
        -------
        spec : Job or Service
            Returns a service if ``service`` is specified, otherwise returns
            the whole ``Job``.
        """
        if service is None:
            resp = self._call('getJob', proto.Empty())
            return Job.from_protobuf(resp)
        else:
            resp = self._call('getService', proto.ServiceRequest(name=service))
            return Service.from_protobuf(resp)

    @classmethod
    def connect_to_current(cls):
        """Create an application client from within a running container.

        Useful for connecting to the application master from a running
        container in a job.
        """
        address = os.environ.get(ADDRESS_ENV_VAR)
        if address is None:
            raise context.ValueError("Address not found at "
                                     "%r" % ADDRESS_ENV_VAR)
        channel = secure_channel(address, Security(".skein.crt", ".skein.pem"))
        return cls(address, channel=channel)


class Application(object):
    """A possibly running Skein application.

    May be used as a contextmanager to ensure that the enclosed application is
    stopped before exiting.

    The constructor shouldn't be used directly, instead use
    ``Client.submit``."""
    def __init__(self, client, app_id):
        self._client = client
        self.app_id = app_id

    def __repr__(self):
        return 'Application<id=%r>' % self.app_id

    def status(self):
        """The application status.

        Returns
        -------
        status : ApplicationReport
        """
        return self._client.status(self.app_id)

    def is_running(self):
        """Return True if the application state is RUNNING"""
        return self.status().state == ApplicationState.RUNNING

    def kill(self):
        """Kill the application."""
        return self._client.kill(self.app_id)

    def connect(self, wait=True):
        """Connect to the application.

        Parameters
        ----------
        wait : bool, optional
            If true [default], blocks until the application starts. If False,
            will raise a ``ApplicationNotRunningError`` immediately if the
            application isn't running.

        Returns
        -------
        app_client : ApplicationClient

        Raises
        ------
        ApplicationNotRunningError
            If the application isn't running.
        """
        return self._client.connect(self.app_id, wait=wait)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        try:
            if self.status().state not in {ApplicationState.FINISHED,
                                           ApplicationState.FAILED,
                                           ApplicationState.KILLED}:
                self.kill()
        except Exception as exc:
            warnings.warn("Failed to ensure application %s was stopped. "
                          "Exception: %s" % (self.app_id, exc))
