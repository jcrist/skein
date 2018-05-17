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
from collections import namedtuple
from contextlib import closing, contextmanager

import grpc

from . import proto
from .compatibility import PY2, ConnectionError, FileExistsError
from .model import Job, Service, ApplicationReport
from .utils import cached_property, format_list, implements


__all__ = ('Client', 'Application', 'ApplicationClient', 'start_global_daemon',
           'stop_global_daemon', 'Security')


ADDRESS_ENV_VAR = 'SKEIN_APPMASTER_ADDRESS'
CONFIG_DIR = os.path.join(os.path.expanduser('~'), '.skein')
DAEMON_PATH = os.path.join(CONFIG_DIR, 'daemon')


def _find_skein_jar():
    this_dir = os.path.dirname(os.path.relpath(__file__))
    jars = glob.glob(os.path.join(this_dir, 'java', 'skein-*.jar'))
    if not jars:
        raise ValueError("Failed to find the skein jar file")
    assert len(jars) == 1
    return jars[0]


class DaemonError(Exception):
    pass


class ApplicationMasterError(Exception):
    pass


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
        return cls.from_directory(CONFIG_DIR)

    @classmethod
    def from_directory(cls, directory):
        """Create a security object from a directory, relying on standard names
        for each file."""
        cert_path = os.path.join(directory, 'skein.crt')
        key_path = os.path.join(directory, 'skein.pem')
        return Security(cert_path, key_path)

    @classmethod
    def write_security_configuration(cls, directory=None, force=False):
        """Write security configuration.

        This is equivalent to the cli command ``skein config``. Should only
        need to be called *once* per user upon install. Call again with
        ``force=True`` to generate new TLS keys and certificates.

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

        if not force:
            if os.path.exists(cert_path):
                raise FileExistsError("skein.crt file already exists, use "
                                      "``force`` to override")
            elif os.path.exists(key_path):
                raise FileExistsError("skein.pem file already exists, use "
                                      "``force`` to override")

        for path, data in [(cert_path, cert_bytes), (key_path, key_bytes)]:
            with open(path, mode='wb') as fil:
                fil.write(data)
                os.chmod(path, 0o600)  # User only read/write permissions

        return cls(cert_path, key_path)


@contextmanager
def convert_errors(daemon=True):
    exc = None
    try:
        yield
    except grpc.RpcError as _exc:
        exc = _exc

    if exc is not None:
        code = exc.code()
        if code == grpc.StatusCode.UNAVAILABLE:
            server_name = 'daemon' if daemon else 'application master'
            raise ConnectionError("Unable to connect to %s" % server_name)
        elif code in (grpc.StatusCode.INVALID_ARGUMENT,
                      grpc.StatusCode.NOT_FOUND,
                      grpc.StatusCode.ALREADY_EXISTS):
            raise ValueError(exc.details())
        else:
            cls = DaemonError if daemon else ApplicationMasterError
            raise cls(exc.details())


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
                        raise ValueError("Failed to read in client port")
                    port = struct.unpack("!i", msg)[0]
                    break
        else:
            raise ValueError("Failed to start java process")

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
    except ConnectionError:
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
    except ConnectionError:
        pass
    else:
        os.kill(pid, signal.SIGTERM)

    try:
        os.remove(DAEMON_PATH)
    except OSError:
        pass


class Client(object):
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
    def __init__(self, address=None, security=None):
        if address is None:
            address, _ = _read_daemon()

            if address is None:
                raise ConnectionError("No daemon currently running")

        stub = proto.DaemonStub(secure_channel(address, security))

        # Ping server to check connection
        with convert_errors(daemon=True):
            stub.ping(proto.Empty())

        self.address = address
        self._stub = stub
        self._proc = None

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

    def application(self, app_id):
        """Access an Application.

        Parameters
        ----------
        app_id : str
            The id of the application.

        Returns
        -------
        app : Application
        """
        return Application(self, app_id)

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
        with convert_errors(daemon=True):
            resp = self._stub.submit(job.to_protobuf())
        return Application(self, resp.id)

    def status(self, app_id=None, state=None):
        """Get the status of a skein application or applications.

        Parameters
        ----------
        app_id : str, optional
            A single application id to check the status of.
        state : str or sequence, optional
            If provided, applications will be filtered to these application
            states.

        Returns
        -------
        Either a single ``ApplicationReport`` (in the case of a provided
        ``app_id``), or a list of ``ApplicationReport``s.

        Examples
        --------
        To get the status of a single application

        >>> client.status(app_id='application_1526134340424_0012')
        ApplicationReport<name='demo'>

        To get all the finished and failed applications

        >>> client.status(state=['FINISHED', 'FAILED'])
        [ApplicationReport<name='demo'>,
         ApplicationReport<name='dask'>,
         ApplicationReport<name='demo'>]
        """
        if app_id is not None and state is not None:
            raise ValueError("Cannot provide both app_id and state")

        if app_id is not None:
            with convert_errors(daemon=True):
                resp = self._stub.getStatus(proto.Application(id=app_id))
            return ApplicationReport.from_protobuf(resp)

        if state is not None:
            valid = {'ACCEPTED', 'FAILED', 'FINISHED', 'KILLED', 'NEW',
                     'NEW_SAVING', 'RUNNING', 'SUBMITTED'}
            if isinstance(state, str):
                state = [state]

            states = {s.upper() for s in state}
            invalid = states.difference(valid)
            if invalid:
                raise ValueError("Invalid application states:\n"
                                 "%s" % format_list(invalid))
            states = list(states)
        else:
            states = []

        req = proto.ApplicationsRequest(states=states)
        with convert_errors(daemon=True):
            resp = self._stub.getApplications(req)
        return [ApplicationReport.from_protobuf(r) for r in resp.reports]

    def kill(self, app_id):
        """Kill an application.

        Parameters
        ----------
        app_id : str
            The id of the application to kill.
        """
        with convert_errors(daemon=True):
            self._stub.kill(proto.Application(id=app_id))


class ApplicationClient(object):
    """A client for the application master.

    Parameters
    ----------
    address : str
        The address of the application master.
    security : Security, optional
        The security configuration to use to communicate with the daemon.
        Defaults to the global configuration.
    """
    def __init__(self, address, security=None):
        self._address = address
        self._stub = proto.MasterStub(secure_channel(address, security))

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
            with convert_errors(daemon=False):
                resp = self._stub.keystore(proto.Empty())
            return dict(resp.items)

        with convert_errors(daemon=False):
            req = proto.GetKeyRequest(key=key, wait=wait)
            resp = self._stub.keystoreGet(req)
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
            raise ValueError("len(key) must be > 0")

        with convert_errors(daemon=False):
            self._stub.keystoreSet(proto.SetKeyRequest(key=key, val=value))

    def inspect(self, service=None):
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
            with convert_errors(daemon=False):
                resp = self._stub.getJob(proto.Empty())
            return Job.from_protobuf(resp)
        else:
            with convert_errors(daemon=False):
                resp = self._stub.getService(proto.ServiceRequest(name=service))
            return Service.from_protobuf(resp)

    @classmethod
    def current_application(cls):
        """Create an application client from within a running container.

        Useful for connecting to the application master from a running
        container in a job.
        """
        address = os.environ.get(ADDRESS_ENV_VAR)
        if address is None:
            raise ValueError("Address not found at %r" % ADDRESS_ENV_VAR)
        channel = secure_channel(address, Security(".skein.crt", ".skein.pem"))
        return cls(address, channel=channel)


class Application(object):
    """A Skein application."""
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

    @implements(ApplicationClient.inspect)
    def inspect(self, service=None):
        return self._am_client.inspect(service=service)

    @implements(ApplicationClient.get_key)
    def get_key(self, key=None, wait=False):
        return self._am_client.get_key(key=key, wait=wait)

    @implements(ApplicationClient.set_key)
    def set_key(self, key, value):
        return self._am_client.set_key(key, value)

    @cached_property
    def _am_client(self):
        s = self._client.status(self.app_id)
        if s.state != 'RUNNING':
            raise ValueError("This operation requires state: RUNNING. "
                             "Current state: %s." % s.state)
        return ApplicationClient('%s:%d' % (s.host, s.port))
