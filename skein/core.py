from __future__ import print_function, division, absolute_import

import glob
import os
import select
import signal
import socket
import struct
import subprocess
import warnings
from contextlib import closing, contextmanager

import grpc

from . import proto
from .compatibility import PY2, ConnectionError
from .model import Job, Service, ApplicationReport
from .utils import (cached_property, read_daemon, write_daemon,
                    ADDRESS_ENV_VAR, DAEMON_PATH, format_list)


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


class Client(object):
    def __init__(self, new_daemon=None, persist=False):
        if new_daemon is None:
            try:
                # Try to connect
                address, proc = self._connect()
            except ConnectionError:
                kind = 'persistent' if persist else 'temporary'
                warnings.warn("Failed to connect to global daemon, starting new "
                              "%s daemon." % kind)
                address, proc = self._create(persist=persist)
        elif new_daemon:
            address, proc = self._create(persist=persist)
        else:
            address, proc = self._connect()

        self.address = address
        self._proc = proc
        self._stub = proto.DaemonStub(grpc.insecure_channel(address))

    @staticmethod
    def _connect():
        address, _ = read_daemon()
        if address is not None:
            stub = proto.DaemonStub(grpc.insecure_channel(address))

            # Ping server to check connection
            with convert_errors(daemon=True):
                stub.ping(proto.Empty())

            return address, None

        raise ConnectionError("No daemon currently running")

    @staticmethod
    def _clear_global_daemon():
        address, pid = read_daemon()
        if address is None:
            return

        stub = proto.DaemonStub(grpc.insecure_channel(address))

        try:
            stub.ping(proto.Empty())
        except:
            pass
        else:
            os.kill(pid, signal.SIGTERM)

        try:
            os.remove(DAEMON_PATH)
        except OSError:
            pass

    @staticmethod
    def _create(verbose=True, persist=False):
        jar = _find_skein_jar()

        if persist:
            command = ["yarn", "jar", jar, jar, '--daemon']
        else:
            command = ["yarn", "jar", jar, jar]

        callback = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        callback.bind(('127.0.0.1', 0))
        callback.listen(1)

        with closing(callback):
            _, callback_port = callback.getsockname()

            env = dict(os.environ)
            env.update({'SKEIN_CALLBACK_PORT': str(callback_port)})

            if PY2:
                popen_kwargs = dict(preexec_fn=os.setsid)
            else:
                popen_kwargs = dict(start_new_session=True)

            outfil = None if verbose else subprocess.DEVNULL
            infil = None if persist else subprocess.PIPE

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

        address = '127.0.0.1:%d' % port

        if persist:
            Client._clear_global_daemon()
            write_daemon(address, proc.pid)
            proc = None

        return address, proc

    def __repr__(self):
        return 'Client<%s>' % self.address

    def close(self):
        if self._proc is not None:
            self._proc.stdin.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def application(self, app_id):
        return Application(self, app_id)

    def submit(self, job_or_path):
        if isinstance(job_or_path, str):
            job = Job.from_file(job_or_path)
        else:
            job = job_or_path
        with convert_errors(daemon=True):
            resp = self._stub.submit(job.to_protobuf())
        return Application(self, resp.id)

    def status(self, app_id=None, state=None):
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
        with convert_errors(daemon=True):
            self._stub.kill(proto.Application(id=app_id))


class AMClient(object):
    def __init__(self, address):
        self._address = address
        self._stub = proto.MasterStub(grpc.insecure_channel(address))

    def get_key(self, key=None, wait=False):
        if key is None:
            with convert_errors(daemon=False):
                resp = self._stub.keystore(proto.Empty())
            return dict(resp.items)

        with convert_errors(daemon=False):
            req = proto.GetKeyRequest(key=key, wait=wait)
            resp = self._stub.keystoreGet(req)
        return resp.val

    def set_key(self, key, value):
        if not len(key):
            raise ValueError("len(key) must be > 0")

        with convert_errors(daemon=False):
            self._stub.keystoreSet(proto.SetKeyRequest(key=key, val=value))

    def inspect(self, service=None):
        if service is None:
            with convert_errors(daemon=False):
                resp = self._stub.getJob(proto.Empty())
            return Job.from_protobuf(resp)
        else:
            with convert_errors(daemon=False):
                resp = self._stub.getService(proto.ServiceRequest(name=service))
            return Service.from_protobuf(resp)

    @classmethod
    def from_env(cls):
        address = os.environ.get(ADDRESS_ENV_VAR)
        if address is None:
            raise ValueError("Address not found at %r" % ADDRESS_ENV_VAR)

        return cls(address)

    @classmethod
    def from_id(cls, app_id, client=None):
        client = client or Client()
        s = client.status(app_id)
        if s.state != 'RUNNING':
            raise ValueError("This operation requires state: RUNNING. "
                             "Current state: %s." % s.state)
        return cls('%s:%d' % (s.host, s.port))


class Application(object):
    def __init__(self, client, app_id):
        self.client = client
        self.app_id = app_id

    def __repr__(self):
        return 'Application<id=%r>' % self.app_id

    def status(self):
        return self.client.status(self.app_id)

    def inspect(self, service=None):
        return self._am_client.inspect(service=service)

    def get_key(self, key=None, wait=False):
        return self._am_client.get_key(key=key, wait=wait)

    def set_key(self, key, value):
        return self._am_client.set_key(key, value)

    @cached_property
    def _am_client(self):
        return AMClient.from_id(self.app_id, client=self.client)
