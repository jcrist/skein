from __future__ import print_function, division, absolute_import

import os
import time
import weakref
from collections import MutableMapping
from threading import Thread

import pytest

import skein
from skein.exceptions import FileNotFoundError, FileExistsError
from skein.test.conftest import run_sleeper_app, wait_for_containers


def test_security(tmpdir):
    path = str(tmpdir)
    s1 = skein.Security.from_new_directory(path)
    s2 = skein.Security.from_directory(path)
    assert s1 == s2

    with pytest.raises(FileExistsError):
        skein.Security.from_new_directory(path)

    # Test force=True
    with open(s1.cert_path) as fil:
        data = fil.read()

    s1 = skein.Security.from_new_directory(path, force=True)

    with open(s1.cert_path) as fil:
        data2 = fil.read()

    assert data != data2

    os.remove(s1.cert_path)
    with pytest.raises(FileNotFoundError):
        skein.Security.from_directory(path)


def pid_exists(pid):
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def test_client(security, kinit, tmpdir):
    logpath = str(tmpdir.join("log.txt"))

    with skein.Client(security=security, log=logpath) as client:
        # smoketests
        client.applications()
        repr(client)

        client2 = skein.Client(address=client.address, security=security)
        assert client2._proc is None

        # smoketests
        client2.applications()
        repr(client2)

    # Process was definitely closed
    assert not pid_exists(client._proc.pid)

    # Log was written
    assert os.path.exists(logpath)
    with open(logpath) as fil:
        assert len(fil.read()) > 0

    # Connection error on closed client
    with pytest.raises(skein.ConnectionError):
        client2.applications()

    # Connection error on connecting to missing daemon
    with pytest.raises(skein.ConnectionError):
        skein.Client(address=client.address, security=security)


def test_client_closed_when_reference_dropped(security, kinit):
    client = skein.Client(security=security, log=False)
    ref = weakref.ref(client)

    pid = client._proc.pid

    del client
    assert ref() is None
    assert not pid_exists(pid)


def test_simple_app(client):
    with run_sleeper_app(client) as app:
        # Nest manager here to call cleanup manually in this test
        with app:
            # wait for app to start
            ac = app.connect()

            assert app.is_running()

            # calling again is fine
            isinstance(app.connect(), skein.ApplicationClient)
            isinstance(app.connect(wait=False), skein.ApplicationClient)

            # smoketest reprs
            repr(app)
            repr(ac)

            report = app.status()
            running_apps = client.applications()
            assert report.id in {a.id for a in running_apps}

            assert report.state == 'RUNNING'
            assert report.final_status == 'UNDEFINED'

    report = app.status()
    assert report.state == 'KILLED'
    assert report.final_status == 'KILLED'

    with pytest.raises(skein.ConnectionError):
        app.connect()

    running_apps = client.applications()
    assert report.id not in {a.id for a in running_apps}

    killed_apps = client.applications(states=['killed'])
    assert report.id in {a.id for a in killed_apps}


def test_shutdown_app(client):
    with run_sleeper_app(client) as app:
        ac = app.connect()

        ac.shutdown(status='SUCCEEDED')

    assert app.status().final_status == 'SUCCEEDED'


def test_describe(client):
    with run_sleeper_app(client) as app:
        ac = app.connect()

        s = ac.describe(service='sleeper')
        assert isinstance(s, skein.Service)
        a = ac.describe()
        assert isinstance(a, skein.ApplicationSpec)
        assert a.services['sleeper'] == s


def test_key_value(client):
    with run_sleeper_app(client) as app:
        ac = app.connect()

        assert isinstance(ac.kv, MutableMapping)
        assert ac.kv is ac.kv

        assert dict(ac.kv) == {}

        ac.kv['foo'] = 'bar'
        assert ac.kv['foo'] == 'bar'

        assert dict(ac.kv) == {'foo': 'bar'}
        assert ac.kv.to_dict() == {'foo': 'bar'}
        assert len(ac.kv) == 1

        del ac.kv['foo']
        assert ac.kv.to_dict() == {}
        assert len(ac.kv) == 0

        with pytest.raises(KeyError):
            ac.kv['fizz']

        with pytest.raises(TypeError):
            ac.kv[1] = 'foo'

        with pytest.raises(TypeError):
            ac.kv['foo'] = 1

        def set_foo():
            time.sleep(0.5)
            ac2 = app.connect()
            ac2.kv['foo'] = 'baz'

        setter = Thread(target=set_foo)
        setter.daemon = True
        setter.start()

        val = ac.kv.wait('foo')
        assert val == 'baz'

        # Get immediately for set keys
        val2 = ac.kv.wait('foo')
        assert val2 == 'baz'


def test_dynamic_containers(client):
    with run_sleeper_app(client) as app:
        ac = app.connect()

        initial = wait_for_containers(ac, 1, states=['RUNNING'])
        assert initial[0].state == 'RUNNING'
        assert initial[0].service_name == 'sleeper'

        # Scale sleepers up to 3 containers
        new = ac.scale('sleeper', 3)
        assert len(new) == 2
        for c in new:
            assert c.state == 'REQUESTED'
        wait_for_containers(ac, 3, services=['sleeper'], states=['RUNNING'])

        # Scale down to 1 container
        stopped = ac.scale('sleeper', 1)
        assert len(stopped) == 2
        # Stopped oldest 2 instances
        assert stopped[0].instance == 0
        assert stopped[1].instance == 1

        # Scale up to 2 containers
        new = ac.scale('sleeper', 2)
        # Calling twice is no-op
        new2 = ac.scale('sleeper', 2)
        assert len(new2) == 0
        assert new[0].instance == 3
        current = wait_for_containers(ac, 2, services=['sleeper'],
                                      states=['RUNNING'])
        assert current[0].instance == 2
        assert current[1].instance == 3

        # Manually kill instance 3
        ac.kill('sleeper_3')
        current = ac.containers()
        assert len(current) == 1
        assert current[0].instance == 2

        # Fine to kill already killed container
        ac.kill('sleeper_1')

        # All killed containers
        killed = ac.containers(states=['killed'])
        assert len(killed) == 3
        assert [c.instance for c in killed] == [0, 1, 3]

        # Can't scale non-existant service
        with pytest.raises(ValueError):
            ac.scale('foobar', 2)

        # Can't scale negative
        with pytest.raises(ValueError):
            ac.scale('sleeper', -5)

        # Can't kill non-existant container
        with pytest.raises(ValueError):
            ac.kill('foobar_1')

        with pytest.raises(ValueError):
            ac.kill('sleeper_500')

        # Invalid container id
        with pytest.raises(ValueError):
            ac.kill('fooooooo')

        # Can't get containers for non-existant service
        with pytest.raises(ValueError):
            ac.containers(services=['sleeper', 'missing'])
