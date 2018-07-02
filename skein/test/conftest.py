from __future__ import print_function, division, absolute_import

import os
import time
import subprocess
from contextlib import contextmanager

import pytest

import skein


@contextmanager
def set_skein_config(tmpdir):
    tmpdir = str(tmpdir)
    old = skein.core.CONFIG_DIR
    try:
        skein.core.CONFIG_DIR = tmpdir
        yield tmpdir
    finally:
        skein.core.CONFIG_DIR = old


@pytest.fixture
def skein_config(tmpdir_factory):
    with set_skein_config(tmpdir_factory.mktemp('config')) as config:
        yield config


@pytest.fixture(scope="session")
def security(tmpdir_factory):
    return skein.Security.from_new_directory(str(tmpdir_factory.mktemp('security')))


@pytest.fixture(scope="session")
def has_kerberos_enabled():
    command = ["hdfs", "getconf", "-confKey", "hadoop.security.authentication"]
    return "kerberos" in subprocess.check_output(command).decode()


@pytest.fixture(scope="session")
def kinit():
    keytab = "/home/testuser/testuser.keytab"
    if os.path.exists(keytab):
        subprocess.check_call(["kinit", "-kt", keytab, "testuser"])
    return


@pytest.fixture(scope="session")
def client(security, kinit):
    with skein.Client(security=security) as client:
        yield client


sleeper = skein.Service(resources=skein.Resources(memory=128, vcores=1),
                        commands=['sleep infinity'])


sleep_until_killed = skein.ApplicationSpec(name="sleep_until_killed",
                                           queue="default",
                                           tags={'sleeps'},
                                           services={'sleeper': sleeper})


def check_is_shutdown(client, app_id, status=None):
    timeleft = 5
    while timeleft:
        if client.status(app_id).state != 'RUNNING':
            break
        time.sleep(0.1)
        timeleft -= 0.1
    else:
        assert False, "Application wasn't properly terminated"

    if status is not None:
        assert client.status(app_id).final_status == status


def wait_for_success(app, timeout=30):
    while timeout:
        state = app.status().state
        if state == 'FINISHED':
            return
        elif state in ['FAILED', 'KILLED']:
            assert False, "Application state = %r, expected 'FINISHED'" % state
        time.sleep(0.1)
        timeout -= 0.1
    else:
        assert False, "Application timed out"


@contextmanager
def run_application(client, spec=sleep_until_killed):
    app = client.submit(spec)

    try:
        yield app
    finally:
        app.kill()
    check_is_shutdown(client, app.app_id)


def wait_for_containers(ac, n, **kwargs):
    timeleft = 5
    while timeleft:
        containers = ac.containers(**kwargs)
        if len(containers) == n:
            break
        time.sleep(0.1)
        timeleft -= 0.1
    else:
        assert False, "timeout"

    return containers


def get_logs(app_id, tries=3):
    command = ["yarn", "logs", "-applicationId", app_id]
    for i in range(tries - 1):
        try:
            return subprocess.check_output(command).decode()
        except Exception:
            pass
    return subprocess.check_output(command).decode()
