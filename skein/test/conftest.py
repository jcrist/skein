from __future__ import print_function, division, absolute_import

import os
import time
import subprocess
from contextlib import contextmanager

import pytest

import skein


@pytest.fixture(scope="session")
def security(tmpdir_factory):
    return skein.Security.from_new_directory(str(tmpdir_factory.mktemp('security')))


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


@contextmanager
def run_sleeper_app(client):
    app = client.submit(sleep_until_killed)

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
