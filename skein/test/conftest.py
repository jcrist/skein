from __future__ import print_function, division, absolute_import

import os
import subprocess

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
