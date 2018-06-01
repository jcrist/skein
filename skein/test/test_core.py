from __future__ import print_function, division, absolute_import

import os

import pytest

import skein
from skein.exceptions import FileNotFoundError, FileExistsError


@pytest.fixture(scope="module")
def security(tmpdir_factory):
    return skein.Security.new_key_pair(str(tmpdir_factory.mktemp('security')))


@pytest.fixture(scope="module")
def client(security):
    with skein.Client.temporary(security=security) as client:
        yield client


def test_security(tmpdir):
    path = str(tmpdir)
    s1 = skein.Security.new_key_pair(path)
    s2 = skein.Security.from_directory(path)
    assert s1 == s2

    with pytest.raises(FileExistsError):
        skein.Security.new_key_pair(path)

    # Test force=True
    with open(s1.cert_path) as fil:
        data = fil.read()

    s1 = skein.Security.new_key_pair(path, force=True)

    with open(s1.cert_path) as fil:
        data2 = fil.read()

    assert data != data2

    os.remove(s1.cert_path)
    with pytest.raises(FileNotFoundError):
        skein.Security.from_directory(path)
