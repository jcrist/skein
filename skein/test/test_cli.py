from __future__ import absolute_import, print_function, division

import io
import os
import sys
from contextlib import contextmanager

import pytest

import skein
from skein.compatibility import PY2
from skein.exceptions import context
from skein.cli import main
from skein.test.conftest import (run_application, sleep_until_killed,
                                 check_is_shutdown, wait_for_containers,
                                 set_skein_config, ensure_shutdown)


bad_spec_yaml = """
name: bad_spec_file
queue: default
tags:
    - sleeps

services:
    sleeper:
        commands:
            - sleep infinity
"""


def run_command(command, error=False):
    with pytest.raises(SystemExit) as exc:
        main([arg for arg in command.split(' ') if arg])
    assert not context.is_cli
    if error:
        assert exc.value.code != 0
    else:
        assert exc.value.code == 0


@contextmanager
def stop_global_daemon():
    try:
        yield
    finally:
        run_command('daemon stop')


@pytest.fixture(scope='module')
def global_client(kinit, tmpdir_factory):
    with set_skein_config(tmpdir_factory.mktemp('config')):
        run_command('config gencerts')
        try:
            run_command('daemon start')
            yield skein.Client.from_global_daemon()
        finally:
            run_command('daemon stop')


@pytest.mark.parametrize('command',
                         ['',
                          'config',
                          'config gencerts',
                          'daemon',
                          'daemon start',
                          'daemon stop',
                          'daemon restart',
                          'daemon address',
                          'application',
                          'application submit',
                          'application status',
                          'application ls',
                          'application specification',
                          'application kill',
                          'application shutdown',
                          'container',
                          'container scale',
                          'container kill',
                          'container ls',
                          'kv',
                          'kv get',
                          'kv put',
                          'kv del',
                          'kv ls'])
def test_cli_help(command, capsys):
    run_command(command + ' -h')

    out, err = capsys.readouterr()
    assert not err
    assert 'usage: skein' in out


@pytest.mark.parametrize('group',
                         ['', 'daemon', 'application', 'container', 'kv'])
def test_cli_call_command_group(group, capsys):
    run_command(group, error=True)

    out, err = capsys.readouterr()
    assert not out
    assert 'usage: skein' in err


def test_cli_version(capsys):
    run_command('--version')

    out, err = capsys.readouterr()
    assert not err
    assert skein.__version__ in out


def test_cli_config_gencerts(capsys, skein_config):
    # Generate certificates in clean directory
    run_command('config gencerts')
    out, err = capsys.readouterr()
    assert not err
    assert not out

    security = skein.Security.from_directory(skein_config)
    cert = security._get_bytes('cert')
    key = security._get_bytes('key')

    # Running again fails due to missing --force
    run_command('config gencerts', error=True)
    out, err = capsys.readouterr()
    assert not out
    assert err.startswith('Error: ')
    assert 'already exists' in err

    # files aren't overwritten on error
    cert2 = security._get_bytes('cert')
    key2 = security._get_bytes('key')

    assert cert == cert2
    assert key == key2

    # Run again with --force
    run_command('config gencerts --force')
    out, err = capsys.readouterr()
    assert not out
    assert not err

    # Files are overwritten
    cert2 = security._get_bytes('cert')
    key2 = security._get_bytes('key')

    assert cert != cert2
    assert key != key2


def test_works_if_cli_daemon_not_running(capfd, skein_config):
    run_command('application ls')
    out, err = capfd.readouterr()
    assert 'APPLICATION_ID' in out
    assert 'INFO' in err  # daemon logs go to stderr


def test_cli_daemon(capsys, skein_config):
    with stop_global_daemon():
        # Errors if no daemon currently running
        run_command('daemon address', error=True)
        out, err = capsys.readouterr()
        assert not out
        assert 'No skein daemon is running' in err

        # Start daemon without generating certificates
        run_command('daemon start')
        out, err = capsys.readouterr()
        assert "Skein global security credentials not found" in err
        assert 'localhost' in out

        # Daemon start is idempotent
        run_command('daemon start')
        out2, err = capsys.readouterr()
        assert not err
        assert out2 == out

        # Get address
        run_command('daemon address')
        out2, err = capsys.readouterr()
        assert not err
        assert out2 == out

        # Restart daemon
        run_command('daemon restart')
        out2, err = capsys.readouterr()
        assert not err
        assert out2 != out

        # Stop daemon
        run_command('daemon stop')
        out, err = capsys.readouterr()
        assert not out
        assert not err

        # Stop is idempotent
        run_command('daemon stop')
        out, err = capsys.readouterr()
        assert not out
        assert not err


def test_cli_application_submit_errors(tmpdir, capsys, global_client):
    spec_path = os.path.join(str(tmpdir), 'spec.yaml')

    # No spec at path
    run_command('application submit %s' % spec_path, error=True)
    out, err = capsys.readouterr()
    assert not out
    assert 'No application specification file' in err
    assert spec_path in err

    # Error in file
    with open(spec_path, 'w') as f:
        f.write(bad_spec_yaml)
    run_command('application submit %s' % spec_path, error=True)
    out, err = capsys.readouterr()
    assert not out
    assert ('Error: In file %r' % spec_path) in err


def test_cli_application(tmpdir, capsys, global_client):
    spec_path = os.path.join(str(tmpdir), 'spec.yaml')
    sleep_until_killed.to_file(spec_path)

    run_command('application submit %s' % spec_path)
    out, err = capsys.readouterr()
    assert not err

    app_id = out.strip()

    with ensure_shutdown(global_client, app_id):
        # Wait for app to start
        global_client.connect(app_id)

        # `skein application status`
        run_command('application status %s' % app_id)
        out, err = capsys.readouterr()
        assert not err
        assert len(out.splitlines()) == 2
        assert 'RUNNING' in out

        # `skein application ls`
        run_command('application ls')
        out, err = capsys.readouterr()
        assert not err
        assert len(out.splitlines()) >= 2
        assert app_id in out

        # `skein application specification`
        run_command('application specification %s' % app_id)
        out, err = capsys.readouterr()
        assert not err
        skein.ApplicationSpec.from_yaml(out)

        # `skein application shutdown`
        run_command('application shutdown %s' % app_id)
        out, err = capsys.readouterr()
        assert not out
        assert not err
        check_is_shutdown(global_client, app_id, 'SUCCEEDED')

        # `skein application ls -a`
        run_command('application ls -a')
        out, err = capsys.readouterr()
        assert not err
        assert app_id in out


def test_cli_kv(global_client, capfdbinary):
    with run_application(global_client) as app:
        # List empty key-value store
        run_command('kv ls %s' % app.id)
        out, err = capfdbinary.readouterr()
        assert not err
        assert not out

        # Set keys
        run_command('kv put %s --key foo --value bar' % app.id)
        run_command('kv put %s --key fizz --value buzz' % app.id)
        out, err = capfdbinary.readouterr()
        assert not out
        assert not err

        # Set key from stdin. Not valid unicode.
        bytes_val = b'H\x9e[\x0e\xa6~\x7fVb\xea'
        buf = io.BytesIO(bytes_val)
        mock_stdin = buf if PY2 else io.TextIOWrapper(buf)
        old_stdin = sys.stdin
        try:
            sys.stdin = mock_stdin
            run_command('kv put %s --key from_stdin' % app.id)
        except Exception:
            sys.stdin = old_stdin

        # Get key
        run_command('kv get %s --key foo' % app.id)
        out, err = capfdbinary.readouterr()
        assert not err
        assert out == b'bar\n'

        # Get binary key
        run_command('kv get %s --key from_stdin' % app.id)
        out, err = capfdbinary.readouterr()
        assert not err
        assert out == bytes_val + b'\n'

        # List whole key-value store
        run_command('kv ls %s' % app.id)
        out, err = capfdbinary.readouterr()
        assert not err
        assert out == b'fizz\nfoo\nfrom_stdin\n'

        # Delete key
        run_command('kv del %s --key fizz' % app.id)
        out, err = capfdbinary.readouterr()
        assert not out
        assert not err

        # Get missing key
        run_command('kv get %s --key fizz' % app.id, error=True)
        out, err = capfdbinary.readouterr()
        assert not out
        assert ((b"Error: Key %s is not set\n"
                 % (b"u'fizz'" if PY2 else b"'fizz'")) == err)

        # Kill application
        run_command('application kill %s' % app.id)
        out, err = capfdbinary.readouterr()
        assert not out
        assert not err


def test_cli_container(global_client, capsys):
    with run_application(global_client) as app:
        wait_for_containers(app, 1, states=['RUNNING'])

        # skein container scale
        run_command('container scale %s --service sleeper --number 3' % app.id)
        out, err = capsys.readouterr()
        assert not out
        assert not err
        wait_for_containers(app, 3, services=['sleeper'], states=['RUNNING'])

        # skein container ls
        run_command('container ls %s' % app.id)
        out, err = capsys.readouterr()
        assert not err
        assert len(out.splitlines()) == 4

        # skein container kill
        container_id = app.get_containers()[0].id
        run_command('container kill %s --id %s' % (app.id, container_id))
        out, err = capsys.readouterr()
        assert not out
        assert not err
        wait_for_containers(app, 2, services=['sleeper'], states=['RUNNING'])

        # `skein container ls -a`
        run_command('container ls %s -a' % app.id)
        out, err = capsys.readouterr()
        assert not err
        assert container_id in out

        # Errors bubble up nicely
        run_command('container kill %s --id foobar_0' % app.id, error=True)
        out, err = capsys.readouterr()
        assert not out
        assert err.startswith('Error: ')

        app.shutdown()
