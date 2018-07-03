from __future__ import absolute_import, print_function, division

import os
from contextlib import contextmanager

import pytest
import yaml

import skein
from skein.exceptions import context
from skein.cli import main
from skein.test.conftest import (run_application, sleep_until_killed,
                                 check_is_shutdown, wait_for_containers,
                                 set_skein_config)


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
def ensure_app_shutdown(client, app_id):
    try:
        yield
    finally:
        client.kill(app_id)
    check_is_shutdown(client, app_id)


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
                          'application describe',
                          'application kill',
                          'application shutdown',
                          'container',
                          'container scale',
                          'container kill',
                          'container ls',
                          'kv',
                          'kv get',
                          'kv set',
                          'kv del'])
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
    with open(security.cert_path) as f:
        cert = f.read()

    with open(security.key_path) as f:
        key = f.read()

    # Running again fails due to missing --force
    run_command('config gencerts', error=True)
    out, err = capsys.readouterr()
    assert not out
    assert err.startswith('Error: ')
    assert 'already exists' in err

    # files aren't overwritten on error
    with open(security.cert_path) as f:
        cert2 = f.read()

    with open(security.key_path) as f:
        key2 = f.read()

    assert cert == cert2
    assert key == key2

    # Run again with --force
    run_command('config gencerts --force')
    out, err = capsys.readouterr()
    assert not out
    assert not err

    # Files are overwritten
    with open(security.cert_path) as f:
        cert2 = f.read()

    with open(security.key_path) as f:
        key2 = f.read()

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

    with ensure_app_shutdown(global_client, app_id):
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

        # `skein application describe`
        run_command('application describe %s' % app_id)
        out, err = capsys.readouterr()
        assert not err
        skein.ApplicationSpec.from_yaml(out)

        # `skein application describe --service sleeper`
        run_command('application describe %s --service sleeper' % app_id)
        out, err = capsys.readouterr()
        assert not err
        services = yaml.safe_load(out)
        assert len(services) == 1
        skein.Service.from_dict(services['sleeper'])

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


def test_cli_kv(global_client, capsys):
    with run_application(global_client) as app:
        # Wait until started
        app.connect()
        app_id = app.app_id

        # Set keys
        run_command('kv set %s --key foo --value bar' % app_id)
        run_command('kv set %s --key fizz --value buzz' % app_id)
        out, err = capsys.readouterr()
        assert not out
        assert not err

        # Get key
        run_command('kv get %s --key foo' % app_id)
        out, err = capsys.readouterr()
        assert not err
        assert out == 'bar\n'

        # Get whole key-value store
        run_command('kv get %s' % app_id)
        out, err = capsys.readouterr()
        assert not err
        assert out == ('fizz: buzz\n'
                       'foo: bar\n')

        # Delete key
        run_command('kv del %s --key fizz' % app_id)
        out, err = capsys.readouterr()
        assert not out
        assert not err

        # Get missing key
        run_command('kv get %s --key fizz' % app_id, error=True)
        out, err = capsys.readouterr()
        assert not out
        assert "Error: Key 'fizz' is not set\n" == err

        # Kill application
        run_command('application kill %s' % app_id)
        out, err = capsys.readouterr()
        assert not out
        assert not err


def test_cli_container(global_client, capsys):
    with run_application(global_client) as app:
        app_id = app.app_id

        ac = app.connect()
        wait_for_containers(ac, 1, states=['RUNNING'])

        # skein container scale
        run_command('container scale %s --service sleeper --number 3' % app_id)
        out, err = capsys.readouterr()
        assert not out
        assert not err
        wait_for_containers(ac, 3, services=['sleeper'], states=['RUNNING'])

        # skein container ls
        run_command('container ls %s' % app_id)
        out, err = capsys.readouterr()
        assert not err
        assert len(out.splitlines()) == 4

        # skein container kill
        container_id = ac.containers()[0].id
        run_command('container kill %s --id %s' % (app_id, container_id))
        out, err = capsys.readouterr()
        assert not out
        assert not err
        wait_for_containers(ac, 2, services=['sleeper'], states=['RUNNING'])

        # `skein container ls -a`
        run_command('container ls %s -a' % app_id)
        out, err = capsys.readouterr()
        assert not err
        assert container_id in out

        # Errors bubble up nicely
        run_command('container kill %s --id foobar_0' % app_id, error=True)
        out, err = capsys.readouterr()
        assert not out
        assert err.startswith('Error: ')
