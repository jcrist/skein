from __future__ import print_function, division, absolute_import

import os
import subprocess
import time
import weakref

import pytest

import skein
from skein.core import Properties
from skein.exceptions import FileNotFoundError, FileExistsError
from skein.test.conftest import (run_application, wait_for_containers,
                                 wait_for_completion, get_logs, KEYTAB_PATH,
                                 pid_exists)


def test_properties():
    assert len(skein.properties) == len(dict(skein.properties))
    assert skein.properties.config_dir == skein.properties['config_dir']
    assert 'config_dir' in dir(skein.properties)
    assert 'missing' not in skein.properties

    with pytest.raises(AttributeError):
        skein.properties.missing

    with pytest.raises(AttributeError):
        skein.properties.missing = 1

    with pytest.raises(KeyError):
        skein.properties['missing']

    with pytest.raises(TypeError):
        skein.properties['missing'] = 1


def test_security(tmpdir):
    path = str(tmpdir)
    s1 = skein.Security.new_credentials()
    s2 = s1.to_directory(path)
    s3 = skein.Security.from_directory(path)
    assert s1 != s2
    assert s2 == s3

    with pytest.raises(FileExistsError):
        s1.to_directory(path)

    s3 = skein.Security.new_credentials()
    s3.to_directory(path, force=True)

    cert_path = os.path.join(path, 'skein.crt')

    with open(cert_path, 'rb') as fil:
        cert3 = fil.read()
    assert s3.cert_bytes == cert3
    assert s3.cert_bytes != s1.cert_bytes

    os.remove(cert_path)
    with pytest.raises(FileNotFoundError):
        skein.Security.from_directory(path)


def test_security_get_bytes(tmpdir):
    path = str(tmpdir)
    s1 = skein.Security.new_credentials()
    s2 = s1.to_directory(path)

    for kind in ['cert', 'key']:
        x1 = s1._get_bytes(kind)
        x2 = s2._get_bytes(kind)
        assert isinstance(x1, bytes)
        assert isinstance(x2, bytes)
        assert x1 == x2

    not_local = skein.Security(cert_file="hdfs:///some/path",
                               key_file="hdfs:///some/path")
    missing = skein.Security(cert_file="definitely/a/missing/path",
                             key_file="definitely/a/missing/path")

    for kind in ['cert', 'key']:
        with pytest.raises(ValueError):
            not_local._get_bytes(kind)

        with pytest.raises(FileNotFoundError):
            missing._get_bytes(kind)


def test_security_auto_inits(skein_config):
    with pytest.warns(None) as rec:
        sec = skein.Security.from_default()

    assert len(rec) == 1
    assert 'Skein global security credentials not found' in str(rec[0])

    cert_path = os.path.join(skein.properties.config_dir, 'skein.crt')
    key_path = os.path.join(skein.properties.config_dir, 'skein.crt')

    assert os.path.exists(cert_path)
    assert os.path.exists(key_path)

    with pytest.warns(None) as rec:
        sec2 = skein.Security.from_default()

    assert not rec
    assert sec == sec2


def test_client(security, kinit, tmpdir):
    logpath = str(tmpdir.join("log.txt"))

    with skein.Client(security=security, log=logpath) as client:
        # smoketests
        client.get_applications()
        repr(client)

        client2 = skein.Client(address=client.address, security=security)
        assert client2._proc is None

        # smoketests
        client2.get_applications()
        repr(client2)

    # Process was definitely closed
    assert not pid_exists(client._proc.pid)

    # no-op to call close again
    client.close()

    # Log was written
    assert os.path.exists(logpath)
    with open(logpath) as fil:
        assert len(fil.read()) > 0

    # Connection error on closed client
    with pytest.raises(skein.ConnectionError):
        client2.get_applications()

    # Connection error on connecting to missing driver
    with pytest.raises(skein.ConnectionError):
        skein.Client(address=client.address, security=security)


def test_client_closed_when_reference_dropped(security, kinit):
    client = skein.Client(security=security, log=False)
    ref = weakref.ref(client)

    pid = client._proc.pid

    del client
    assert ref() is None
    assert not pid_exists(pid)


def test_client_errors_nicely_if_not_logged_in(security, not_logged_in):
    appid = 'application_1526134340424_0012'

    spec = skein.ApplicationSpec(
        name="should_never_get_to_run",
        queue="default",
        services={
            'service': skein.Service(
                resources=skein.Resources(memory=128, vcores=1),
                script='env')
        }
    )

    with skein.Client(security=security) as client:
        for func, args in [('get_applications', ()),
                           ('application_report', (appid,)),
                           ('connect', (appid,)),
                           ('kill_application', (appid,)),
                           ('submit', (spec,))]:
            with pytest.raises(skein.DriverError) as exc:
                getattr(client, func)(*args)
            assert 'kinit' in str(exc.value)


def test_client_starts_without_java_home(monkeypatch, tmpdir, security, kinit):
    monkeypatch.delenv('JAVA_HOME', raising=False)

    logpath = str(tmpdir.join("log.txt"))

    with skein.Client(security=security, log=logpath) as client:
        # do an operation to ensure everything is working
        client.get_applications()

    with open(logpath) as fil:
        data = fil.read()
        assert 'WARN' not in data
        assert 'native-hadoop' not in data


def test_client_set_log_level(security, kinit, tmpdir):
    logpath = str(tmpdir.join("log.txt"))

    with skein.Client(security=security, log=logpath, log_level='debug') as client:
        # do an operation to ensure everything is working
        client.get_applications()

    with open(logpath) as fil:
        data = fil.read()
        assert 'DEBUG' in data


@pytest.mark.parametrize('use_env', [True, False])
def test_client_forward_java_options(use_env, security, kinit, tmpdir, monkeypatch):
    logpath = str(tmpdir.join("log.txt"))

    if use_env:
        monkeypatch.setenv('SKEIN_DRIVER_JAVA_OPTIONS',
                           '-Dskein.log.level=debug')
        kwargs = {}
    else:
        kwargs = {'java_options': ['-Dskein.log.level=debug']}

    with skein.Client(security=security, log=logpath, **kwargs) as client:
        # do an operation to ensure everything is working
        client.get_applications()

    with open(logpath) as fil:
        data = fil.read()
        assert 'DEBUG' in data


def test_client_login_from_keytab(security, not_logged_in):
    with skein.Client(principal='testuser', keytab=KEYTAB_PATH,
                      security=security) as client:
        # login worked
        client.get_applications()

    # Improper principal/keytab pair
    with pytest.raises(skein.DriverError):
        skein.Client(principal='not_the_right_user', keytab=KEYTAB_PATH,
                     security=security)

    # Keytab file missing
    with pytest.raises(FileNotFoundError):
        skein.Client(principal='testuser', keytab='/not/a/real/path',
                     security=security)

    # Must specify both principal and keytab
    with pytest.raises(ValueError):
        skein.Client(principal='testuser', security=security)

    with pytest.raises(ValueError):
        skein.Client(keytab=KEYTAB_PATH, security=security)


def test_appclient_and_security_in_container(monkeypatch, tmpdir, security):
    # Not running in a container
    with pytest.raises(ValueError) as exc:
        skein.ApplicationClient.from_current()
    assert str(exc.value) == "Not running inside a container"

    # Patch environment variables so it looks like a container
    app_id = 'application_1526134340424_0012'
    container_id = 'container_1526134340424_0012_01_000005'
    address = 'edge.example.com:8765'
    bad_dir = str(tmpdir.mkdir("nothing_in_here"))

    for key, val in [('SKEIN_APPLICATION_ID', app_id),
                     ('CONTAINER_ID', container_id),
                     ('SKEIN_APPMASTER_ADDRESS', address),
                     ('LOCAL_DIRS', bad_dir)]:
        monkeypatch.setenv(key, val)

    monkeypatch.setattr(skein.core, 'properties', Properties())

    # In container, but unable to find security configuration
    with pytest.raises(FileNotFoundError) as exc:
        skein.ApplicationClient.from_current()
    assert str(exc.value) == "Failed to resolve .skein.{crt,pem} in 'LOCAL_DIRS'"

    with pytest.raises(FileNotFoundError) as exc:
        skein.Security.from_default()
    assert str(exc.value) == "Failed to resolve .skein.{crt,pem} in 'LOCAL_DIRS'"

    # Add proper LOCAL_DIRS environment
    good_dir = tmpdir.mkdir('good_dir')
    local_dir = good_dir.mkdir(container_id)
    with open(str(local_dir.join(".skein.crt")), 'wb') as fil:
        fil.write(security._get_bytes('cert'))
    with open(str(local_dir.join(".skein.pem")), 'wb') as fil:
        fil.write(security._get_bytes('key'))
    monkeypatch.setenv('LOCAL_DIRS', '%s,%s' % (bad_dir, good_dir))

    # Picks up full configuration from environment variables
    app = skein.ApplicationClient.from_current()
    assert app.id == app_id
    assert app.address == address

    security2 = skein.Security.from_default()
    assert security._get_bytes('key') == security2._get_bytes('key')
    assert security._get_bytes('cert') == security2._get_bytes('cert')


def test_simple_app(client):
    with run_application(client) as app:
        # smoketest repr
        repr(app)

        # Test get_specification
        a = app.get_specification()
        assert isinstance(a, skein.ApplicationSpec)
        assert 'sleeper' in a.services

        assert client.application_report(app.id).state == 'RUNNING'

        app.shutdown()

    with pytest.raises(skein.ConnectionError):
        client.connect(app.id)

    with pytest.raises(skein.ConnectionError):
        client.connect(app.id, wait=False)

    # On Travis CI there can be some lag between application being shutdown and
    # application actually shutting down. Retry up to 5 seconds before failing.
    with pytest.raises(skein.ConnectionError):
        timeout = 5
        while timeout:
            try:
                app.get_specification()
            except skein.ConnectionError:
                raise
            else:
                # Didn't fail, try again later
                time.sleep(0.1)
                timeout -= 0.1

    running_apps = client.get_applications()
    assert app.id not in {a.id for a in running_apps}

    finished_apps = client.get_applications(states=['finished'])
    assert app.id in {a.id for a in finished_apps}


def test_shutdown_arguments(client):
    status = 'killed'
    diagnostics = 'This is a test diagnostic message'

    with run_application(client) as app:
        app.shutdown(status, diagnostics)
        assert wait_for_completion(client, app.id) == 'KILLED'

    # There's a noticeable lag in the YARN resource manager between an
    # application being marked as finished and its diagnostics message being
    # updated. Retry up to 5 seconds before failing.
    timeout = 5
    while timeout:
        report = client.application_report(app.id)
        if report.diagnostics:
            break
        time.sleep(0.1)
        timeout -= 0.1
    assert report.diagnostics == diagnostics
    assert report.final_status == status


def test_dynamic_containers(client):
    with run_application(client) as app:
        initial = wait_for_containers(app, 1, states=['RUNNING'])
        assert initial[0].state == 'RUNNING'
        assert initial[0].service_name == 'sleeper'

        # Scale sleepers up to 3 containers
        new = app.scale('sleeper', 3)
        assert len(new) == 2
        for c in new:
            assert c.state == 'REQUESTED'
        wait_for_containers(app, 3, services=['sleeper'], states=['RUNNING'])

        # Scale down to 1 container
        stopped = app.scale('sleeper', 1)
        assert len(stopped) == 2
        # Stopped oldest 2 instances
        assert stopped[0].instance == 0
        assert stopped[1].instance == 1

        # Scale up to 2 containers
        new = app.scale('sleeper', 2)
        # Calling twice is no-op
        new2 = app.scale('sleeper', 2)
        assert len(new2) == 0
        assert new[0].instance == 3
        current = wait_for_containers(app, 2, services=['sleeper'],
                                      states=['RUNNING'])
        assert current[0].instance == 2
        assert current[1].instance == 3

        # Manually kill instance 3
        app.kill_container('sleeper_3')
        current = app.get_containers()
        assert len(current) == 1
        assert current[0].instance == 2

        # Fine to kill already killed container
        app.kill_container('sleeper_1')

        # All killed containers
        killed = app.get_containers(states=['killed'])
        assert len(killed) == 3
        assert [c.instance for c in killed] == [0, 1, 3]
        # All completed containers have an exit message
        assert all(c.exit_message for c in killed)

        # Can't scale non-existant service
        with pytest.raises(ValueError):
            app.scale('foobar', 2)

        # Can't scale negative
        with pytest.raises(ValueError):
            app.scale('sleeper', -5)

        # Can't kill non-existant container
        with pytest.raises(ValueError):
            app.kill_container('foobar_1')

        with pytest.raises(ValueError):
            app.kill_container('sleeper_500')

        # Invalid container id
        with pytest.raises(ValueError):
            app.kill_container('fooooooo')

        # Can't get containers for non-existant service
        with pytest.raises(ValueError):
            app.get_containers(services=['sleeper', 'missing'])

        app.shutdown()


@pytest.mark.parametrize('runon', ['service', 'master'])
def test_container_environment(runon, client, has_kerberos_enabled):
    script = ('set -e\n'
              'env\n'
              'echo "LOGIN_ID=[$(whoami)]"\n'
              'hdfs dfs -touchz /user/testuser/test_container_permissions\n'
              'yarn application -list')
    kwargs = dict(resources=skein.Resources(memory=512, vcores=1),
                  script=script)
    services = master = None
    if runon == 'service':
        services = {'service': skein.Service(**kwargs)}
    else:
        master = skein.Master(**kwargs)

    spec = skein.ApplicationSpec(name="test_container_permissions_%s" % runon,
                                 queue="default",
                                 services=services,
                                 master=master)

    with run_application(client, spec=spec, connect=False) as app_id:
        assert wait_for_completion(client, app_id) == 'SUCCEEDED'

    logs = get_logs(app_id)
    assert "USER=testuser" in logs
    assert 'SKEIN_APPMASTER_ADDRESS=' in logs
    assert 'SKEIN_APPLICATION_ID=%s' % app_id in logs
    if runon == 'service':
        assert 'SKEIN_CONTAINER_ID=service_0' in logs
    assert 'SKEIN_RESOURCE_MEMORY=512' in logs
    assert 'SKEIN_RESOURCE_VCORES=1' in logs
    assert 'CLASSPATH' not in logs

    if has_kerberos_enabled:
        assert "LOGIN_ID=[testuser]" in logs
        assert "HADOOP_USER_NAME" not in logs
    else:
        assert "LOGIN_ID=[yarn]" in logs
        assert "HADOOP_USER_NAME" in logs


def test_file_systems(client):
    script = 'hdfs dfs -touchz /user/testuser/test_file_systems'
    service = skein.Service(resources=skein.Resources(memory=128, vcores=1),
                            script=script)
    spec = skein.ApplicationSpec(name="test_file_systems",
                                 queue="default",
                                 services={'service': service},
                                 file_systems=["hdfs://master.example.com:9000"])

    with run_application(client, spec=spec) as app:
        assert wait_for_completion(client, app.id) == 'SUCCEEDED'


@pytest.mark.parametrize('use_skein', [True, False])
def test_kill_application_removes_appdir(use_skein, client):
    hdfs = pytest.importorskip('pyarrow.hdfs')

    with run_application(client) as app:
        if use_skein:
            client.kill_application(app.id)
        else:
            subprocess.check_call(["yarn", "application", "-kill", app.id])

    fs = hdfs.connect()
    assert not fs.exists("/user/testuser/.skein/%s" % app.id)


custom_log4j_properties = """
# Root logger option
log4j.rootCategory=INFO, console

# Redirect log messages to console
log4j.appender.console=org.apache.log4j.ConsoleAppender
log4j.appender.console.Target=System.out
log4j.appender.console.layout=org.apache.log4j.PatternLayout
log4j.appender.console.layout.ConversionPattern=CUSTOM-LOG4J-SUCCEEDED %m
"""


def test_custom_log4j_properties(client, tmpdir):
    configpath = str(tmpdir.join("log4j.properties"))
    service = skein.Service(resources=skein.Resources(memory=128, vcores=1),
                            script='ls')
    spec = skein.ApplicationSpec(name="test_custom_log4j_properties",
                                 queue="default",
                                 master=skein.Master(log_config=configpath),
                                 services={'service': service})
    with open(configpath, 'w') as f:
        f.write(custom_log4j_properties)

    with run_application(client, spec=spec) as app:
        assert wait_for_completion(client, app.id) == 'SUCCEEDED'

    logs = get_logs(app.id)
    assert 'CUSTOM-LOG4J-SUCCEEDED' in logs


def test_set_log_level(client):
    service = skein.Service(resources=skein.Resources(memory=128, vcores=1),
                            script='ls')
    spec = skein.ApplicationSpec(name="test_custom_log4j_properties",
                                 queue="default",
                                 master=skein.Master(log_level='debug'),
                                 services={'service': service})

    with run_application(client, spec=spec) as app:
        assert wait_for_completion(client, app.id) == 'SUCCEEDED'

    logs = get_logs(app.id)
    assert 'DEBUG' in logs


@pytest.mark.parametrize('kind', ['master', 'service'])
def test_memory_limit_exceeded(kind, client):
    resources = skein.Resources(memory=128, vcores=1)
    # Allocate noticeably more memory than the 128 MB limit
    script = 'python -c "b = bytearray(int(256e6)); import time; time.sleep(10)"'

    master = services = None
    if kind == 'master':
        master = skein.Master(resources=resources, script=script)
        search_txt = "memory limit"
    else:
        services = {
            'service': skein.Service(resources=resources, script=script)
        }
        search_txt = "memory used"
    spec = skein.ApplicationSpec(name="test_memory_limit_exceeded_%s" % kind,
                                 queue="default",
                                 master=master,
                                 services=services)
    with run_application(client, spec=spec, connect=False) as app_id:
        assert wait_for_completion(client, app_id) == "FAILED"
    logs = get_logs(app_id)
    assert search_txt in logs

    if kind == 'master':
        report = client.application_report(app_id)
        assert 'memory limit' in report.diagnostics


@pytest.mark.parametrize('strict', [False, True])
def test_node_locality(client, strict):
    if strict:
        relax_locality = False
        nodes = ['worker.example.com']
        racks = []
    else:
        relax_locality = True
        nodes = ['not.a.real.host.name']
        racks = ['not.a.real.rack.name']

    service = skein.Service(
        resources=skein.Resources(memory=128, vcores=1),
        script='sleep infinity',
        nodes=nodes,
        racks=racks,
        relax_locality=relax_locality
    )
    spec = skein.ApplicationSpec(name="test_node_locality",
                                 queue="default",
                                 services={"service": service})
    with run_application(client, spec=spec) as app:
        wait_for_containers(app, 1, states=['RUNNING'])
        spec2 = app.get_specification()
        app.shutdown()

    service2 = spec2.services['service']
    assert service2.nodes == nodes
    assert service2.racks == racks
    assert service2.relax_locality == relax_locality


def test_set_application_progress(client):
    with run_application(client) as app:
        app.set_progress(0.5)
        # Give the allocate loop time to update
        time.sleep(2)
        report = client.application_report(app.id)
        assert report.progress == 0.5

        with pytest.raises(ValueError):
            app.set_progress(-0.5)

        with pytest.raises(ValueError):
            app.set_progress(1.5)

        app.shutdown()


def test_proxy_user(client):
    hdfs = pytest.importorskip('pyarrow.hdfs')

    spec = skein.ApplicationSpec(
        name="test_proxy_user",
        user="alice",
        services={
            "service": skein.Service(
                resources=skein.Resources(memory=128, vcores=1),
                script='sleep infinity')
        }
    )
    with run_application(client, spec=spec) as app:
        spec2 = app.get_specification()
        client.kill_application(app.id, user="alice")

    # Alice used throughout process
    assert spec2.user == 'alice'
    for fil in spec2.services['service'].files.values():
        assert fil.source.startswith('hdfs://master.example.com:9000/user/alice')

    # Application directory deleted after kill
    fs = hdfs.connect()
    assert not fs.exists("/user/testuser/.skein/%s" % app.id)


def test_proxy_user_no_permissions(client):
    spec = skein.ApplicationSpec(
        name="test_proxy_user_no_permissions",
        user="bob",
        services={
            'service': skein.Service(
                resources=skein.Resources(memory=128, vcores=1),
                script='env')
        }
    )
    # No permission to submit as user
    with pytest.raises(skein.DriverError) as exc:
        client.submit(spec)

    exc_msg = str(exc.value)
    assert 'testuser' in exc_msg
    assert 'bob' in exc_msg


def test_security_specified(client):
    security = skein.Security.new_credentials()
    spec = skein.ApplicationSpec(
        name="test_security_specified",
        master=skein.Master(security=security,
                            script='sleep infinity')
    )
    with run_application(client, spec=spec) as app:
        assert app.security is security
        assert app.security != client.security

        spec2 = app.get_specification()

        app2 = client.connect(app.id, security=security)
        # Smoketest, can communicate
        app2.get_specification()

        app3 = client.connect(app.id)
        with pytest.raises(skein.ConnectionError):
            # Improper security credentials
            app3.get_specification()

        app.shutdown()

    remote_security = spec2.master.security
    assert remote_security.cert_bytes is None
    assert remote_security.key_bytes is None
    assert remote_security.cert_file.source.startswith('hdfs')
    assert remote_security.key_file.source.startswith('hdfs')


def test_daemon_errors_deprecated():
    with pytest.warns(UserWarning):
        assert isinstance(skein.DriverError(), skein.DaemonError)
    with pytest.warns(UserWarning):
        assert not isinstance(skein.SkeinError(), skein.DaemonError)

    with pytest.warns(UserWarning):
        assert isinstance(skein.DriverNotRunningError(),
                          skein.DaemonNotRunningError)
    with pytest.warns(UserWarning):
        assert not isinstance(skein.SkeinError(), skein.DaemonNotRunningError)


def test_master_driver_foo(client, tmpdir):
    filpath = str(tmpdir.join("dummy-file"))
    with open(filpath, 'w') as fil:
        fil.write('foobar')

    spec = skein.ApplicationSpec(
        name="test_master_driver",
        master=skein.Master(
            script='ls\nenv',
            env={'FOO': 'BAR'},
            files={'myfile': filpath}
        )
    )
    with run_application(client, spec=spec, connect=False) as app_id:
        assert wait_for_completion(client, app_id) == 'SUCCEEDED'

    logs = get_logs(app_id)
    assert 'FOO=BAR' in logs
    assert 'myfile' in logs


@pytest.mark.parametrize('kind, master_cmd, service_cmd', [
    ('service_succeeds', 'sleep infinity', 'exit 0'),
    ('service_fails', 'sleep infinity', 'exit 1'),
    ('driver_succeeds', 'exit 0', 'sleep infinity'),
    ('driver_fails', 'exit 1', 'sleep infinity')
])
def test_master_driver_shutdown_sequence(kind, master_cmd, service_cmd,
                                         client, tmpdir):
    spec = skein.ApplicationSpec(
        name="test_master_driver_shutdown_sequence_%s" % kind,
        master=skein.Master(script=master_cmd),
        services={
            'service': skein.Service(
                resources=skein.Resources(memory=128, vcores=1),
                script=service_cmd
            )
        }
    )

    state = 'SUCCEEDED' if kind.endswith('succeeds') else 'FAILED'

    if kind == 'service_succeeds':
        with run_application(client, spec=spec) as app:
            wait_for_containers(app, 1, states=['SUCCEEDED'])
            assert len(app.get_containers()) == 0
            # App hangs around until driver completes
            app.shutdown()
            assert wait_for_completion(client, app.id) == state
    else:
        with run_application(client, spec=spec, connect=False) as app_id:
            # service_fails results in immediate failure
            # driver_succeeds results in immediate success
            # driver_fails results in immediate failure
            assert wait_for_completion(client, app_id) == state


test_retries_script_template = """
if [[ $CONTAINER_ID =~ container_[0-9]+_[0-9]+_{succeed_on}_[0-9]+ ]]; then
  echo "Succeeding on attempt {succeed_on}"
  exit 0
else
  echo "Failing on other attempts"
  exit 1
fi
"""


def test_retries_succeeds(client):
    hdfs = pytest.importorskip('pyarrow.hdfs')

    spec = skein.ApplicationSpec(
        name="test_application_retries_succeeds",
        max_attempts=2,
        master=skein.Master(
            script=test_retries_script_template.format(succeed_on='02')
        )
    )
    with run_application(client, spec=spec, connect=False) as app_id:
        assert wait_for_completion(client, app_id) == 'SUCCEEDED'
    logs = get_logs(app_id)
    assert 'Failing on other attempts' in logs
    assert 'Application attempt 1 out of 2 failed, will retry' in logs
    assert 'Succeeding on attempt 02' in logs

    fs = hdfs.connect()
    assert not fs.exists("/user/testuser/.skein/%s" % app_id)


def test_retries_fails(client):
    hdfs = pytest.importorskip('pyarrow.hdfs')

    # Global maximum is 2, checks that appmaster uses 2 instead of 10
    max_attempts = 10

    spec = skein.ApplicationSpec(
        name="test_application_retries_fails",
        max_attempts=max_attempts,
        master=skein.Master(
            script=test_retries_script_template.format(succeed_on='03')
        )
    )
    with run_application(client, spec=spec, connect=False) as app_id:
        assert wait_for_completion(client, app_id) == 'FAILED'
    logs = get_logs(app_id)
    assert logs.count('Failing on other attempts') == 2
    assert 'Application attempt 1 out of 2 failed' in logs

    fs = hdfs.connect()
    assert not fs.exists("/user/testuser/.skein/%s" % app_id)
