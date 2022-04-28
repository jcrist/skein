import copy
import datetime
import math
import os
import pickle

import pytest

from skein.model import (ApplicationSpec, Service, Resources, File,
                         ApplicationState, FinalStatus, FileType, ACLs, Master,
                         DelegationTokenProvider, Container, ApplicationReport,
                         ResourceUsageReport, NodeReport, LogLevel, parse_memory,
                         Security, Queue, ApplicationLogs)


def indent(s, n):
    pad = ' ' * n
    return '\n'.join(pad + l for l in s.splitlines())


service_spec = """\
node_label: gpu
resources:
    vcores: 1
    memory: 1 GiB
files:
    testfile: /path/to/testfile
    testarchive: /path/to/testarchive.zip
    otherpath:
        source: /path/to/other
        type: file
env:
    key1: val1
    key2: val2
script: |-
    command 1
    command 2
nodes:
    - worker.example.com
relax_locality: true
"""

app_spec = """\
name: test
queue: default
node_label: cpu
max_attempts: 2
tags:
    - tag1
    - tag2
file_systems:
    - hdfs://preprod

acls:
    enable: true
    view_users:
        - '*'

services:
    service_1:
%s""" % indent(service_spec, 8)


def check_base_methods(obj, obj2):
    # equality
    assert obj == copy.deepcopy(obj)
    assert not (obj != copy.deepcopy(obj))
    assert obj2 == copy.deepcopy(obj2)
    assert obj != obj2
    assert obj != 'incorrect_type'

    # smoketest repr
    repr(obj)

    cls = type(obj)
    proto = obj.to_protobuf()
    obj2 = cls.from_protobuf(proto)
    assert obj == obj2


def check_specification_methods(obj, obj2):
    check_base_methods(obj, obj2)

    # conversions
    cls = type(obj)
    for skip in [True, False]:
        for method in ['json', 'yaml', 'dict']:
            msg = getattr(obj, 'to_' + method)(skip_nulls=skip)
            obj2 = getattr(cls, 'from_' + method)(msg)
            assert obj == obj2


def test_parse_memory():
    mib = 2 ** 20
    assert parse_memory('100') == 100
    assert parse_memory('100 MiB') == 100
    assert parse_memory('100 MB') == math.ceil(100 * 1e6 / mib)
    assert parse_memory('100M') == math.ceil(100 * 1e6 / mib)
    assert parse_memory('100Mi') == 100
    assert parse_memory('5kB') == 1
    assert parse_memory('5.4 MiB') == 6
    assert parse_memory('0.9 MiB') == 1
    assert parse_memory('1e3') == 1000
    assert parse_memory('1e6 kB') == math.ceil(1e6 * 1e3 / mib)
    assert parse_memory('MiB') == 1

    with pytest.raises(ValueError):
        parse_memory('5 foos')

    with pytest.raises(ValueError):
        parse_memory('')

    with pytest.raises(ValueError):
        parse_memory('1.1.1GB')

    with pytest.raises(ValueError):
        parse_memory(-1)

    with pytest.raises(ValueError):
        parse_memory('-1.5 MiB')

    with pytest.raises(TypeError):
        parse_memory([])


def test_resources():
    r = Resources(memory=1024, vcores=1)
    r2 = Resources(memory=1024, vcores=2)
    r3 = Resources(memory=1, vcores=2, gpus=3, fpgas=4)
    check_specification_methods(r, r2)
    check_specification_methods(r3, r2)


def test_resources_invariants():
    assert Resources(memory='1024 MiB', vcores=1).memory == 1024
    assert Resources(memory='5 GiB', vcores=1).memory == 5 * 1024
    assert Resources(memory='512', vcores=1).memory == 512
    assert Resources(memory=1e3, vcores=1).memory == 1000
    assert Resources(memory='0.9 MiB', vcores=1).memory == 1

    r = Resources(memory='1 GiB', vcores=1)
    assert r.memory == 1024
    r.memory = '2 GiB'
    assert r.memory == 2048
    r.memory = 1e3
    assert r.memory == 1000

    with pytest.raises(ValueError):
        r.memory = -1

    with pytest.raises(ValueError):
        r.memory = -1

    with pytest.raises(TypeError):
        Resources()

    with pytest.raises(TypeError):
        Resources(None, None)

    with pytest.raises(ValueError):
        Resources(memory='foo', vcores=1)

    with pytest.raises(TypeError):
        Resources(memory=1, vcores='foo')

    with pytest.raises(ValueError):
        Resources(memory=-1, vcores=-1)


def test_file():
    fil = File(source='/test/path')
    fil2 = File(source='/test/path', size=1024)
    check_specification_methods(fil, fil2)


def test_file_invariants():
    with pytest.raises(TypeError):
        File()

    with pytest.raises(TypeError):
        File(1)

    with pytest.raises(ValueError):
        File('/foo/bar.zip', type='invalid')

    with pytest.raises(ValueError):
        File('/foo/bar.zip', visibility='invalid')

    fil = File(source='/test/path')

    with pytest.raises(ValueError):
        fil.type = 'invalid'

    with pytest.raises(ValueError):
        fil.visibility = 'invalid'

    # relative paths
    sol = 'file://%s' % os.path.join(os.getcwd(), 'foo/bar.zip')
    assert File('foo/bar.zip').source == sol

    # relative path, with _origin specified (only used when reading from file)
    f = File.from_dict({'source': '../../file'}, _origin='/path/to/origin/spot')
    assert f.source == 'file:///path/to/file'

    # scheme specified
    assert File('hdfs:///foo/bar.zip').source == 'hdfs:///foo/bar.zip'
    assert (File('hdfs://foo.com:9000/foo/bar.zip').source ==
            'hdfs://foo.com:9000/foo/bar.zip')

    assert (File(source='/test/path', type='file') ==
            File(source='/test/path', type='FILE'))

    assert File(source='/test/path').type == FileType.FILE
    assert File(source='/test/path.zip').type == FileType.ARCHIVE
    f = File(source='/test/path.zip', type='file')
    assert f.type == FileType.FILE
    f.type = 'archive'
    assert f.type == FileType.ARCHIVE


def test_acls():
    acl1 = ACLs()
    acl2 = ACLs(enable=True, view_users=['ted', 'nancy'])
    check_specification_methods(acl1, acl2)


def test_acls_invariants():
    with pytest.raises(TypeError):
        ACLs(enable=1)

    with pytest.raises(TypeError):
        ACLs(view_users="*")


def test_security(tmpdir):
    bytes = Security.new_credentials()
    file = bytes.to_directory(str(tmpdir))
    other = Security.new_credentials()
    check_specification_methods(bytes, other)
    check_specification_methods(file, other)


def test_security_invariants():
    # relative paths
    path = 'foo/bar'
    sol = 'file://%s' % os.path.join(os.getcwd(), path)
    s = Security(cert_file=path, key_file=path)
    assert s.cert_file.source == sol
    assert s.key_file.source == sol

    keywords = ['cert_file', 'cert_bytes', 'key_file', 'key_bytes']
    for keyword in keywords:
        kwargs = dict.fromkeys(keywords, 'foo/bar')
        kwargs[keyword] = 1
        with pytest.raises(TypeError):
            Security(**kwargs)

    # Must specify one
    for keyword in keywords:
        with pytest.raises(ValueError):
            Security(**{keyword: 'foo/bar'})

    # Can't specify both
    with pytest.raises(ValueError):
        Security(cert_file='/path.crt', cert_bytes=b'foobar',
                 key_file='/path.pem')

    with pytest.raises(ValueError):
        Security(key_file='/path.pem', key_bytes=b'foobar',
                 cert_file='/path.crt')


def test_master():
    m1 = Master(log_level='debug',
                log_config='/test/path.properties',
                security=Security.new_credentials())
    m2 = Master(resources=Resources(memory='1 GiB', vcores=2),
                script='script',
                env={'FOO': 'BAR'},
                files={'file': '/test/path'})
    m3 = Master()
    check_specification_methods(m1, m3)
    check_specification_methods(m2, m3)


def test_master_invariants():
    with pytest.raises(TypeError):
        Master(log_config=1)

    # Strings are converted to File objects
    m = Master(log_config='/test/path.properties')
    assert isinstance(m.log_config, File)
    assert m.log_config.type == 'file'

    # Relative paths are converted
    sol = 'file://%s' % os.path.join(os.getcwd(), 'foo/bar.properties')
    assert Master(log_config='foo/bar.properties').log_config.source == sol

    # setter/getter
    f = Master(log_level='debug')
    assert f.log_level == LogLevel.DEBUG
    f.log_level = 'info'
    assert f.log_level == LogLevel.INFO

    with pytest.raises(TypeError):
        Master(script=1)

    with pytest.raises(TypeError):
        Master(env={'a': 1})

    # Mutable defaults properly set
    m = Master()
    assert isinstance(m.env, dict)
    assert isinstance(m.files, dict)

    # Strings are converted to File objects
    m = Master(files={'target': '/source.zip',
                      'target2': '/source2.txt'})
    assert m.files['target'].type == 'archive'
    assert m.files['target2'].type == 'file'

    # File targets are checked
    with pytest.raises(ValueError):
        Master(files={'foo/bar': '/source.zip'})
    # Local relative paths are fine
    Master(files={'./bar': '/source.zip'})


def test_delegation_token_provider_spec():
    p1 = DelegationTokenProvider(name='hive',
                                 config={
                                     'hive.jdbc.url': 'hive2://127.0.0.1:10000/myDatabase',
                                     'hive.jdbc.principal': 'hive/my.hadoop.mycompany.com@HADOOP.MYCOMPANY.COM'})
    p3 = DelegationTokenProvider()
    check_specification_methods(p1, p3)


def test_service():
    r = Resources(memory=1024, vcores=1)
    s1 = Service(resources=r,
                 script='script',
                 node_label="testlabel",
                 files={'file': File(source='/test/path')},
                 nodes=['worker.example.com'],
                 racks=['rack1', 'rack2'],
                 relax_locality=True)
    s2 = Service(resources=r,
                 script='script',
                 files={'file': File(source='/test/path', size=1024)})
    check_specification_methods(s1, s2)


def test_service_invariants():
    r = Resources(memory=1024, vcores=1)

    with pytest.raises(TypeError):
        Service()

    # Empty script
    with pytest.raises(ValueError):
        Service(script="", resources=r)

    with pytest.raises(TypeError):
        Service(script=1, resources=r)

    with pytest.raises(ValueError):
        Service(script="script", resources=r, instances=-1)

    with pytest.raises(ValueError):
        Service(script="script", resources=r, max_restarts=-2)

    with pytest.raises(TypeError):
        Service(script="script", resources=r, env={'a': 1})

    with pytest.raises(TypeError):
        Service(script="script", resources=r, depends=[1])

    # Mutable defaults properly set
    s = Service(script="script", resources=r)
    assert isinstance(s.env, dict)
    assert isinstance(s.files, dict)
    assert isinstance(s.depends, set)

    # Strings are converted to File objects
    s = Service(script="script", resources=r,
                files={'target': '/source.zip',
                       'target2': '/source2.txt'})
    assert s.files['target'].type == 'archive'
    assert s.files['target2'].type == 'file'

    # File targets are checked
    with pytest.raises(ValueError):
        Service(script="script", resources=r,
                files={'foo/bar': '/source.zip'})
    # Local relative paths are fine
    Service(script="script", resources=r,
            files={'./bar': '/source.zip'})


def test_application_spec():
    r = Resources(memory=1024, vcores=1)
    s1 = Service(resources=r, script="script",
                 files={'file': File(source='/test/path')})
    s2 = Service(resources=r, script="script",
                 files={'file': File(source='/test/path', size=1024)})
    spec1 = ApplicationSpec(name='test',
                            queue='testqueue',
                            node_label='testlabel',
                            services={'service': s1})
    spec2 = ApplicationSpec(master=Master(script='script', resources=r))
    spec3 = ApplicationSpec(services={'service': s2})
    check_specification_methods(spec1, spec3)
    check_specification_methods(spec2, spec3)


def test_application_spec_invariants():
    s = Service(script="script",
                resources=Resources(memory=1024, vcores=1))

    # No services
    with pytest.raises(ValueError):
        ApplicationSpec(name='dask', queue='default')

    # No master script
    with pytest.raises(ValueError):
        ApplicationSpec(name='dask', queue='default',
                        master=Master())

    for k, v in [('name', 1), ('queue', 1), ('tags', 1),
                 ('tags', {1, 2, 3}), ('max_attempts', 'foo')]:
        with pytest.raises(TypeError):
            ApplicationSpec(services={'service': s}, **{k: v})

    with pytest.raises(ValueError):
        ApplicationSpec(max_attempts=0, services={'service': s})

    r = Resources(memory=1024, vcores=1)

    # Unknown dependency name
    with pytest.raises(ValueError):
        ApplicationSpec(services={'a': s,
                                  'b': Service(resources=r, script="script",
                                               depends=['c', 'd'])})

    # Cyclical dependencies
    with pytest.raises(ValueError):
        ApplicationSpec(services={'a': Service(resources=r, script="script",
                                               depends=['c']),
                                  'b': Service(resources=r, script="script",
                                               depends=['a']),
                                  'c': Service(resources=r, script="script",
                                               depends=['b'])})


def test_service_from_yaml():
    # Check that syntactic sugar for files, etc... is properly parsed
    s = Service.from_yaml(service_spec)
    assert isinstance(s, Service)

    assert s.node_label == 'gpu'

    assert s.resources.vcores == 1
    assert s.resources.memory == 1024

    assert isinstance(s.files, dict)
    fil = s.files['testfile']
    assert fil.source == 'file:///path/to/testfile'
    assert fil.type == 'FILE'
    archive = s.files['testarchive']
    assert archive.source == 'file:///path/to/testarchive.zip'
    assert archive.type == 'ARCHIVE'
    other = s.files['otherpath']
    assert other.source == 'file:///path/to/other'
    assert other.type == 'FILE'

    assert s.env == {'key1': 'val1', 'key2': 'val2'}
    assert s.script == ('command 1\n'
                        'command 2')
    assert s.depends == set()


def test_service_roundtrip():
    s = Service.from_yaml(service_spec)
    s2 = Service.from_yaml(s.to_yaml())
    assert s == s2


def test_application_spec_from_yaml():
    spec = ApplicationSpec.from_yaml(app_spec)
    assert isinstance(spec, ApplicationSpec)

    assert spec.name == 'test'
    assert spec.queue == 'default'
    assert spec.node_label == 'cpu'
    assert spec.tags == {'tag1', 'tag2'}
    assert spec.file_systems == ['hdfs://preprod']
    assert spec.max_attempts == 2
    assert spec.acls.enable
    assert spec.acls.view_users == ['*']
    assert isinstance(spec.services, dict)
    assert isinstance(spec.services['service_1'], Service)


def test_application_spec_roundtrip():
    spec = ApplicationSpec.from_yaml(app_spec)
    spec2 = ApplicationSpec.from_yaml(spec.to_yaml())
    assert spec == spec2


def test_to_file_from_file(tmpdir):
    spec = ApplicationSpec.from_yaml(app_spec)

    for name, format in [('test.yaml', 'infer'),
                         ('test.json', 'infer'),
                         ('test2.yaml', 'json')]:
        path = os.path.join(str(tmpdir), name)
        assert not os.path.exists(path)
        spec.to_file(path, format=format)
        assert os.path.exists(path)
        spec2 = ApplicationSpec.from_file(path, format=format)
        assert spec == spec2

    for name, format in [('bad.yaml', 'invalid'), ('bad.invalid', 'infer')]:
        path = os.path.join(str(tmpdir), name)
        with pytest.raises(ValueError):
            spec.to_file(path, format=format)
        assert not os.path.exists(path)


def test_application_spec_from_any(tmpdir):
    spec = ApplicationSpec.from_yaml(app_spec)
    spec_path = os.path.join(str(tmpdir), 'test.yaml')
    spec.to_file(spec_path)
    spec_dict = spec.to_dict()

    for obj in [spec, spec_path, spec_dict]:
        spec2 = ApplicationSpec._from_any(obj)
        assert spec == spec2

    with pytest.raises(TypeError):
        ApplicationSpec._from_any(None)


def test_enums():
    assert type(ApplicationState.RUNNING) is ApplicationState
    assert ApplicationState.RUNNING is ApplicationState('RUNNING')
    assert ApplicationState.RUNNING is ApplicationState('running')
    assert ApplicationState.RUNNING is ApplicationState(ApplicationState.RUNNING)
    assert ApplicationState.RUNNING == ApplicationState.RUNNING
    assert ApplicationState.RUNNING == 'RUNNING'
    assert not ApplicationState.RUNNING != 'RUNNING'
    assert ApplicationState.RUNNING == 'running'
    assert ApplicationState.RUNNING != 'foo'

    assert ApplicationState.KILLED != FinalStatus.KILLED

    assert ApplicationState.KILLED in {ApplicationState.KILLED,
                                       ApplicationState.FINISHED}

    assert len(ApplicationState) == len(ApplicationState.values())
    assert tuple(ApplicationState) == ApplicationState.values()
    assert repr(ApplicationState.RUNNING) == "ApplicationState.RUNNING"
    assert str(ApplicationState.RUNNING) == 'RUNNING'

    assert (pickle.loads(pickle.dumps(ApplicationState.RUNNING))
            is ApplicationState.RUNNING)

    with pytest.raises(TypeError):
        ApplicationState(FinalStatus.KILLED)

    with pytest.raises(TypeError):
        ApplicationState(1)

    with pytest.raises(ValueError):
        ApplicationState('foobar')


def test_container():
    start = datetime.datetime(2018, 6, 7, 23, 24, 25, 26 * 1000)
    finish = datetime.datetime(2018, 6, 7, 23, 21, 25, 26 * 1000)

    kwargs = dict(service_name="foo",
                  instance=0,
                  yarn_container_id='container_1528138529205_0038_01_000001',
                  yarn_node_http_address='worker.example.com:14420',
                  exit_message="")

    c = Container(state='RUNNING',
                  start_time=start,
                  finish_time=None,
                  **kwargs)
    c2 = Container(state='SUCCEEDED',
                   start_time=start,
                   finish_time=finish,
                   **kwargs)
    c3 = Container(state='WAITING',
                   start_time=None,
                   finish_time=None,
                   **kwargs)

    check_base_methods(c, c2)

    assert c.id == "foo_0"

    assert c2.runtime == c2.finish_time - c2.start_time

    before = datetime.datetime.now()
    runtime = c.runtime
    after = datetime.datetime.now()
    assert (before - c.start_time) <= runtime <= (after - c.start_time)

    assert c3.runtime == datetime.timedelta(0)


def test_resource_usage_report():
    r1 = Resources(memory=128, vcores=1)
    r2 = Resources(memory=256, vcores=2)
    r3 = Resources(memory=384, vcores=3)

    a = ResourceUsageReport(10, 20, 2, r1, r2, r3)
    b = ResourceUsageReport(11, 20, 2, r1, r2, r3)

    check_base_methods(a, b)


def test_application_report():
    usage = ResourceUsageReport(10, 20, 2,
                                Resources(memory=128, vcores=1),
                                Resources(memory=256, vcores=2),
                                Resources(memory=384, vcores=3))

    start = datetime.datetime(2018, 6, 7, 23, 24, 25, 26 * 1000)
    finish = datetime.datetime(2018, 6, 7, 23, 21, 25, 26 * 1000)

    kwargs = dict(id='application_1528138529205_0001',
                  name='test',
                  user='testuser',
                  queue='default',
                  tags=['foo', 'bar', 'baz'],
                  host='worker.example.com',
                  port=8181,
                  tracking_url='',
                  usage=usage,
                  diagnostics='')

    a = ApplicationReport(state='running',
                          final_status='undefined',
                          progress=0.5,
                          start_time=start,
                          finish_time=None,
                          **kwargs)

    b = ApplicationReport(state='finished',
                          final_status='succeeded',
                          progress=1.0,
                          start_time=start,
                          finish_time=finish,
                          **kwargs)

    check_base_methods(a, b)

    assert b.runtime == b.finish_time - b.start_time
    before = datetime.datetime.now()
    runtime = a.runtime
    after = datetime.datetime.now()
    assert (before - a.start_time) <= runtime <= (after - a.start_time)


def test_node_report():
    a = NodeReport(id='worker1.example.com:34721',
                   http_address='worker1.example.com:8042',
                   rack_name='/default-rack',
                   labels={'gpu', 'fpga'},
                   health_report='some words here',
                   state='RUNNING',
                   total_resources=Resources(memory=8192, vcores=8),
                   used_resources=Resources(memory=0, vcores=0))

    b = NodeReport(id='worker2.example.com:34721',
                   http_address='worker2.example.com:8042',
                   rack_name='/default-rack',
                   state='NEW',
                   labels=set(),
                   health_report='',
                   total_resources=Resources(memory=8192, vcores=8),
                   used_resources=Resources(memory=0, vcores=0))

    check_base_methods(a, b)

    assert a.host == 'worker1.example.com'
    assert a.port == 34721


def test_queue():
    a = Queue('queue1', 'RUNNING', 50.0, 60.0, 80.0, set(), '')
    b = Queue('queue2', 'RUNNING', 40.0, 60.0, 30.0, set('*'), '')
    check_base_methods(a, b)


def test_application_logs():
    raw = {"container_1528138529205_0038_01_000001": "line 1\nline 2\nline 3",
           "container_1528138529205_0038_01_000002": "line 4\nline 5\nline 6"}

    logs = ApplicationLogs('application_1528138529205_0001', raw)
    assert repr(logs) == "ApplicationLogs<application_1528138529205_0001>"
    assert len(logs) == 2
    assert dict(logs) == raw
    out = logs.dumps()
    assert "line 6" in out
    assert logs.app_id in out
    assert all(k in out for k in raw)

    html = logs._repr_html_()
    assert logs.app_id in html
