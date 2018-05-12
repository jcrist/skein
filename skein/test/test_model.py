from __future__ import print_function, division, absolute_import

import copy
import os

import pytest

from skein.model import Job, Service, Resources, File


def indent(s, n):
    pad = ' ' * n
    return '\n'.join(pad + l for l in s.splitlines())


service_spec = """\
resources:
    vcores: 1
    memory: 1024
files:
    - file: /path/to/testfile
    - archive: /path/to/testarchive.zip
    - source: /path/to/other
      dest: otherpath
      type: FILE
env:
    key1: val1
    key2: val2
commands:
    - command 1
    - command 2
"""

job_spec = """\
name: test
queue: default

services:
    service_1:
%s""" % indent(service_spec, 8)


def check_basic_methods(obj, obj2):
    # equality
    assert obj == copy.deepcopy(obj)
    assert obj != obj2
    assert obj != 'incorrect_type'

    # smoketest repr
    repr(obj)

    # conversions
    cls = type(obj)
    for skip in [True, False]:
        for method in ['json', 'yaml', 'dict']:
            msg = getattr(obj, 'to_' + method)(skip_nulls=skip)
            obj2 = getattr(cls, 'from_' + method)(msg)
            assert obj == obj2

    proto = obj.to_protobuf()
    obj2 = cls.from_protobuf(proto)
    assert obj == obj2


def test_resources():
    r = Resources(memory=1024, vcores=1)
    r2 = Resources(memory=1024, vcores=2)
    check_basic_methods(r, r2)


def test_resources_invariants():
    with pytest.raises(TypeError):
        Resources()

    with pytest.raises(TypeError):
        Resources(None, None)

    with pytest.raises(TypeError):
        Resources(memory='foo', vcores='bar')

    with pytest.raises(ValueError):
        Resources(memory=-1, vcores=-1)


def test_file():
    fil = File(source='/test/path')
    fil2 = File(source='/test/path', size=1024)
    check_basic_methods(fil, fil2)


def test_file_invariants():
    with pytest.raises(TypeError):
        File()

    with pytest.raises(TypeError):
        File(1)

    with pytest.raises(ValueError):
        File('/foo/bar.zip', type='invalid')

    with pytest.raises(ValueError):
        File('/foo/bar.zip', visibility='invalid')

    with pytest.raises(ValueError):
        File('foo/bar.zip')


def test_service():
    r = Resources(memory=1024, vcores=1)
    c = ['commands']
    s1 = Service(resources=r, commands=c,
                 files={'file': File(source='/test/path')})
    s2 = Service(resources=r, commands=c,
                 files={'file': File(source='/test/path', size=1024)})
    check_basic_methods(s1, s2)


def test_service_invariants():
    r = Resources(memory=1024, vcores=1)
    c = ['command']

    with pytest.raises(TypeError):
        Service()

    # No commands provided
    with pytest.raises(ValueError):
        Service(commands=[], resources=r)

    with pytest.raises(ValueError):
        Service(commands=c, resources=r, instances=-1)

    with pytest.raises(TypeError):
        Service(commands=c, resources=r, env={'a': 1})

    with pytest.raises(TypeError):
        Service(commands=c, resources=r, depends=[1])

    # Mutable defaults properly set
    s = Service(commands=c, resources=r)
    assert isinstance(s.env, dict)
    assert isinstance(s.files, dict)
    assert isinstance(s.depends, list)


def test_job():
    r = Resources(memory=1024, vcores=1)
    c = ['commands']
    s1 = Service(resources=r, commands=c,
                 files={'file': File(source='/test/path')})
    s2 = Service(resources=r, commands=c,
                 files={'file': File(source='/test/path', size=1024)})
    j1 = Job(services={'service': s1})
    j2 = Job(services={'service': s2})
    check_basic_methods(j1, j2)


def test_job_invariants():
    s = Service(commands=['command'],
                resources=Resources(memory=1024, vcores=1))

    # No services
    with pytest.raises(ValueError):
        Job(name='dask', queue='default', services={})

    with pytest.raises(TypeError):
        Job(name=1, services={'service': s})

    with pytest.raises(TypeError):
        Job(queue=1, services={'service': s})


def test_service_from_yaml():
    # Check that syntactic sugar for files, etc... is properly parsed
    s = Service.from_yaml(service_spec)
    assert isinstance(s, Service)

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
    assert s.commands == ['command 1', 'command 2']
    assert s.depends == []


def test_service_roundtrip():
    s = Service.from_yaml(service_spec)
    s2 = Service.from_yaml(s.to_yaml())
    assert s == s2


def test_job_from_yaml():
    j = Job.from_yaml(job_spec)
    assert isinstance(j, Job)

    assert j.name == 'test'
    assert j.queue == 'default'
    assert isinstance(j.services, dict)
    assert isinstance(j.services['service_1'], Service)


def test_job_roundtrip():
    j = Job.from_yaml(job_spec)
    j2 = Job.from_yaml(j.to_yaml())
    assert j == j2


def test_to_file_from_file(tmpdir):
    job = Job.from_yaml(job_spec)

    for name, format in [('test.yaml', 'infer'),
                         ('test.json', 'infer'),
                         ('test2.yaml', 'json')]:
        path = os.path.join(str(tmpdir), name)
        assert not os.path.exists(path)
        job.to_file(path, format=format)
        assert os.path.exists(path)
        job2 = Job.from_file(path, format=format)
        assert job == job2

    for name, format in [('bad.yaml', 'invalid'), ('bad.invalid', 'infer')]:
        path = os.path.join(str(tmpdir), name)
        with pytest.raises(ValueError):
            job.to_file(path, format=format)
        assert not os.path.exists(path)
