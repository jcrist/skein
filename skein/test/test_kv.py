from __future__ import print_function, division, absolute_import

import copy
from collections import MutableMapping

import pytest

import skein
from skein.test.conftest import run_application


def test_clean_tab_completion_kv_namespace():
    public = [n for n in dir(skein.kv) if not n.startswith('_')]
    assert set(public) == set(skein.kv.__all__)


@pytest.mark.parametrize('cls', [skein.kv.count, skein.kv.list_keys])
def test_count_and_list_keys_types(cls):
    x1 = cls()
    assert repr(x1) == ("%s(range_start=None, range_end=None)" % cls.__name__)
    x2 = cls(prefix='foo')
    assert repr(x2) == ("%s(prefix='foo')" % cls.__name__)
    x3 = cls(range_start='foo')
    assert repr(x3) == ("%s(range_start='foo', range_end=None)" % cls.__name__)
    for x in [x1, x2, x3]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(ValueError):
        cls(prefix='foo', range_start='bar')

    for k in ['prefix', 'range_start', 'range_end']:
        with pytest.raises(TypeError):
            cls(**{k: b'bad type'})


@pytest.mark.parametrize('cls', [skein.kv.contains, skein.kv.discard])
def test_contains_and_discard_types(cls):
    x = cls('foo')
    assert repr(x) == ("%s('foo')" % cls.__name__)
    assert x == copy.deepcopy(x)
    assert x != cls('bar')

    with pytest.raises(AttributeError):
        x.foobar = 1

    with pytest.raises(TypeError):
        cls(b'foo')


@pytest.mark.parametrize('cls', [skein.kv.get, skein.kv.pop])
def test_get_and_pop_types(cls):
    name = cls.__name__
    x1 = cls('key')
    assert repr(x1) == ("%s('key', default=None, return_owner=False)" % name)
    x2 = cls('key', default=b'bar', return_owner=True)
    assert repr(x2) == ("%s('key', default=b'bar', return_owner=True)" % name)
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(TypeError):
        cls('foo', default='bar')

    with pytest.raises(TypeError):
        cls('foo', return_owner=None)

    with pytest.raises(TypeError):
        cls(b'foo')


@pytest.mark.parametrize('cls,kw', [(skein.kv.get_prefix, 'return_owner'),
                                    (skein.kv.pop_prefix, 'return_owner'),
                                    (skein.kv.discard_prefix, 'return_keys')])
def test_prefix_types(cls, kw):
    name = cls.__name__
    x1 = cls('key')
    assert repr(x1) == ("%s('key', %s=False)" % (name, kw))
    x2 = cls('key', **{kw: True})
    assert repr(x2) == ("%s('key', %s=True)" % (name, kw))
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(TypeError):
        cls(b'foo')

    with pytest.raises(TypeError):
        cls('foo', **{kw: None})


@pytest.mark.parametrize('cls,kw', [(skein.kv.get_range, 'return_owner'),
                                    (skein.kv.pop_range, 'return_owner'),
                                    (skein.kv.discard_range, 'return_keys')])
def test_range_types(cls, kw):
    name = cls.__name__
    x1 = cls()
    assert repr(x1) == ("%s(start=None, end=None, %s=False)" % (name, kw))
    x2 = cls(start='key', end='bar', **{kw: True})
    assert repr(x2) == ("%s(start='key', end='bar', %s=True)" % (name, kw))
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    with pytest.raises(AttributeError):
        x1.foobar = 1

    for k in ['start', 'end', kw]:
        with pytest.raises(TypeError):
            cls(**{k: b'bad type'})


@pytest.mark.parametrize('cls,has_return_owner',
                         [(skein.kv.put, False), (skein.kv.swap, True)])
def test_put_and_swap_types(cls, has_return_owner):
    name = cls.__name__
    if has_return_owner:
        extra = ', return_owner=False'
    else:
        extra = ''
    x1 = cls('foo', value=b'bar')
    assert repr(x1) == ("%s('foo', value=b'bar', owner=no_change%s)"
                        % (name, extra))
    x2 = cls('foo', owner='container_1')
    assert repr(x2) == ("%s('foo', value=no_change, owner='container_1'%s)"
                        % (name, extra))
    x3 = cls('foo', owner=None)
    assert repr(x3) == ("%s('foo', value=no_change, owner=None%s)"
                        % (name, extra))
    for x in [x1, x2, x3]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(ValueError):
        cls('foo', owner='invalid')

    with pytest.raises(TypeError):
        cls(b'wrong_type', value=b'bar')

    with pytest.raises(TypeError):
        cls('foo', owner=b'wrong_type')

    with pytest.raises(ValueError):
        cls('foo')


def test_key_value(client):
    with run_application(client) as app:
        assert isinstance(app.kv, MutableMapping)
        assert app.kv is app.kv

        assert dict(app.kv) == {}

        app.kv['foo'] = b'bar'
        assert app.kv['foo'] == b'bar'

        assert dict(app.kv) == {'foo': b'bar'}
        assert len(app.kv) == 1
        assert 'foo' in app.kv
        assert 'food' not in app.kv

        del app.kv['foo']
        assert len(app.kv) == 0

        app.kv.update({'a': b'one', 'b': b'two'})
        assert len(app.kv) == 2
        app.kv.clear()
        assert len(app.kv) == 0

        with pytest.raises(KeyError):
            del app.kv['foo']

        with pytest.raises(KeyError):
            app.kv['fizz']

        with pytest.raises(TypeError):
            app.kv[1] = 'foo'

        with pytest.raises(TypeError):
            app.kv['foo'] = u'bar'

        with pytest.raises(TypeError):
            app.kv['foo'] = 1

        app.shutdown()
