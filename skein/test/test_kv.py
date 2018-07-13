from __future__ import print_function, division, absolute_import

import copy
from collections import MutableMapping, OrderedDict

import pytest

import skein
from skein.test.conftest import run_application


@pytest.fixture(scope="module")
def kv_test_app_persistent(client):
    with run_application(client) as app:
        try:
            yield app
        finally:
            app.shutdown()


@pytest.fixture
def kv_test_app(kv_test_app_persistent):
    try:
        yield kv_test_app_persistent
    finally:
        kv_test_app_persistent.kv.clear()


kv_test_data = {'bar': b'a',
                'barf': b'b',
                'bars': b'c',
                'bart': b'd',
                'foo': b'e',
                'food': b'f',
                'foodie': b'g'}


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


def test_key_value(kv_test_app):
    assert isinstance(kv_test_app.kv, MutableMapping)
    assert kv_test_app.kv is kv_test_app.kv

    assert dict(kv_test_app.kv) == {}

    kv_test_app.kv['foo'] = b'bar'
    assert kv_test_app.kv['foo'] == b'bar'

    assert dict(kv_test_app.kv) == {'foo': b'bar'}
    assert len(kv_test_app.kv) == 1
    assert 'foo' in kv_test_app.kv
    assert 'food' not in kv_test_app.kv

    del kv_test_app.kv['foo']
    assert len(kv_test_app.kv) == 0

    kv_test_app.kv.update({'a': b'one', 'b': b'two'})
    assert len(kv_test_app.kv) == 2
    kv_test_app.kv.clear()
    assert len(kv_test_app.kv) == 0

    with pytest.raises(KeyError):
        del kv_test_app.kv['foo']

    with pytest.raises(KeyError):
        kv_test_app.kv['fizz']

    with pytest.raises(TypeError):
        kv_test_app.kv[1] = 'foo'

    with pytest.raises(TypeError):
        kv_test_app.kv['foo'] = u'bar'

    with pytest.raises(TypeError):
        kv_test_app.kv['foo'] = 1


def test_key_value_count(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.count() == 7
    assert kv_test_app.kv.count(prefix='bar') == 4
    assert kv_test_app.kv.count(range_start='bars') == 5
    assert kv_test_app.kv.count(range_end='bars') == 2
    assert kv_test_app.kv.count(range_start='bars', range_end='food') == 3


def test_key_value_list_keys(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.list_keys() == list(sorted(kv_test_data))
    assert (kv_test_app.kv.list_keys(prefix='bar') ==
            ['bar', 'barf', 'bars', 'bart'])
    assert (kv_test_app.kv.list_keys(range_start='bars') ==
            ['bars', 'bart', 'foo', 'food', 'foodie'])
    assert kv_test_app.kv.list_keys(range_end='bars') == ['bar', 'barf']
    assert (kv_test_app.kv.list_keys(range_start='bars', range_end='food') ==
            ['bars', 'bart', 'foo'])


def test_key_value_contains(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert 'bar' in kv_test_app.kv
    assert 'missing' not in kv_test_app.kv
    assert kv_test_app.kv.contains('bar')
    assert not kv_test_app.kv.contains('missing')


def test_key_value_discard(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard('bar')
    assert 'bar' not in kv_test_app.kv
    assert not kv_test_app.kv.discard('missing')


def test_key_value_discard_prefix(kv_test_app):
    bars = sorted(k for k in kv_test_data if k.startswith('bar'))

    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard_prefix('bar') == len(bars)
    assert kv_test_app.kv.discard_prefix('bar') == 0
    assert kv_test_app.kv.discard_prefix('missing') == 0

    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard_prefix('bar', return_keys=True) == bars
    assert kv_test_app.kv.discard_prefix('bar', return_keys=True) == []
    assert kv_test_app.kv.discard_prefix('missing', return_keys=True) == []


def test_key_value_discard_range(kv_test_app):
    # start
    kv_test_app.kv.update(kv_test_data)
    sol = sorted(k for k in kv_test_data if 'bar' <= k)
    assert kv_test_app.kv.discard_range(start='bar') == len(sol)
    assert kv_test_app.kv.discard_range(start='bar') == 0
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard_range(start='bar', return_keys=True) == sol
    assert kv_test_app.kv.discard_range(start='bar', return_keys=True) == []

    # end
    kv_test_app.kv.update(kv_test_data)
    sol = sorted(k for k in kv_test_data if k < 'food')
    assert kv_test_app.kv.discard_range(end='food') == len(sol)
    assert kv_test_app.kv.discard_range(end='food') == 0
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard_range(end='food', return_keys=True) == sol
    assert kv_test_app.kv.discard_range(end='food', return_keys=True) == []

    # both
    kv_test_app.kv.update(kv_test_data)
    sol = sorted(k for k in kv_test_data if 'bar' <= k < 'food')
    assert kv_test_app.kv.discard_range(start='bar', end='food') == len(sol)
    assert kv_test_app.kv.discard_range(start='bar', end='food') == 0
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard_range(start='bar', end='food',
                                        return_keys=True) == sol
    assert kv_test_app.kv.discard_range(start='bar', end='food',
                                        return_keys=True) == []

    # neither
    kv_test_app.kv.update(kv_test_data)
    sol = sorted(kv_test_data)
    assert kv_test_app.kv.discard_range(return_keys=True) == sol
    assert kv_test_app.kv.discard_range() == 0

    # empty
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.discard_range(start='zzzz') == 0
    assert kv_test_app.kv.discard_range(start='z', end='a') == 0


def test_key_value_get(kv_test_app):
    kv_test_app.kv.update(kv_test_data)

    assert kv_test_app.kv.get('bar') == b'a'
    assert kv_test_app.kv.get('bar', return_owner=True) == (b'a', None)

    assert kv_test_app.kv.get('missing') is None
    assert kv_test_app.kv.get('missing', b'default') == b'default'
    assert kv_test_app.kv.get('missing', return_owner=True) == (None, None)
    assert (kv_test_app.kv.get('missing', b'default', return_owner=True) ==
            (b'default', None))


def test_key_value_get_prefix(kv_test_app):
    empty = OrderedDict()
    kv_test_app.kv.update(kv_test_data)

    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if k.startswith('bar')))
    assert kv_test_app.kv.get_prefix('bar') == sol
    assert kv_test_app.kv.get_prefix('missing') == empty

    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if k.startswith('bar')))
    assert kv_test_app.kv.get_prefix('bar', return_owner=True) == sol
    assert kv_test_app.kv.get_prefix('missing', return_owner=True) == empty


def test_key_value_get_range(kv_test_app):
    kv_test_app.kv.update(kv_test_data)

    # start
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if 'bar' <= k))
    assert kv_test_app.kv.get_range(start='bar') == sol
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if 'bar' <= k))
    assert kv_test_app.kv.get_range(start='bar', return_owner=True) == sol

    # end
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if k < 'food'))
    assert kv_test_app.kv.get_range(end='food') == sol
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if k < 'food'))
    assert kv_test_app.kv.get_range(end='food', return_owner=True) == sol

    # both
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if 'bar' <= k < 'food'))
    assert kv_test_app.kv.get_range(start='bar', end='food') == sol
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if 'bar' <= k < 'food'))
    assert kv_test_app.kv.get_range(start='bar', end='food',
                                    return_owner=True) == sol

    # neither
    sol = OrderedDict(sorted(kv_test_data.items()))
    assert kv_test_app.kv.get_range() == sol

    # empty
    assert kv_test_app.kv.get_range(start='zzzz') == OrderedDict()
    assert kv_test_app.kv.get_range(start='z', end='a') == OrderedDict()


def test_key_value_pop(kv_test_app):
    kv_test_app.kv.update(kv_test_data)

    assert kv_test_app.kv.pop('bar') == b'a'
    assert 'bar' not in kv_test_app.kv

    assert kv_test_app.kv.pop('barf', return_owner=True) == (b'b', None)
    assert 'barf' not in kv_test_app.kv

    assert kv_test_app.kv.pop('missing') is None
    assert kv_test_app.kv.pop('missing', b'default') == b'default'
    assert kv_test_app.kv.pop('missing', return_owner=True) == (None, None)
    assert (kv_test_app.kv.pop('missing', b'default', return_owner=True) ==
            (b'default', None))


def test_key_value_pop_prefix(kv_test_app):
    empty = OrderedDict()

    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if k.startswith('bar')))
    assert kv_test_app.kv.pop_prefix('bar') == sol
    assert kv_test_app.kv.pop_prefix('bar') == empty
    assert kv_test_app.kv.pop_prefix('missing') == empty

    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if k.startswith('bar')))
    assert kv_test_app.kv.pop_prefix('bar', return_owner=True) == sol
    assert kv_test_app.kv.pop_prefix('bar', return_owner=True) == empty
    assert kv_test_app.kv.pop_prefix('missing', return_owner=True) == empty


def test_key_value_pop_range(kv_test_app):
    empty = OrderedDict()

    # start
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if 'bar' <= k))
    assert kv_test_app.kv.pop_range(start='bar') == sol
    assert kv_test_app.kv.pop_range(start='bar') == empty
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if 'bar' <= k))
    assert kv_test_app.kv.pop_range(start='bar', return_owner=True) == sol
    assert kv_test_app.kv.pop_range(start='bar', return_owner=True) == empty

    # end
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if k < 'food'))
    assert kv_test_app.kv.pop_range(end='food') == sol
    assert kv_test_app.kv.pop_range(end='food') == empty
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if k < 'food'))
    assert kv_test_app.kv.pop_range(end='food', return_owner=True) == sol
    assert kv_test_app.kv.pop_range(end='food', return_owner=True) == empty

    # both
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, v) for k, v in kv_test_data.items()
                             if 'bar' <= k < 'food'))
    assert kv_test_app.kv.pop_range(start='bar', end='food') == sol
    assert kv_test_app.kv.pop_range(start='bar', end='food') == empty
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted((k, (v, None)) for k, v in kv_test_data.items()
                             if 'bar' <= k < 'food'))
    assert kv_test_app.kv.pop_range(start='bar', end='food',
                                    return_owner=True) == sol
    assert kv_test_app.kv.pop_range(start='bar', end='food',
                                    return_owner=True) == empty

    # neither
    kv_test_app.kv.update(kv_test_data)
    sol = OrderedDict(sorted(kv_test_data.items()))
    assert kv_test_app.kv.pop_range() == sol
    assert kv_test_app.kv.pop_range() == empty

    # empty
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.pop_range(start='zzzz') == empty
    assert kv_test_app.kv.pop_range(start='z', end='a') == empty


def test_key_value_put(kv_test_app):
    # New, value only
    kv_test_app.kv.put('foo', value=b'a')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'a', None)

    # New, owner and value
    kv_test_app.kv.put('bar', value=b'a', owner='sleeper_0')
    assert kv_test_app.kv.get('bar', return_owner=True) == (b'a', 'sleeper_0')

    # Overwrite existing key
    kv_test_app.kv.put('foo', value=b'b')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'b', None)

    # Set owner only, value remains unchanged
    kv_test_app.kv.put('foo', owner='sleeper_0')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'b', 'sleeper_0')

    # Set value only, owner remains unchanged
    kv_test_app.kv.put('foo', value=b'c')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'c', 'sleeper_0')

    # Clear owner, value remains unchanged
    kv_test_app.kv.put('foo', owner=None)
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'c', None)

    # Set both value and owner
    kv_test_app.kv.put('foo', value=b'd', owner='sleeper_0')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'd', 'sleeper_0')

    # Set value, clear owner
    kv_test_app.kv.put('foo', value=b'e', owner=None)
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'e', None)

    # Can't create new key without setting value as well
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.put('missing', owner='sleeper_0')
    assert str(exc.value) == "ignore_value=True & key isn't already set"


def test_key_value_swap(kv_test_app):
    # New, return value only
    assert kv_test_app.kv.swap('bar', value=b'a') is None

    # Existing, return value only
    assert kv_test_app.kv.swap('bar', value=b'b') == b'a'

    # Return owner for remaining tests
    ro = {'return_owner': True}

    # New, value only
    assert kv_test_app.kv.swap('foo', value=b'a', **ro) == (None, None)
    assert kv_test_app.kv.get('foo', **ro) == (b'a', None)

    # Overwrite existing key
    assert kv_test_app.kv.swap('foo', value=b'b', **ro) == (b'a', None)
    assert kv_test_app.kv.get('foo', **ro) == (b'b', None)

    # Set owner only, value remains unchanged
    assert kv_test_app.kv.swap('foo', owner='sleeper_0', **ro) == (b'b', None)
    assert kv_test_app.kv.get('foo', **ro) == (b'b', 'sleeper_0')

    # Set value only, owner remains unchanged
    assert kv_test_app.kv.swap('foo', value=b'c', **ro) == (b'b', 'sleeper_0')
    assert kv_test_app.kv.get('foo', **ro) == (b'c', 'sleeper_0')

    # Clear owner, value remains unchanged
    assert kv_test_app.kv.swap('foo', owner=None, **ro) == (b'c', 'sleeper_0')
    assert kv_test_app.kv.get('foo', **ro) == (b'c', None)

    # Set both value and owner
    assert (kv_test_app.kv.swap('foo', value=b'd', owner='sleeper_0', **ro) ==
            (b'c', None))
    assert kv_test_app.kv.get('foo', **ro) == (b'd', 'sleeper_0')

    # Set value, clear owner
    assert (kv_test_app.kv.swap('foo', value=b'e', owner=None, **ro) ==
            (b'd', 'sleeper_0'))
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'e', None)

    # Can't create new key without setting value as well
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.swap('missing', owner='sleeper_0')
    assert str(exc.value) == "ignore_value=True & key isn't already set"
