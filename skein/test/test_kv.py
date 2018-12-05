from __future__ import print_function, division, absolute_import

import copy
import operator
import sys
import threading
import time
import weakref
from collections import OrderedDict

import pytest

from skein import kv
from skein.compatibility import MutableMapping
from skein.exceptions import ConnectionError
from skein.test.conftest import run_application


@pytest.fixture(scope="module")
def kv_test_app_persistent(client):
    with run_application(client) as app:
        # ensure one container id exists, but already completed
        app.scale('sleeper', 2)
        app.kill_container('sleeper_0')
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
        kv_test_app_persistent.scale('sleeper', 1)


kv_test_data = {'bar': b'a',
                'barf': b'b',
                'bars': b'c',
                'bart': b'd',
                'foo': b'e',
                'food': b'f',
                'foodie': b'g'}


def test_clean_tab_completion_kv_namespace():
    public = [n for n in dir(kv) if not n.startswith('_')]
    assert set(public) == set(kv.__all__)


@pytest.mark.parametrize('field', ['value', 'owner'])
def test_field_accessor_types(field):
    cls = getattr(kv, field)
    rec = cls('foo')

    assert repr(rec) == "%s('foo')" % field

    assert not kv.is_condition(rec)

    rec2 = copy.deepcopy(rec)
    assert rec2.key == 'foo'
    assert isinstance(rec2, cls)

    with pytest.raises(TypeError):
        cls(1)


def test_comparison_type():
    # test using `comparison` directly
    v = kv.comparison('foo', 'value', '==', b'bar')
    assert v == (kv.value('foo') == b'bar')

    o = kv.comparison('foo', 'owner', '==', 'bar_1')
    assert o == (kv.owner('foo') == 'bar_1')

    with pytest.raises(TypeError):
        kv.comparison(1, 'value', '==', b'bar')

    with pytest.raises(ValueError):
        kv.comparison('foo', 'unknown', '==', b'bar')

    with pytest.raises(ValueError):
        kv.comparison('foo', 'value', 'unknown', b'bar')

    with pytest.raises(TypeError):
        kv.comparison('foo', 'owner', '<=', None)


@pytest.mark.parametrize('field, rhs1, rhs2, supports_none',
                         [('value', b'rhs1', b'rhs2', False),
                          ('owner', 'c_1', 'c_2', True)])
@pytest.mark.parametrize('op, symbol', [(operator.eq, '=='), (operator.ne, '!='),
                                        (operator.lt, '<'), (operator.le, '<='),
                                        (operator.gt, '>'), (operator.ge, '>=')])
def test_comparison_builder_types(field, rhs1, rhs2, supports_none, op, symbol):
    cls = getattr(kv, field)
    rec = cls('foo')

    x1 = op(rec, rhs1)
    assert kv.is_condition(x1)
    assert isinstance(x1, kv.comparison)
    assert repr(x1) == "%s('foo') %s %r" % (field, symbol, rhs1)

    x2 = op(rec, rhs2)
    assert kv.is_condition(x2)
    assert isinstance(x2, kv.comparison)
    assert repr(x2) == "%s('foo') %s %r" % (field, symbol, rhs2)

    assert x1 == copy.deepcopy(x1)
    assert x1 != x2

    if supports_none and symbol in ('==', '!='):
        op(rec, None)
    else:
        with pytest.raises(TypeError):
            op(rec, None)

    with pytest.raises(TypeError):
        op(rec, 123)

    for attr, val in [('key', 'foo'), ('field', field),
                      ('operator', symbol), ('rhs', rhs1)]:
        assert getattr(x1, attr) == val

        with pytest.raises(AttributeError):
            setattr(x1, attr, val)  # immutable


@pytest.mark.parametrize('cls', [kv.exists,
                                 kv.missing])
def test_exists_and_missing_types(cls):
    x = cls('foo')
    assert repr(x) == ("%s('foo')" % cls.__name__)
    assert x == copy.deepcopy(x)
    assert x != cls('bar')

    assert kv.is_condition(x)
    assert kv.is_operation(x)

    with pytest.raises(AttributeError):
        x.foobar = 1

    with pytest.raises(TypeError):
        cls(1)


@pytest.mark.parametrize('cls', [kv.count, kv.list_keys])
def test_count_and_list_keys_types(cls):
    x1 = cls()
    assert repr(x1) == ("%s(start=None, end=None)" % cls.__name__)
    x2 = cls(prefix='foo')
    assert repr(x2) == ("%s(prefix='foo')" % cls.__name__)
    x3 = cls(start='foo')
    assert repr(x3) == ("%s(start='foo', end=None)" % cls.__name__)
    for x in [x1, x2, x3]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(ValueError):
        cls(prefix='foo', start='bar')

    for k in ['prefix', 'start', 'end']:
        with pytest.raises(TypeError):
            cls(**{k: 1})


def test_discard_type():
    x = kv.discard('foo')
    assert repr(x) == "discard('foo')"
    assert x == copy.deepcopy(x)
    assert x != kv.discard('bar')

    assert kv.is_operation(x)

    with pytest.raises(AttributeError):
        x.foobar = 1

    with pytest.raises(TypeError):
        kv.discard(1)


@pytest.mark.parametrize('cls', [kv.get, kv.pop])
def test_get_and_pop_types(cls):
    name = cls.__name__
    x1 = cls('key')
    assert repr(x1) == ("%s('key', default=None, return_owner=False)" % name)
    x2 = cls('key', default=b'bar', return_owner=True)
    assert repr(x2) == ("%s('key', default=%s, return_owner=True)" % (name, repr(b'bar')))
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(TypeError):
        cls('foo', default=1)

    with pytest.raises(TypeError):
        cls('foo', return_owner=None)

    with pytest.raises(TypeError):
        cls(1)


@pytest.mark.parametrize('cls,kw', [(kv.get_prefix, 'return_owner'),
                                    (kv.pop_prefix, 'return_owner'),
                                    (kv.discard_prefix, 'return_keys')])
def test_prefix_types(cls, kw):
    name = cls.__name__
    x1 = cls('key')
    assert repr(x1) == ("%s('key', %s=False)" % (name, kw))
    x2 = cls('key', **{kw: True})
    assert repr(x2) == ("%s('key', %s=True)" % (name, kw))
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(TypeError):
        cls(1)

    with pytest.raises(TypeError):
        cls('foo', **{kw: None})


@pytest.mark.parametrize('cls,kw', [(kv.get_range, 'return_owner'),
                                    (kv.pop_range, 'return_owner'),
                                    (kv.discard_range, 'return_keys')])
def test_range_types(cls, kw):
    name = cls.__name__
    x1 = cls()
    assert repr(x1) == ("%s(start=None, end=None, %s=False)" % (name, kw))
    x2 = cls(start='key', end='bar', **{kw: True})
    assert repr(x2) == ("%s(start='key', end='bar', %s=True)" % (name, kw))
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    for k in ['start', 'end', kw]:
        with pytest.raises(TypeError):
            cls(**{k: 1})


@pytest.mark.parametrize('cls,has_return_owner',
                         [(kv.put, False), (kv.swap, True)])
def test_put_and_swap_types(cls, has_return_owner):
    name = cls.__name__
    if has_return_owner:
        extra = ', return_owner=False'
    else:
        extra = ''
    x1 = cls('foo', value=b'bar')
    assert repr(x1) == ("%s('foo', value=%s, owner=no_change%s)"
                        % (name, repr(b'bar'), extra))
    x2 = cls('foo', owner='container_1')
    assert repr(x2) == ("%s('foo', value=no_change, owner='container_1'%s)"
                        % (name, extra))
    x3 = cls('foo', owner=None)
    assert repr(x3) == ("%s('foo', value=no_change, owner=None%s)"
                        % (name, extra))
    for x in [x1, x2, x3]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(ValueError):
        cls('foo', owner='invalid')

    with pytest.raises(TypeError):
        cls(1, value=b'bar')

    with pytest.raises(TypeError):
        cls('foo', owner=1)

    with pytest.raises(ValueError):
        cls('foo')


class MapLike(object):
    def __init__(self, data):
        self.data = data

    def keys(self):
        return self.data.keys()

    def __getitem__(self, key):
        return self.data[key]


def test_key_value_mutablemapping(kv_test_app):
    assert isinstance(kv_test_app.kv, MutableMapping)

    assert kv_test_app.kv is kv_test_app.kv

    # Iterators work when empty
    assert len(kv_test_app.kv) == 0
    assert list(kv_test_app.kv) == []
    assert dict(kv_test_app.kv) == {}

    # __setitem__
    kv_test_app.kv['k1'] = b'v1'
    kv_test_app.kv['k2'] = b'v2'

    with pytest.raises(TypeError):
        kv_test_app.kv['k3'] = u'v3'

    # __contains__
    assert 'k1' in kv_test_app.kv
    assert 'missing' not in kv_test_app.kv

    # __len__
    assert len(kv_test_app.kv) == 2

    # __iter__ (note this is sorted)
    assert list(kv_test_app.kv) == ['k1', 'k2']

    # __getitem__
    assert kv_test_app.kv['k1'] == b'v1'

    with pytest.raises(KeyError):
        kv_test_app.kv['missing']

    with pytest.raises(TypeError):
        kv_test_app.kv[1]

    # __delitem__
    kv_test_app.kv['key'] = b'temp'
    assert 'key' in kv_test_app.kv
    del kv_test_app.kv['key']
    assert 'key' not in kv_test_app.kv

    with pytest.raises(KeyError):
        del kv_test_app.kv['missing']

    with pytest.raises(TypeError):
        del kv_test_app.kv[1]

    # setdefault
    assert kv_test_app.kv.setdefault('new_key', b'val1') == b'val1'
    assert kv_test_app.kv.setdefault('new_key', b'val2') == b'val1'

    # clear
    assert len(kv_test_app.kv) > 0
    kv_test_app.kv.clear()
    assert len(kv_test_app.kv) == 0

    # update
    kv_test_app.kv.update({'a': b'0', 'b': b'1'})            # mapping
    kv_test_app.kv.update([('c', b'2'), ('d', b'3')])        # iterable
    kv_test_app.kv.update(MapLike({'e': b'4', 'f': b'5'}))   # class with keys
    kv_test_app.kv.update({'g': b'bad', 'h': b'7'}, g=b'6')  # kwarg overrides
    kv_test_app.kv.update(i=b'8', j=b'9')                    # kwargs only
    sol = {k: str(i).encode() for i, k in enumerate('abcdefghij')}
    assert kv_test_app.kv.get_range() == sol

    with pytest.raises(TypeError):
        kv_test_app.kv.update({'a': 1}, {'b': 2})


def test_key_value_count(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.count() == 7
    assert kv_test_app.kv.count(prefix='bar') == 4
    assert kv_test_app.kv.count(start='bars') == 5
    assert kv_test_app.kv.count(end='bars') == 2
    assert kv_test_app.kv.count(start='bars', end='food') == 3


def test_key_value_list_keys(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert kv_test_app.kv.list_keys() == list(sorted(kv_test_data))
    assert (kv_test_app.kv.list_keys(prefix='bar') ==
            ['bar', 'barf', 'bars', 'bart'])
    assert (kv_test_app.kv.list_keys(start='bars') ==
            ['bars', 'bart', 'foo', 'food', 'foodie'])
    assert kv_test_app.kv.list_keys(end='bars') == ['bar', 'barf']
    assert (kv_test_app.kv.list_keys(start='bars', end='food') ==
            ['bars', 'bart', 'foo'])


def test_key_value_exists_and_missing(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert 'bar' in kv_test_app.kv
    assert 'missing' not in kv_test_app.kv
    assert kv_test_app.kv.exists('bar')
    assert not kv_test_app.kv.missing('bar')
    assert not kv_test_app.kv.exists('missing')
    assert kv_test_app.kv.missing('missing')


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
    cid = kv_test_app.get_containers()[0].id

    # New, value only
    kv_test_app.kv.put('foo', value=b'a')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'a', None)

    # New, owner and value
    kv_test_app.kv.put('bar', value=b'a', owner=cid)
    assert kv_test_app.kv.get('bar', return_owner=True) == (b'a', cid)

    # Overwrite existing key
    kv_test_app.kv.put('foo', value=b'b')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'b', None)

    # Set owner only, value remains unchanged
    kv_test_app.kv.put('foo', owner=cid)
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'b', cid)

    # Set value only, owner remains unchanged
    kv_test_app.kv.put('foo', value=b'c')
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'c', cid)

    # Clear owner, value remains unchanged
    kv_test_app.kv.put('foo', owner=None)
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'c', None)

    # Set both value and owner
    kv_test_app.kv.put('foo', value=b'd', owner=cid)
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'd', cid)

    # Set value, clear owner
    kv_test_app.kv.put('foo', value=b'e', owner=None)
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'e', None)

    # Can't create new key without setting value as well
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.put('missing', owner=cid)
    assert str(exc.value) == "ignore_value=True & key isn't already set"

    # Owner service doesn't exist
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.put('bar', owner='unknown_0')
    assert str(exc.value) == "Unknown service 'unknown'"

    # Owner instance doesn't exist
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.put('bar', owner='sleeper_100')
    assert str(exc.value) == "Service 'sleeper' has no container instance 100"

    # Owner instance already completed
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.put('bar', owner='sleeper_0')
    assert str(exc.value) == "Container 'sleeper_0' has already completed"


def test_key_value_swap(kv_test_app):
    cid = kv_test_app.get_containers()[0].id

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
    assert kv_test_app.kv.swap('foo', owner=cid, **ro) == (b'b', None)
    assert kv_test_app.kv.get('foo', **ro) == (b'b', cid)

    # Set value only, owner remains unchanged
    assert kv_test_app.kv.swap('foo', value=b'c', **ro) == (b'b', cid)
    assert kv_test_app.kv.get('foo', **ro) == (b'c', cid)

    # Clear owner, value remains unchanged
    assert kv_test_app.kv.swap('foo', owner=None, **ro) == (b'c', cid)
    assert kv_test_app.kv.get('foo', **ro) == (b'c', None)

    # Set both value and owner
    assert (kv_test_app.kv.swap('foo', value=b'd', owner=cid, **ro) ==
            (b'c', None))
    assert kv_test_app.kv.get('foo', **ro) == (b'd', cid)

    # Set value, clear owner
    assert (kv_test_app.kv.swap('foo', value=b'e', owner=None, **ro) ==
            (b'd', cid))
    assert kv_test_app.kv.get('foo', return_owner=True) == (b'e', None)

    # Can't create new key without setting value as well
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.swap('missing', owner=cid)
    assert str(exc.value) == "ignore_value=True & key isn't already set"

    # Owner service doesn't exist
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.swap('bar', owner='unknown_0')
    assert str(exc.value) == "Unknown service 'unknown'"

    # Owner instance doesn't exist
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.swap('bar', owner='sleeper_100')
    assert str(exc.value) == "Service 'sleeper' has no container instance 100"

    # Owner instance already completed
    with pytest.raises(ValueError) as exc:
        kv_test_app.kv.swap('bar', owner='sleeper_0')
    assert str(exc.value) == "Container 'sleeper_0' has already completed"


def test_key_value_ownership(kv_test_app):
    kv_test_app.scale('sleeper', 2)
    c1, c2 = (c.id for c in kv_test_app.get_containers())

    # Create some owned keys
    kv_test_app.kv.put('c1_1', value=b'a', owner=c1)
    kv_test_app.kv.put('c1_2', value=b'b', owner=c1)
    kv_test_app.kv.put('c1_3', value=b'c', owner=c1)
    # Change the owners for two of them
    kv_test_app.kv.put('c1_2', owner=c2)
    kv_test_app.kv.put('c1_3', owner=None)

    state = kv_test_app.kv.get_range(return_owner=True)
    assert state['c1_1'] == (b'a', c1)
    assert state['c1_2'] == (b'b', c2)
    assert state['c1_3'] == (b'c', None)

    kv_test_app.kill_container(c1)

    # c1_1 should be deleted, the others should remain
    assert not kv_test_app.kv.exists('c1_1')
    assert kv_test_app.kv.exists('c1_2')
    assert kv_test_app.kv.exists('c1_3')


def test_key_value_keys_fully_removed_from_previous_owners(kv_test_app):
    # Tests changing an owner, removing an owner, and deletion all fully remove
    # a key from being owned by a container.
    kv_test_app.scale('sleeper', 2)
    c1, c2 = (c.id for c in kv_test_app.get_containers())

    # Create some owned keys
    kv_test_app.kv.put('c1_1', value=b'a', owner=c1)
    kv_test_app.kv.put('c1_2', value=b'b', owner=c1)
    kv_test_app.kv.put('c1_3', value=b'c', owner=c1)

    # Change the owners for two of them
    kv_test_app.kv.put('c1_2', owner=c2)
    kv_test_app.kv.put('c1_3', owner=None)

    # Delete all keys
    kv_test_app.kv.clear()

    # Recreate the keys with no owners
    kv_test_app.kv.update({'c1_1': b'a', 'c1_2': b'b', 'c1_3': b'c'})

    # Kill the original owning container
    kv_test_app.kill_container(c1)

    # Check that the keys still exist
    for k in ['c1_1', 'c1_2', 'c1_3']:
        assert k in kv_test_app.kv


def test_transaction_conditions(kv_test_app):
    cid = kv_test_app.get_containers()[0].id
    service, instance = cid.split('_')
    instance = int(instance)
    cid_larger = '%s_%d' % (service, instance + 1)
    cid_smaller = '%s_%d' % (service, instance - 1)

    val = b'bcd'
    larger = b'bce'
    smaller = b'bcc'

    kv_test_app.kv.put('owner', val, owner=cid)
    kv_test_app.kv.put('no_owner', val)

    def run(*conds):
        return kv_test_app.kv.transaction(conds).succeeded

    # -- value --
    # exists
    assert run(kv.exists('owner'))
    assert not run(kv.exists('missing'))
    assert run(kv.missing('missing'))
    assert not run(kv.missing('owner'))

    # ==
    assert run(kv.value('owner') == val)
    assert not run(kv.value('owner') == larger)
    assert not run(kv.value('missing') == val)
    # !=
    assert run(kv.value('owner') != larger)
    assert not run(kv.value('owner') != val)
    assert run(kv.value('missing') != val)
    # <
    assert run(kv.value('owner') < larger)
    assert not run(kv.value('owner') < val)
    assert not run(kv.value('owner') < smaller)
    assert not run(kv.value('missing') < val)
    # <=
    assert run(kv.value('owner') <= larger)
    assert run(kv.value('owner') <= val)
    assert not run(kv.value('owner') <= smaller)
    assert not run(kv.value('missing') <= val)
    # >
    assert not run(kv.value('owner') > larger)
    assert not run(kv.value('owner') > val)
    assert run(kv.value('owner') > smaller)
    assert not run(kv.value('missing') > val)
    # >=
    assert not run(kv.value('owner') >= larger)
    assert run(kv.value('owner') >= val)
    assert run(kv.value('owner') >= smaller)
    assert not run(kv.value('missing') >= val)

    # -- owner --
    # Owner presence/absence
    assert run(kv.owner('owner') != None)  # noqa
    assert not run(kv.owner('owner') == None)  # noqa
    assert run(kv.owner('no_owner') == None)  # noqa
    assert not run(kv.owner('no_owner') != None)  # noqa
    # ==
    assert run(kv.owner('owner') == cid)
    assert not run(kv.owner('owner') == cid_larger)
    assert not run(kv.owner('no_owner') == cid)
    # !=
    assert run(kv.owner('owner') != cid_larger)
    assert not run(kv.owner('owner') != cid)
    assert run(kv.owner('no_owner') != cid)
    # <
    assert run(kv.owner('owner') < cid_larger)
    assert not run(kv.owner('owner') < cid)
    assert not run(kv.owner('owner') < cid_smaller)
    assert not run(kv.owner('no_owner') < cid)
    # <=
    assert run(kv.owner('owner') <= cid_larger)
    assert run(kv.owner('owner') <= cid)
    assert not run(kv.owner('owner') <= cid_smaller)
    assert not run(kv.owner('no_owner') <= cid)
    # >
    assert not run(kv.owner('owner') > cid_larger)
    assert not run(kv.owner('owner') > cid)
    assert run(kv.owner('owner') > cid_smaller)
    assert not run(kv.owner('no_owner') > cid)
    # >=
    assert not run(kv.owner('owner') >= cid_larger)
    assert run(kv.owner('owner') >= cid)
    assert run(kv.owner('owner') >= cid_smaller)
    assert not run(kv.owner('no_owner') >= cid)

    # No conditions
    assert run()

    # Multiple conditions
    true1 = kv.exists('owner')
    true2 = kv.exists('no_owner')
    false = kv.exists('missing')
    assert run(true1, true2)
    assert not run(false, true1)
    assert not run(true1, false, true2)


@pytest.mark.parametrize('op, msg', [
    (kv.put('missing', owner='sleeper_0'), "ignore_value=True & key isn't already set"),
    (kv.put('a', owner='unknown_0'), "Unknown service 'unknown'"),
    (kv.put('a', owner='sleeper_99'), "Service 'sleeper' has no container instance 99"),
    (kv.put('a', owner='sleeper_0'), "Container 'sleeper_0' has already completed")])
def test_transaction_put_key_errors(kv_test_app, op, msg):
    cid = kv_test_app.get_containers()[0].id
    kv_test_app.kv.put('a', b'a', owner=cid)

    for succeeded in [True, False]:
        if succeeded:
            kwargs = {'conditions': [kv.exists('a')],
                      'on_success': [kv.put('b', b'b'), op],
                      'on_failure': [kv.put('c', b'c')]}
        else:
            kwargs = {'conditions': [kv.missing('a')],
                      'on_success': [kv.put('c', b'c')],
                      'on_failure': [kv.put('b', b'b'), op]}

        with pytest.raises(ValueError) as exc:
            kv_test_app.kv.transaction(**kwargs)

        assert str(exc.value) == msg
        assert not kv_test_app.kv.exists('b')
        assert not kv_test_app.kv.exists('c')


def test_transaction(kv_test_app):
    cid = kv_test_app.get_containers()[0].id

    kv_test_app.kv.put('a', b'a', owner=cid)
    kv_test_app.kv.put('aa', b'aa')
    kv_test_app.kv.put('aaa', b'aaa')
    kv_test_app.kv.put('b', b'b')

    true1 = kv.exists('a')
    true2 = kv.exists('b')
    false = kv.missing('a')

    # Test building results in success and failure
    for succeeded in [True, False]:
        if succeeded:
            cond, do, dont = [true1, true2], 'on_success', 'on_failure'
        else:
            cond, do, dont = [true1, false], 'on_failure', 'on_success'

        kwargs = {'conditions': cond,
                  do: [kv.get_prefix('a'), kv.get('missing')],
                  dont: [kv.put('dont_put_me', b'bad')]}

        res = kv_test_app.kv.transaction(**kwargs)

        assert res.succeeded == succeeded
        assert len(res.results) == 2
        assert res.results[0] == kv_test_app.kv.get_prefix('a')
        assert res.results[1] == kv_test_app.kv.get('missing')
        assert not kv_test_app.kv.exists('dont_put_me')

        # Test empty result branch
        kwargs = {'conditions': cond, dont: [kv.put('dont_put_me', b'bad')]}
        res = kv_test_app.kv.transaction(**kwargs)
        assert res == (succeeded, [])
        assert not kv_test_app.kv.exists('dont_put_me')

    # Test one operation of each type
    a_prefixes = kv_test_app.kv.get_prefix('a')

    res = kv_test_app.kv.transaction(
        conditions=[true1, true2],
        on_success=[kv.get_prefix('a'),
                    kv.discard_prefix('a', return_keys=True),
                    kv.put('a', b'later')])

    assert res.succeeded
    assert len(res.results) == 3
    assert res.results[0] == a_prefixes
    assert res.results[1] == list(a_prefixes)
    assert res.results[2] is None

    # Test no operations
    assert kv_test_app.kv.transaction() == (True, [])

    # invalid argument types
    with pytest.raises(TypeError):
        kv_test_app.kv.transaction(conditions=[kv.get('foo')])

    with pytest.raises(TypeError):
        kv_test_app.kv.transaction(on_success=[kv.value('foo') == b'bar'])

    with pytest.raises(TypeError):
        kv_test_app.kv.transaction(on_failure=[kv.value('foo') == b'bar'])


def test_event_filter_type():
    x = kv.EventFilter()
    assert x.start is None
    assert x.end is None
    assert x.event_type == kv.EventType.ALL

    assert copy.copy(x) == x
    assert hash(x) == hash(x)

    # smoketest
    repr(x)

    x2 = kv.EventFilter(prefix='foo')
    assert x2.start == 'foo'
    assert x2.end == 'fop'
    assert kv.EventFilter(prefix='foo') == x2
    assert kv.EventFilter(prefix='bar') != x2

    x3 = kv.EventFilter(key='foo')
    assert x3.start == 'foo'
    assert x3.end == 'foo\x00'
    assert kv.EventFilter(key='foo') == x3
    assert kv.EventFilter(key='bar') != x3

    assert kv.EventFilter(event_type='delete').event_type == 'DELETE'

    for k in ['key', 'prefix', 'start', 'end', 'event_type']:
        with pytest.raises(TypeError):
            kv.EventFilter(**{k: 1})

    with pytest.raises(ValueError):
        kv.EventFilter(key='foo', prefix='bar')

    with pytest.raises(ValueError):
        kv.EventFilter(event_type='foo')


def test_event_queue(kv_test_app):
    q = kv_test_app.kv.events(prefix='foo')
    assert len(q.filters) == 1
    e_foo = next(iter(q.filters))

    # smoketest
    repr(q)

    # Already subscribed
    assert q.subscribe(prefix='foo') == e_foo
    assert len(q.filters) == 1

    # subscribe/unsubscribe with keywords
    e_bar = q.subscribe(key='bar')
    assert len(q.filters) == 2
    assert e_bar in q.filters
    q.unsubscribe(key='bar')
    assert len(q.filters) == 1
    assert e_bar not in q.filters

    # subscribe/unsubscribe with explicit filter
    e_baz = kv.EventFilter(key='baz')
    assert q.subscribe(e_baz) == e_baz
    assert e_baz in q.filters
    assert q.unsubscribe(e_baz) == e_baz
    assert e_baz not in q.filters

    with pytest.raises(ValueError):
        q.unsubscribe(key='missing')

    with pytest.raises(ValueError):
        q.unsubscribe(event_filter=e_foo, prefix='bar')

    with pytest.raises(TypeError):
        q.unsubscribe(event_filter='incorrect type')

    # Context manager
    with q:
        q.subscribe(prefix='more')
        assert len(q.filters) == 2
    assert len(q.filters) == 0

    # Test queue methods
    q.put(1)
    assert q.get() == 1

    # Queue is iterable
    q.put(1)
    q.put(2)
    iq = iter(q)
    next(iq) == 1
    next(iq) == 2

    # Test queue methods with errors
    q.put(ValueError("foobar"))
    for _ in range(2):
        with pytest.raises(ValueError) as exc:
            q.get()
        assert str(exc.value) == 'foobar'


def test_event_queue_cleanup(kv_test_app):
    q = kv_test_app.kv.events(prefix='foo')

    # Put a few events in the queue
    kv_test_app.kv['foo'] = b'1'
    del kv_test_app.kv['foo']

    # No extra references
    assert sys.getrefcount(q) == 2

    # del causes queue to be collected
    assert len(kv_test_app.kv._filter_to_queues) == 1
    ref = weakref.ref(q)
    del q
    assert ref() is None
    assert len(kv_test_app.kv._filter_to_queues) == 0


def get_n(q, n):
    return [q.get(timeout=2) for _ in range(n)]


def put_event(k, v, o, filter):
    return kv.Event(k, kv.ValueOwnerPair(v, o), kv.EventType.PUT, filter)


def del_event(k, filter):
    return kv.Event(k, None, kv.EventType.DELETE, filter)


def test_event_ranges(kv_test_app):
    before_d = kv_test_app.kv.events(end='d')
    after_c = kv_test_app.kv.events(start='c')
    b_to_e = kv_test_app.kv.events(start='b', end='e')
    everything = kv_test_app.kv.events()

    kv_test_app.kv.put('a', b'1')
    kv_test_app.kv.put('b', b'2')
    kv_test_app.kv.put('c', b'3')
    kv_test_app.kv.put('d', b'4')
    kv_test_app.kv.put('e', b'5')
    kv_test_app.kv.put('f', b'6')
    kv_test_app.kv.discard_range(start='b', end='d')
    kv_test_app.kv.discard_range(end='d')
    kv_test_app.kv.discard_range(start='e')
    kv_test_app.kv.discard('d')
    assert len(kv_test_app.kv) == 0

    # Open start
    filt = next(iter(before_d.filters))
    sol = [put_event('a', b'1', None, filt),
           put_event('b', b'2', None, filt),
           put_event('c', b'3', None, filt),
           del_event('b', filt),
           del_event('c', filt),
           del_event('a', filt)]
    res = get_n(before_d, len(sol))
    assert res == sol

    # Open end
    filt = next(iter(after_c.filters))
    sol = [put_event('c', b'3', None, filt),
           put_event('d', b'4', None, filt),
           put_event('e', b'5', None, filt),
           put_event('f', b'6', None, filt),
           del_event('c', filt),
           del_event('e', filt),
           del_event('f', filt),
           del_event('d', filt)]
    res = get_n(after_c, len(sol))
    assert res == sol

    # Closed start and end
    filt = next(iter(b_to_e.filters))
    sol = [put_event('b', b'2', None, filt),
           put_event('c', b'3', None, filt),
           put_event('d', b'4', None, filt),
           del_event('b', filt),
           del_event('c', filt),
           del_event('d', filt)]
    res = get_n(b_to_e, len(sol))
    assert res == sol

    # Open start and end
    filt = next(iter(everything.filters))
    sol = [put_event('a', b'1', None, filt),
           put_event('b', b'2', None, filt),
           put_event('c', b'3', None, filt),
           put_event('d', b'4', None, filt),
           put_event('e', b'5', None, filt),
           put_event('f', b'6', None, filt),
           del_event('b', filt),
           del_event('c', filt),
           del_event('a', filt),
           del_event('e', filt),
           del_event('f', filt),
           del_event('d', filt)]
    res = get_n(everything, len(sol))
    assert res == sol


def test_event_types(kv_test_app):
    put_b_to_e = kv_test_app.kv.events(start='b',
                                       end='e',
                                       event_type='put')
    del_b_to_e = kv_test_app.kv.events(start='b',
                                       end='e',
                                       event_type='delete')

    kv_test_app.kv.put('a', b'1')
    kv_test_app.kv.put('b', b'2')
    kv_test_app.kv.put('c', b'3')
    kv_test_app.kv.put('d', b'4')
    kv_test_app.kv.put('e', b'5')
    kv_test_app.kv.discard_range(start='b', end='d')
    kv_test_app.kv.discard_range(end='d')
    kv_test_app.kv.discard_range(start='e')
    kv_test_app.kv.clear()

    # put
    filt = next(iter(put_b_to_e.filters))
    sol = [put_event('b', b'2', None, filt),
           put_event('c', b'3', None, filt),
           put_event('d', b'4', None, filt)]
    res = get_n(put_b_to_e, len(sol))
    assert res == sol

    # del
    filt = next(iter(del_b_to_e.filters))
    sol = [del_event('b', filt),
           del_event('c', filt),
           del_event('d', filt)]
    res = get_n(del_b_to_e, len(sol))
    assert res == sol


def test_event_multiple_subscriptions(kv_test_app):
    q1 = kv_test_app.kv.event_queue()
    before_b = q1.subscribe(end='b')
    after_c = q1.subscribe(start='c')

    q2 = kv_test_app.kv.event_queue()
    assert q2.subscribe(end='b') == before_b
    after_c_del = q2.subscribe(start='c', event_type='delete')

    kv_test_app.kv.put('a', b'1')
    kv_test_app.kv.put('b', b'2')
    kv_test_app.kv.put('c', b'3')
    kv_test_app.kv.put('d', b'4')
    kv_test_app.kv.discard_range(start='b', end='d')
    kv_test_app.kv.discard_range(end='d')
    kv_test_app.kv.discard_range(start='e')
    kv_test_app.kv.clear()

    sol = [put_event('a', b'1', None, before_b),
           put_event('c', b'3', None, after_c),
           put_event('d', b'4', None, after_c),
           del_event('c', after_c),
           del_event('a', before_b),
           del_event('d', after_c)]
    res = get_n(q1, len(sol))
    assert res == sol

    sol = [put_event('a', b'1', None, before_b),
           del_event('c', after_c_del),
           del_event('a', before_b),
           del_event('d', after_c_del)]
    res = get_n(q2, len(sol))
    assert res == sol


def test_events_and_ownership(kv_test_app):
    kv_test_app.scale('sleeper', 2)
    c1, c2 = (c.id for c in kv_test_app.get_containers())

    q1 = kv_test_app.kv.events(prefix='a')
    q2 = kv_test_app.kv.events(prefix='a', event_type='put')

    # Create some owned keys
    kv_test_app.kv.put('a1', value=b'1', owner=c1)
    kv_test_app.kv.put('a2', value=b'2', owner=c1)
    kv_test_app.kv.put('a3', value=b'3', owner=c2)
    kv_test_app.kv.put('a4', value=b'4', owner=c2)

    # Change owner of some
    kv_test_app.kv.put('a3', owner=c1)
    kv_test_app.kv.put('a4', owner=None)

    # Delete an owned key
    del kv_test_app.kv['a1']

    # Kill c1
    kv_test_app.kill_container(c1)

    # Delete all remaining keys
    kv_test_app.kv.clear()

    filt = next(iter(q1.filters))
    sols = [[put_event('a1', b'1', c1, filt),
             put_event('a2', b'2', c1, filt),
             put_event('a3', b'3', c2, filt),
             put_event('a4', b'4', c2, filt),
             put_event('a3', b'3', c1, filt),
             put_event('a4', b'4', None, filt)] +
            [del_event('a1', filt),
             del_event(x, filt),
             del_event(y, filt),
             del_event('a4', filt)]
            for x, y in [('a2', 'a3'), ('a3', 'a2')]]
    res = get_n(q1, len(sols[0]))
    assert res in sols

    filt = next(iter(q2.filters))
    sol = [put_event('a1', b'1', c1, filt),
           put_event('a2', b'2', c1, filt),
           put_event('a3', b'3', c2, filt),
           put_event('a4', b'4', c2, filt),
           put_event('a3', b'3', c1, filt),
           put_event('a4', b'4', None, filt)]
    res = get_n(q2, len(sol))
    assert res == sol


def test_key_value_wait(kv_test_app):
    def set_foo():
        time.sleep(0.5)
        kv_test_app.kv['foo'] = b'1'

    for return_owner in [True, False]:
        kv_test_app.kv.clear()

        sol = (b'1', None) if return_owner else b'1'

        setter = threading.Thread(target=set_foo)
        setter.daemon = True
        setter.start()

        val = kv_test_app.kv.wait('foo', return_owner=return_owner)
        assert val == sol

        # Get immediately for set keys
        val = kv_test_app.kv.wait('foo', return_owner=return_owner)
        assert val == sol


def test_events_application_shutdown(client):
    with run_application(client) as app:
        q = app.kv.events(prefix='a')
        app.kv.put('a1', b'1')
        app.kv.put('a2', b'2')
        app.kv.put('a3', b'3')
        app.shutdown()

    filt = next(iter(q.filters))
    assert q.get() == put_event('a1', b'1', None, filt)
    assert q.get() == put_event('a2', b'2', None, filt)
    assert q.get() == put_event('a3', b'3', None, filt)

    # All further requests error with connection error
    for _ in range(2):
        with pytest.raises(ConnectionError):
            q.get()
