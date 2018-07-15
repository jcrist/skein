from __future__ import print_function, division, absolute_import

import copy
import operator
from collections import MutableMapping, OrderedDict

import pytest

from skein import kv
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
        cls(b'foo')


def test_comparison_type():
    # test using `comparison` directly
    v = kv.comparison('foo', 'value', '==', b'bar')
    assert v == (kv.value('foo') == b'bar')

    o = kv.comparison('foo', 'owner', '==', 'bar_1')
    assert o == (kv.owner('foo') == 'bar_1')

    with pytest.raises(TypeError):
        kv.comparison(b'foo', 'value', '==', b'bar')

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


@pytest.mark.parametrize('cls', [kv.contains,
                                 kv.missing])
def test_contains_and_missing_types(cls):
    x = cls('foo')
    assert repr(x) == ("%s('foo')" % cls.__name__)
    assert x == copy.deepcopy(x)
    assert x != cls('bar')

    assert kv.is_condition(x)
    assert kv.is_operation(x)

    with pytest.raises(AttributeError):
        x.foobar = 1

    with pytest.raises(TypeError):
        cls(b'foo')


@pytest.mark.parametrize('cls', [kv.count, kv.list_keys])
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

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(ValueError):
        cls(prefix='foo', range_start='bar')

    for k in ['prefix', 'range_start', 'range_end']:
        with pytest.raises(TypeError):
            cls(**{k: b'bad type'})


def test_discard_type():
    x = kv.discard('foo')
    assert repr(x) == "discard('foo')"
    assert x == copy.deepcopy(x)
    assert x != kv.discard('bar')

    assert kv.is_operation(x)

    with pytest.raises(AttributeError):
        x.foobar = 1

    with pytest.raises(TypeError):
        kv.discard(b'foo')


@pytest.mark.parametrize('cls', [kv.get, kv.pop])
def test_get_and_pop_types(cls):
    name = cls.__name__
    x1 = cls('key')
    assert repr(x1) == ("%s('key', default=None, return_owner=False)" % name)
    x2 = cls('key', default=b'bar', return_owner=True)
    assert repr(x2) == ("%s('key', default=b'bar', return_owner=True)" % name)
    for x in [x1, x2]:
        assert x == copy.deepcopy(x)
    assert x1 != x2

    assert kv.is_operation(x1)

    with pytest.raises(AttributeError):
        x1.foobar = 1

    with pytest.raises(TypeError):
        cls('foo', default='bar')

    with pytest.raises(TypeError):
        cls('foo', return_owner=None)

    with pytest.raises(TypeError):
        cls(b'foo')


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
        cls(b'foo')

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
            cls(**{k: b'bad type'})


@pytest.mark.parametrize('cls,has_return_owner',
                         [(kv.put, False), (kv.swap, True)])
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

    assert kv.is_operation(x1)

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


def test_key_value_contains_and_missing(kv_test_app):
    kv_test_app.kv.update(kv_test_data)
    assert 'bar' in kv_test_app.kv
    assert 'missing' not in kv_test_app.kv
    assert kv_test_app.kv.contains('bar')
    assert not kv_test_app.kv.missing('bar')
    assert not kv_test_app.kv.contains('missing')
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
    assert not kv_test_app.kv.contains('c1_1')
    assert kv_test_app.kv.contains('c1_2')
    assert kv_test_app.kv.contains('c1_3')


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
    # contains
    assert run(kv.contains('owner'))
    assert not run(kv.contains('missing'))
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
    true1 = kv.contains('owner')
    true2 = kv.contains('no_owner')
    false = kv.contains('missing')
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
            kwargs = {'conditions': [kv.contains('a')],
                      'on_success': [kv.put('b', b'b'), op],
                      'on_failure': [kv.put('c', b'c')]}
        else:
            kwargs = {'conditions': [kv.missing('a')],
                      'on_success': [kv.put('c', b'c')],
                      'on_failure': [kv.put('b', b'b'), op]}

        with pytest.raises(ValueError) as exc:
            kv_test_app.kv.transaction(**kwargs)

        assert str(exc.value) == msg
        assert not kv_test_app.kv.contains('b')
        assert not kv_test_app.kv.contains('c')


def test_transaction(kv_test_app):
    cid = kv_test_app.get_containers()[0].id

    kv_test_app.kv.put('a', b'a', owner=cid)
    kv_test_app.kv.put('aa', b'aa')
    kv_test_app.kv.put('aaa', b'aaa')
    kv_test_app.kv.put('b', b'b')

    true1 = kv.contains('a')
    true2 = kv.contains('b')
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
        assert not kv_test_app.kv.contains('dont_put_me')

        # Test empty result branch
        kwargs = {'conditions': cond, dont: [kv.put('dont_put_me', b'bad')]}
        res = kv_test_app.kv.transaction(**kwargs)
        assert res == (succeeded, [])
        assert not kv_test_app.kv.contains('dont_put_me')

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
