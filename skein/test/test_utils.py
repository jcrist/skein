from __future__ import absolute_import, print_function, division

import gc
import weakref
from datetime import timedelta

import pytest

from skein.compatibility import PY2
from skein.utils import with_finalizers, humanize_timedelta, format_table


@with_finalizers
class Foo(object):
    def __init__(self, f, tag):
        self.tag = tag
        self._add_finalizer(f, (tag, 1))
        self._add_finalizer(f, (tag, 2))

    def __repr__(self):
        return 'Foo<%r>' % self.tag


def test_with_finalizers():
    log = []
    f = Foo(log.append, 'deleted')
    rf = weakref.ref(f)
    del f
    assert rf() is None
    assert log == [('deleted', 2), ('deleted', 1)]


def test_with_finalizers_explicit():
    log = []
    f = Foo(log.append, 'deleted')
    f._finalize()
    assert log == [('deleted', 2), ('deleted', 1)]
    del log[:]
    f._finalize()
    assert log == []


def test_with_finalizers_indirect_reference_cycle():
    # Involved in a cycle, but not directly
    log = []
    a = Foo(log.append, "a")
    ra = weakref.ref(a)
    cycle = [a]
    cycle.append(cycle)
    del a
    del cycle
    gc.collect()

    assert ra() is None

    assert log == [('a', 2), ('a', 1)]


@pytest.mark.skipif(PY2, reason=("In Python 2, objects in reference cycles "
                                 "with `__del__` implemented are never "
                                 "collected. In this case the finalizers "
                                 "are run at exit, but that's hard to test."))
def test_with_finalizers_direct_reference_cycle():
    # Directly involved in a cycle
    log = []
    b = Foo(log.append, "b")
    c = Foo(log.append, "c")
    rb = weakref.ref(b)
    rc = weakref.ref(c)
    b.c = c
    c.b = b
    del b
    del c
    gc.collect()

    assert rb() is None
    assert rc() is None

    sol1 = [('b', 2), ('b', 1), ('c', 2), ('c', 1)]
    sol2 = [('c', 2), ('c', 1), ('b', 2), ('b', 1)]
    assert log == sol1 or log == sol2


def test_with_finalizers_del_warns_on_exception(capsys):
    log = []

    def finalizer(tag):
        log.append(tag)
        raise ValueError(tag)

    a = Foo(finalizer, 'a')
    del a
    out, err = capsys.readouterr()

    assert not out
    assert err.count('ValueError') >= 2
    assert log == [('a', 2), ('a', 1)]


def test_with_finalizers_finalize_raises_on_exception():
    log = []

    def finalizer(tag):
        log.append(tag)
        raise ValueError(tag)

    a = Foo(finalizer, 'a')

    with pytest.raises(ValueError):
        a._finalize()

    assert log == [('a', 2)]


def test_humanize_timedelta():
    assert humanize_timedelta(timedelta(0, 6)) == '6s'
    assert humanize_timedelta(timedelta(0, 601)) == '10m'
    assert humanize_timedelta(timedelta(0, 6001)) == '1h 40m'


def test_format_table():
    res = format_table(['fruit', 'color'],
                       [('apple', 'red'),
                        ('banana', 'yellow'),
                        ('tomato', 'red'),
                        ('pear', 'green')])
    sol = ('FRUIT     COLOR\n'
           'apple     red\n'
           'banana    yellow\n'
           'tomato    red\n'
           'pear      green')
    assert res == sol
    assert format_table(['fruit', 'color'], []) == 'FRUIT    COLOR'
