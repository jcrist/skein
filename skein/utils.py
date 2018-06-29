from __future__ import print_function, division, absolute_import

import weakref

from .compatibility import PY2, add_method, unicode


if PY2:
    from backports.weakref import finalize
else:
    from weakref import finalize


def with_finalizers(cls):
    """Adds support for robust cleanup of objects by the GC.

    Objects should register handlers during operation using
    ``self._add_finalizer``. On cleanup, these finalizers will be called in the
    reverse order that they're added. Objects can also call ``self._finalize``
    to explicitly trigger finalization. Each finalizer is only run once, even
    upon error. If ``self._finalize`` is called explicitly and an exception
    occurs in one of the finalizers, the remaining finalizers won't be run until
    the object is collected, even if ``self._finalize`` is called again.

    On Python 3, finalizers are guaranteed to be run as soon as object is
    collected. On Python 2 the finalizers usually are run when the object is
    collected, but in certain situations with reference cycles, they may not
    run until program termination.
    """
    _finalizers = weakref.WeakKeyDictionary()

    @add_method(cls)
    def _finalize(self):
        for f in reversed(_finalizers.pop(self, ())):
            f()

    @add_method(cls)
    def _add_finalizer(self, func, *args, **kwargs):
        if self not in _finalizers:
            _finalizers[self] = []
        _finalizers[self].append(finalize(self, func, *args, **kwargs))

    return cls


def ensure_unicode(x):
    if type(x) is not unicode:
        x = x.decode('utf-8')
    return x


def format_list(x):
    return "\n".join("- %s" % s for s in sorted(x))


def humanize_timedelta(td):
    """Pretty-print a timedelta in a human readable format."""
    secs = int(td.total_seconds())
    hours, secs = divmod(secs, 60 * 60)
    mins, secs = divmod(secs, 60)
    if hours:
        return '%dh %dm' % (hours, mins)
    if mins:
        return '%dm' % mins
    return '%ds' % secs


def format_table(columns, rows):
    """Formats an ascii table for given columns and rows.

    Parameters
    ----------
    columns : list
        The column names
    rows : list of tuples
        The rows in the table. Each tuple must be the same length as
        ``columns``.
    """
    rows = [tuple(str(i) for i in r) for r in rows]
    columns = tuple(str(i).upper() for i in columns)
    if rows:
        widths = tuple(max(max(map(len, x)), len(c))
                       for x, c in zip(zip(*rows), columns))
    else:
        widths = tuple(map(len, columns))
    row_template = ('    '.join('%%-%ds' for _ in columns)) % widths
    header = (row_template % tuple(columns)).strip()
    if rows:
        data = '\n'.join((row_template % r).strip() for r in rows)
        return '\n'.join([header, data])
    else:
        return header


class cached_property(object):

    def __init__(self, func):
        self.__doc__ = getattr(func, "__doc__")
        self.func = func

    def __get__(self, obj, cls):
        if obj is None:
            return self

        res = obj.__dict__[self.func.__name__] = self.func(obj)
        return res


def implements(f):
    def decorator(g):
        g.__doc__ = f.__doc__
        return g
    return decorator
