from __future__ import print_function, division, absolute_import

from .compatibility import urlparse


def ensure_bytes(x):
    if type(x) is not bytes:
        x = x.encode('utf-8')
    return x


def format_list(x):
    return "\n".join("- %s" % s for s in sorted(x))


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
    header = row_template % tuple(columns)
    if rows:
        data = '\n'.join((row_template % r).strip() for r in rows)
        return '\n'.join([header, data])
    else:
        return header


def normalize_address(addr, scheme='http'):
    p = urlparse(addr)
    address = p.netloc if p.scheme else p.path
    return '%s://%s' % (scheme, address)


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
