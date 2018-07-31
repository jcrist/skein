from __future__ import absolute_import, print_function, division

from datetime import timedelta

from skein.utils import (humanize_timedelta, format_table,
                         format_comma_separated_list)


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


def test_format_comma_separated_list():
    assert format_comma_separated_list([]) == ''
    assert format_comma_separated_list([None]) == 'None'
    assert format_comma_separated_list([None, False]) == 'None or False'
    assert format_comma_separated_list([None, False, True]) == 'None, False, or True'
