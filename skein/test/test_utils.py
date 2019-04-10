from __future__ import absolute_import, print_function, division

import multiprocessing
from datetime import timedelta, datetime

from skein.compatibility import UTC
from skein.utils import (humanize_timedelta, format_table,
                         format_comma_separated_list, lock_file,
                         datetime_from_millis, datetime_to_millis)


def test_humanize_timedelta():
    assert humanize_timedelta(timedelta(0, 6)) == '6s'
    assert humanize_timedelta(timedelta(0, 601)) == '10m'
    assert humanize_timedelta(timedelta(0, 6001)) == '1h 40m'


def test_datetime_conversion():
    # Timezone naive
    now = datetime.now()
    now2 = datetime_from_millis(datetime_to_millis(now))
    now3 = datetime_from_millis(datetime_to_millis(now2))
    # Rounded to nearest millisecond
    assert abs(now2 - now).total_seconds() < 0.001
    assert now2 == now3

    # Timezone aware
    now = datetime.now(UTC)
    now2 = datetime_from_millis(datetime_to_millis(now)).replace(tzinfo=UTC)
    now3 = datetime_from_millis(datetime_to_millis(now2)).replace(tzinfo=UTC)
    # Rounded to nearest millisecond
    assert abs(now2 - now).total_seconds() < 0.001
    assert now2 == now3


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


def test_lock_file_single_process(tmpdir):
    path = str(tmpdir.join('lock'))
    acquired = False
    with lock_file(path):
        acquired = True
    assert acquired


def test_lock_file_same_object_used_for_same_path(tmpdir):
    path = str(tmpdir.join('lock'))
    lock1 = lock_file(path)
    lock2 = lock_file(path)
    assert lock1 is lock2


class Locker(multiprocessing.Process):
    def __init__(self, path=None):
        super(Locker, self).__init__()
        self.path = path
        self.daemon = True
        self.parent_conn, self.child_conn = multiprocessing.Pipe()

    def shutdown(self):
        if not self.parent_conn.closed:
            self.parent_conn.send('shutdown')
            self.parent_conn.close()
        self.join()

    def run(self):
        lock = lock_file(self.path)
        while True:
            msg = self.child_conn.recv()
            if msg == 'acquire':
                lock.acquire()
                self.child_conn.send('acquired')
            elif msg == 'release':
                lock.release()
                self.child_conn.send('released')
            elif msg == 'shutdown':
                break
        self.child_conn.close()


def test_lock_file_multiple_processes(tmpdir):
    path = str(tmpdir.join('lock'))

    lock = lock_file(path)

    locker = Locker(path=path)
    locker.start()
    locker.parent_conn.send('acquire')
    assert 'acquired' == locker.parent_conn.recv()
    locker.parent_conn.send('release')
    assert 'released' == locker.parent_conn.recv()

    lock.acquire()
    locker.parent_conn.send('acquire')
    assert not locker.parent_conn.poll(0.1)
    lock.release()
    assert 'acquired' == locker.parent_conn.recv()
    locker.parent_conn.send('release')
    assert 'released' == locker.parent_conn.recv()
    locker.shutdown()
    locker.join()
