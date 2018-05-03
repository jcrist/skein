from __future__ import print_function, division, absolute_import

import argparse
import os
import signal

import requests

from . import __version__
from .core import (start_java_client, read_daemon_file, get_daemon_path,
                   SkeinAuth)
from .utils import load_config


def add_help(parser):
    parser.add_argument("--help", "-h", action='help',
                        help="Show this help message then exit")


def arg(*args, **kwargs):
    return (args, kwargs)


def subcommand(subparsers, name, help, *args):
    def _(func):
        parser = subparsers.add_parser(name,
                                       help=help,
                                       description=help,
                                       add_help=False)
        parser.set_defaults(func=func)
        for arg in args:
            parser.add_argument(*arg[0], **arg[1])
        add_help(parser)
        func.parser = parser
        return func
    return _


entry = argparse.ArgumentParser(prog="skein",
                                description="Interact with YARN jobs via skein",
                                add_help=False)
add_help(entry)
entry.add_argument("--version", action='version',
                   version='%(prog)s ' + __version__,
                   help="Show version then exit")
entry_subs = entry.add_subparsers(metavar='command')

verbose = arg("--verbose", "-v", action="count", default=0)


@subcommand(entry_subs,
            'daemon', 'Interact with the skein daemon')
def daemon():
    daemon.parser.print_usage()


daemon.subs = daemon.parser.add_subparsers(metavar='command')


@subcommand(daemon.subs,
            'start', 'Start the skein daemon')
def daemon_start():
    secret = load_config()['skein.secret']
    address, pid = read_daemon_file()
    if address is not None:
        try:
            resp = requests.get("%s/skein" % address, auth=SkeinAuth(secret))
        except requests.ConnectionError:
            ok = False
        else:
            ok = resp.ok
    else:
        ok = False

    if not ok:
        daemon_stop(verbose=False)
        address, _ = start_java_client(secret, daemon=True)
    print(address)


@subcommand(daemon.subs,
            'address', 'The address of the running daemon')
def daemon_address():
    address, _ = read_daemon_file()
    if address is None:
        print("No skein daemon is running")
    else:
        print(address)


@subcommand(daemon.subs,
            'stop', 'Stop the skein daemon')
def daemon_stop(verbose=True):
    secret = load_config()['skein.secret']
    address, pid = read_daemon_file()
    if address is None:
        if verbose:
            print("No skein daemon is running")
        return

    try:
        resp = requests.post("%s/skein/shutdown" % address,
                             auth=SkeinAuth(secret))
    except requests.ConnectionError:
        pass
    else:
        if resp.status_code == 401:
            # Skein daemon started with a different secret, kill it manually
            os.kill(pid, signal.SIGTERM)
    os.unlink(get_daemon_path())
    if verbose:
        print("Daemon stopped")


@subcommand(entry_subs,
            'list', 'List all Skein jobs',
            verbose)
def do_list(verbose):
    pass


@subcommand(entry_subs,
            'start', 'Start a Skein Job',
            arg('spec', type=str, help='the specification file'),
            verbose)
def do_start(spec, verbose):
    pass


@subcommand(entry_subs,
            'stop', 'Stop a Skein Job',
            arg('id', type=str, help='the application id'),
            verbose)
def do_stop(id, verbose):
    pass


@subcommand(entry_subs,
            'kill', 'Kill a Skein Job',
            arg('id', type=str, help='the application id'),
            verbose)
def do_kill(id, verbose):
    pass


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    kwargs.pop('func')(**kwargs)


if __name__ == '__main__':
    main()
