from __future__ import print_function, division, absolute_import

import argparse
import os
import signal
import sys

import requests

from . import __version__
from .core import start_java_client, SkeinAuth, Client
from .utils import read_secret, read_daemon, daemon_path


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


@subcommand(entry_subs,
            'daemon', 'Interact with the skein daemon')
def daemon():
    daemon.parser.print_usage()


daemon.subs = daemon.parser.add_subparsers(metavar='command')


@subcommand(daemon.subs,
            'start', 'Start the skein daemon')
def daemon_start():
    secret = read_secret()
    address, pid = read_daemon()
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
    address, _ = read_daemon()
    if address is None:
        print("No skein daemon is running")
    else:
        print(address)


@subcommand(daemon.subs,
            'stop', 'Stop the skein daemon')
def daemon_stop(verbose=True):
    secret = read_secret()
    address, pid = read_daemon()
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
    os.unlink(daemon_path())
    if verbose:
        print("Daemon stopped")


def get_client():
    try:
        return Client(start_java=False)
    except ValueError:
        print("Skein daemon not found, please run `skein daemon start`")
        sys.exit(1)


@subcommand(entry_subs,
            'start', 'Start a Skein Job',
            arg('spec', type=str, help='the specification file'))
def do_start(spec):
    client = get_client()
    app = client.submit(spec)
    print(app.app_id)


@subcommand(entry_subs,
            'status', 'Status of a Skein Job',
            arg('id', type=str, help='the application id'))
def do_status(id):
    client = get_client()
    resp = client.status(id)
    state = resp['state']
    status = resp['finalStatus']
    print("State: %r, Status: %r" % (state, status))


@subcommand(entry_subs,
            'kill', 'Kill a Skein Job',
            arg('id', type=str, help='the application id'))
def do_kill(id):
    client = get_client()
    client.kill(id)


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    kwargs.pop('func')(**kwargs)


if __name__ == '__main__':
    main()
