from __future__ import print_function, division, absolute_import

import argparse
import base64
import errno
import os
import signal
import sys

import requests
import yaml

from . import __version__
from .core import start_java_client, SkeinAuth, Client, AMClient
from .utils import (read_secret, read_daemon, CONFIG_PATH, DAEMON_PATH,
                    SECRET_PATH, format_table)


def eprint(x):
    print(x, file=sys.stderr)


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


def node(subs, name, help):
    @subcommand(subs, name, help)
    def f():
        f.parser.print_usage()
    f.subs = f.parser.add_subparsers(metavar='command')
    return f


entry = argparse.ArgumentParser(prog="skein",
                                description="Define and run YARN jobs",
                                add_help=False)
add_help(entry)
entry.add_argument("--version", action='version',
                   version='%(prog)s ' + __version__,
                   help="Show version then exit")
entry.set_defaults(func=lambda: entry.print_usage())
entry_subs = entry.add_subparsers(metavar='command')

# Sub command nodes
daemon = node(entry_subs, 'daemon', 'Manage the skein daemon')
keystore = node(entry_subs, 'keystore', 'Manage the skein keystore')

# Common arguments
app_id = arg('app_id', type=str, help='The application id')
spec = arg('spec', type=str, help='The specification file')
app_id_optional = arg('--id', dest='app_id', type=str,
                      help='The application id. If used in a container during '
                           'a skein job, omit this to have it inferred from '
                           'the environment')


def get_client():
    try:
        return Client(start_java=False)
    except ValueError:
        eprint("Skein daemon not found, please run `skein daemon start`")
        sys.exit(1)


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
    os.unlink(DAEMON_PATH)
    if verbose:
        print("Daemon stopped")


@subcommand(keystore.subs,
            'get', 'Get a value from the keystore',
            app_id_optional,
            arg('key', type=str, help='The key to get'))
def keystore_get(key, app_id=None):
    if app_id is None:
        am_client = AMClient.from_env()
    else:
        client = get_client()
        am_client = AMClient.from_id(app_id, client=client)
    print(am_client.get_key(key))


@subcommand(keystore.subs,
            'set', 'Set a value in the keystore',
            app_id_optional,
            arg('key', type=str, help='The key to set'),
            arg('val', type=str, help='The value to set'))
def keystore_set(key, val, app_id=None):
    if app_id is None:
        am_client = AMClient.from_env()
    else:
        client = get_client()
        am_client = AMClient.from_id(app_id, client=client)
    return am_client.set_key(key, val)


@subcommand(entry_subs,
            'start', 'Start a Skein Job',
            spec)
def do_start(spec):
    client = get_client()
    app = client.submit(spec)
    print(app.app_id)


@subcommand(entry_subs,
            'status', 'Status of Skein Jobs',
            app_id_optional,
            arg("--state", "-s", action='append'))
def do_status(app_id=None, state=None):
    client = get_client()
    apps = client.status(app_id=app_id, state=state)
    if app_id is not None:
        apps = [apps]
    header = ['application_id', 'name', 'state', 'status', 'containers',
              'vcores', 'memory']
    data = []
    for a in apps:
        data.append((a.id, a.name, a.state, a.final_status,
                     a.usage.num_used_containers,
                     a.usage.used_resources.vcores,
                     a.usage.used_resources.memory))
    print(format_table(header, sorted(data)))


@subcommand(entry_subs,
            'inspect', 'Information about a Skein Job',
            app_id,
            arg('--service', '-s', type=str, help='Service name'))
def do_inspect(app_id, service=None):
    client = get_client()
    resp = client.application(app_id).inspect(service=service)
    if service is not None:
        resp = {service: resp}
    data = {k: v.to_dict() for k, v in resp.items()}
    print(yaml.dump(data, default_flow_style=False))


@subcommand(entry_subs,
            'kill', 'Kill a Skein Job',
            app_id)
def do_kill(app_id):
    client = get_client()
    client.kill(app_id)


@subcommand(entry_subs,
            'config', 'Initialize skein configuration',
            arg('--force', '-f', action='store_true',
                help='Overwrite existing configuration'))
def do_config(force):
    os.makedirs(CONFIG_PATH, exist_ok=True)
    secret = base64.b64encode(os.urandom(30))
    try:
        mode = 'wb' if force else 'xb'
        with open(SECRET_PATH, mode=mode) as fil:
            fil.write(secret)
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            eprint("secret file already exists, use --force to override")
            sys.exit(1)


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    kwargs.pop('func')(**kwargs)


if __name__ == '__main__':
    main()
