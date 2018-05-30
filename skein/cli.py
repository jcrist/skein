from __future__ import print_function, division, absolute_import

import argparse
import os
import sys
import traceback

import yaml

from . import __version__
from .core import (Client, ApplicationClient, Security, start_global_daemon,
                   stop_global_daemon)
from .exceptions import context, SkeinError, DaemonNotRunningError
from .model import Job
from .utils import format_table, humanize_timedelta


class _Formatter(argparse.HelpFormatter):
    """Format with a fixed argument width, due to bug in argparse measuring
    argument widths"""
    @property
    def _action_max_length(self):
        return 16

    @_action_max_length.setter
    def _action_max_length(self, value):
        pass


def fail(msg, prefix=True):
    if prefix:
        msg = 'Error: %s' % msg
    print(msg, file=sys.stderr)
    sys.exit(1)


def add_help(parser):
    parser.add_argument("--help", "-h", action='help',
                        help="Show this help message then exit")


def arg(*args, **kwargs):
    return (args, kwargs)


def subcommand(subparsers, name, help, *args):
    def _(func):
        parser = subparsers.add_parser(name,
                                       help=help,
                                       formatter_class=_Formatter,
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
                                formatter_class=_Formatter,
                                add_help=False)
add_help(entry)
entry.add_argument("--version", action='version',
                   version='%(prog)s ' + __version__,
                   help="Show version then exit")
entry.set_defaults(func=lambda: entry.print_usage())
entry_subs = entry.add_subparsers(metavar='command')

# Common arguments
app_id = arg('app_id', help='The application id', metavar='APP_ID')
optional_app_id = arg('app_id', metavar='APP_ID',
                      help='The application id. To use in a container during '
                           'a skein job, pass in "current"')
container_id = arg('--id', required=True,
                   help='The container id', metavar='CONTAINER_ID')


###################
# DAEMON COMMANDS #
###################

daemon = node(entry_subs, 'daemon', 'Manage the skein daemon')

log = arg("--log", default=False,
          help="If provided, the daemon will write logs here.")


@subcommand(daemon.subs,
            'start', 'Start the skein daemon',
            log)
def daemon_start(log=False):
    print(start_global_daemon(log=log))


@subcommand(daemon.subs,
            'address', 'The address of the running daemon')
def daemon_address():
    try:
        client = Client()
        print(client.address)
    except DaemonNotRunningError:
        print("No skein daemon is running")


@subcommand(daemon.subs,
            'stop', 'Stop the skein daemon')
def daemon_stop():
    stop_global_daemon()


@subcommand(daemon.subs,
            'restart', 'Restart the skein daemon',
            log)
def daemon_restart(log=False):
    daemon_stop()
    daemon_start(log=log)


#####################
# KEYSTORE COMMANDS #
#####################

kv = node(entry_subs, 'kv', 'Manage the skein key-value store')


@subcommand(kv.subs,
            'get', 'Get a value from the key-value store',
            optional_app_id,
            arg('--key', help='The key to get. Omit to get the whole '
                              'key-value store.'),
            arg('--wait', action='store_true',
                help='If true, will block until the key is set'))
def kv_get(app_id, key=None, wait=False):
    if app_id == 'current':
        app = ApplicationClient.connect_to_current()
    else:
        app = Client().connect(app_id)

    if key is None:
        result = app.kv.to_dict()
        if result:
            print('\n'.join('%s: %s' % i for i in result.items()))
    else:
        result = app.kv.wait(key) if wait else app.kv[key]
        print(result)


@subcommand(kv.subs,
            'set', 'Set a value in the key-value store',
            optional_app_id,
            arg('--key', required=True, help='The key to set'),
            arg('--value', required=True, help='The value to set'))
def kv_set(app_id, key=None, value=None):
    if key is None:
        fail("--key is required")
    elif value is None:
        fail("--value is required")

    if app_id == 'current':
        app = ApplicationClient.connect_to_current()
    else:
        app = Client().connect(app_id)

    app.kv[key] = value


@subcommand(kv.subs,
            'del', 'Delete a value from the key-value store',
            optional_app_id,
            arg('--key', help='The key to delete.'))
def kv_del(app_id, key=None, wait=False):
    if app_id == 'current':
        app = ApplicationClient.connect_to_current()
    else:
        app = Client().connect(app_id)

    del app.kv[key]


########################
# APPLICATION COMMANDS #
########################

application = node(entry_subs, 'application', 'Manage applications')


def _print_application_status(apps):
    header = ['application_id', 'name', 'state', 'status', 'containers',
              'vcores', 'memory']
    data = []
    for a in apps:
        data.append((a.id, a.name, a.state, a.final_status,
                     a.usage.num_used_containers,
                     a.usage.used_resources.vcores,
                     a.usage.used_resources.memory))
    print(format_table(header, sorted(data)))


@subcommand(application.subs,
            'submit', 'Submit a Skein Job',
            arg('spec', help='The specification file'))
def application_submit(spec):
    if not os.path.exists(spec):
        fail("No job specification file at %r" % spec)
    try:
        job = Job.from_file(spec)
    except SkeinError as exc:
        # Prettify expected errors, let rest bubble up
        fail('In file %r, %s' % (spec, exc))

    app = Client().submit(job)
    print(app.app_id)


@subcommand(application.subs,
            'ls', 'List applications',
            arg("--state", "-s", action='append',
                help=('Filter by application states. May be repeated '
                      'to select multiple states.')))
def application_ls(state=None):
    apps = Client().applications(states=state)
    _print_application_status(apps)


@subcommand(application.subs,
            'status', 'Status of a Skein application',
            app_id)
def application_status(app_id):
    apps = Client().status(app_id)
    _print_application_status([apps])


@subcommand(application.subs,
            'kill', 'Kill a Skein application',
            app_id)
def application_kill(app_id):
    Client().kill(app_id)


@subcommand(application.subs,
            'describe', 'Get specifications for a running skein application',
            app_id,
            arg('--service', '-s', help='Service name'))
def application_describe(app_id, service=None):
    client = Client()
    resp = client.connect(app_id).describe(service=service)
    if service is not None:
        out = yaml.dump({service: resp.to_dict(skip_nulls=True)},
                        default_flow_style=False)
    else:
        out = resp.to_yaml(skip_nulls=True)
    print(out)


######################
# CONTAINER COMMANDS #
######################

container = node(entry_subs, 'container', 'Manage containers')


def _print_container_status(containers):
    header = ['service', 'id', 'state', 'age']
    data = []
    for c in containers:
        age = humanize_timedelta(c.age) if c.age is not None else ''
        data.append((c.service, c.id, c.state, age))
    print(format_table(header, sorted(data)))


@subcommand(container.subs,
            'ls', 'List containers',
            optional_app_id,
            arg("--service", action='append',
                help=('Filter by container services. May be repeated '
                      'to select multiple services.')),
            arg("--state", action='append',
                help=('Filter by container states. May be repeated '
                      'to select multiple states.')))
def container_ls(app_id=None, service=None, state=None):
    if app_id == 'current':
        app = ApplicationClient.connect_to_current()
    else:
        app = Client().connect(app_id)

    containers = app.containers(states=state, services=service)
    _print_container_status(containers)


@subcommand(container.subs,
            'kill', 'Kill a container',
            optional_app_id,
            container_id)
def container_kill(app_id=None, id=None):
    if id is None:
        fail("--id is required")

    if app_id == 'current':
        app = ApplicationClient.connect_to_current()
    else:
        app = Client().connect(app_id)
    app.kill(id)


@subcommand(container.subs,
            'scale', 'Scale a service to a requested number of containers',
            optional_app_id,
            arg('--service', '-s', required=True, help='Service name'),
            arg('--number', '-n', type=int, required=True,
                help='The requested number of instances'))
def container_scale(app_id, service=None, number=None):
    if service is None:
        fail("--service is required")
    if number is None:
        fail("--number is required")

    if app_id == 'current':
        app = ApplicationClient.connect_to_current()
    else:
        app = Client().connect(app_id)
    app.scale(service, number)


################
# INIT COMMAND #
################


@subcommand(entry_subs,
            'init', 'Initialize skein configuration',
            arg('--force', '-f', action='store_true',
                help='Overwrite existing configuration'))
def entry_init(force=False):
    Security.from_new_key_pair(force=force)


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    func = kwargs.pop('func')
    try:
        with context.set_cli():
            func(**kwargs)
    except KeyError as exc:
        fail("Key %s is not set" % str(exc))
    except SkeinError as exc:
        fail(str(exc))
    except DaemonNotRunningError as exc:
        fail("Skein daemon not found, please run `skein daemon start`")
    except Exception as exc:
        fail("Unexpected Error:\n%s" % traceback.format_exc(), prefix=False)


if __name__ == '__main__':
    main()
