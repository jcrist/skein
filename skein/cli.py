from __future__ import print_function, division, absolute_import

import argparse

from . import __version__


def add_help(parser):
    parser.add_argument("--help", "-h", action='help',
                        help="Show this help message then exit")


entry = argparse.ArgumentParser(prog="skein",
                                description="Interact with YARN jobs via skein",
                                add_help=False)
add_help(entry)
entry.add_argument("--version", action='version',
                   version='%(prog)s ' + __version__,
                   help="Show version then exit")
subparsers = entry.add_subparsers(metavar='command')


def arg(*args, **kwargs):
    return (args, kwargs)


def subcommand(name, help, *args):
    def _(func):
        sub = subparsers.add_parser(name, help=help, description=help,
                                    add_help=False)
        sub.set_defaults(func=func)
        for arg in args:
            sub.add_argument(*arg[0], **arg[1])
        add_help(sub)
        return func
    return _


verbose = arg("--verbose", "-v", action="count", default=0)


@subcommand('list', 'List all Skein jobs',
            verbose)
def do_list(verbose):
    pass


@subcommand('start', 'Start a Skein Job',
            arg('spec', type=str, help='the specification file'),
            verbose)
def do_start(spec, verbose):
    pass


@subcommand('stop', 'Stop a Skein Job',
            arg('id', type=str, help='the application id'),
            verbose)
def do_stop(id, verbose):
    pass


@subcommand('kill', 'Kill a Skein Job',
            arg('id', type=str, help='the application id'),
            verbose)
def do_kill(id, verbose):
    pass


def main(args=None):
    kwargs = vars(entry.parse_args(args=args))
    kwargs.pop('func')(**kwargs)


if __name__ == '__main__':
    main()
