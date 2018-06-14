from __future__ import absolute_import, print_function, division

import os
import json
import socket

from ipykernel.kernelapp import IPKernelApp

from ..core import ApplicationClient


class SkeinIPKernelApp(IPKernelApp):
    """A remote ipython kernel setup to run in a YARN container
    and publish its address to the skein application master.

    Examples
    --------
    Create an application specification:

    >>> from skein import Client, ApplicationSpec, Service, Resources
    >>> ipykernel = Service(resources=Resources(memory=2048, vcores=1),
    ...                     files={'environment': 'environment.tar.gz'},
    ...                     commands=['source environment/bin/activate',
    ...                               'python -m skein.recipes.ipython_kernel'])
    >>> spec = ApplicationSpec(name='ipython-kernel',
    ...                        services={'ipykernel': ipykernel})

    Launch the application:

    >>> client = Client()
    >>> app = client.submit(spec)

    Wait until application has started:

    >>> app_client = app.connect()

    Get the connection information:

    >>> import json
    >>> info = json.loads(app_client.kv.wait('ipykernel.info'))

    Use the connection info as you see fit for your application. When written
    to a file, this can be used with ``jupyter console --existing file.json``
    to connect to the remote kernel.
    """
    ip = '0.0.0.0'

    def initialize(self, argv=None):
        self.skein_app_client = ApplicationClient.from_current()
        super(SkeinIPKernelApp, self).initialize(argv=argv)

    def write_connection_file(self):
        super(SkeinIPKernelApp, self).write_connection_file()

        with open(self.abs_connection_file, 'r') as fil:
            data = json.loads(fil.read())

        if data['ip'] in ('', '0.0.0.0'):
            data['ip'] = socket.gethostbyname(socket.gethostname())

        self.skein_app_client.kv['ipykernel.info'] = json.dumps(data)


def start_ipython_kernel(argv=None):
    if os.environ.get('JUPYTER_RUNTIME_DIR') is None:
        if not os.path.exists('.jupyter'):
            os.mkdir('.jupyter')
        os.environ['JUPYTER_RUNTIME_DIR'] = './.jupyter'
    SkeinIPKernelApp.launch_instance(argv=argv)


if __name__ == '__main__':
    start_ipython_kernel()
