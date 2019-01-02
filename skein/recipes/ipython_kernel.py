from __future__ import absolute_import, print_function, division

import os
import json
import socket
from copy import copy

from ipykernel.kernelapp import IPKernelApp, kernel_aliases
from traitlets import Unicode, Dict

from ..core import ApplicationClient


skein_kernel_aliases = dict(kernel_aliases)
skein_kernel_aliases.update({
    'kernel-info-key': 'SkeinIPKernelApp.kernel_info_key'
})


class SkeinIPKernelApp(IPKernelApp):
    """A remote ipython kernel setup to run in a YARN container
    and publish its address to the skein application master.

    Examples
    --------
    Create an application specification:

    >>> from skein import Client, ApplicationSpec, Service, Resources
    >>> ipykernel = Service(resources=Resources(memory=2048, vcores=1),
    ...                     files={'environment': 'environment.tar.gz'},
    ...                     script=('source environment/bin/activate\n'
    ...                             'python -m skein.recipes.ipython_kernel'))
    >>> spec = ApplicationSpec(name='ipython-kernel',
    ...                        services={'ipykernel': ipykernel})

    Launch the application:

    >>> client = Client()
    >>> app = client.submit_and_connect(spec)

    Get the connection information:

    >>> import json
    >>> info = json.loads(app.kv.wait('ipython.kernel.info'))

    Use the connection info as you see fit for your application. When written
    to a file, this can be used with ``jupyter console --existing file.json``
    to connect to the remote kernel.
    """
    name = 'python -m skein.recipes.ipython_kernel'

    aliases = Dict(skein_kernel_aliases)

    ip = copy(IPKernelApp.ip)
    ip.default_value = '0.0.0.0'

    kernel_info_key = Unicode("ipython.kernel.info",
                              config=True,
                              help=("Skein key in which to store the "
                                    "connection information"))

    def write_connection_file(self):
        super(SkeinIPKernelApp, self).write_connection_file()

        with open(self.abs_connection_file, 'r') as fil:
            data = json.loads(fil.read())

        if data['ip'] in ('', '0.0.0.0'):
            data['ip'] = socket.gethostbyname(socket.gethostname())

        app = ApplicationClient.from_current()
        app.kv[self.kernel_info_key] = json.dumps(data).encode()


def start_ipython_kernel(argv=None):
    if os.environ.get('JUPYTER_RUNTIME_DIR') is None:
        if not os.path.exists('.jupyter'):
            os.mkdir('.jupyter')
        os.environ['JUPYTER_RUNTIME_DIR'] = './.jupyter'
    SkeinIPKernelApp.launch_instance(argv=argv)


if __name__ == '__main__':
    start_ipython_kernel()
