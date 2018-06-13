from __future__ import absolute_import, print_function, division

import os
import json
import socket

from notebook.notebookapp import NotebookApp

from ..core import ApplicationClient


class SkeinNotebookApp(NotebookApp):
    """A jupyter notebook server setup to run in a YARN container
    and publish its address to the skein application master.

    Examples
    --------
    Create an application specification:

    >>> from skein import Client, ApplicationSpec, Service, Resources
    >>> notebook = Service(resources=Resources(memory=2048, vcores=1),
    ...                    files={'environment': 'environment.tar.gz'},
    ...                    commands=['source environment/bin/activate',
    ...                              'python -m skein.recipes.jupyter_notebook'])
    >>> spec = ApplicationSpec(name='jupyter-notebook',
    ...                        services={'notebook': notebook})

    Launch the application:

    >>> client = Client()
    >>> app = client.submit(spec)

    Wait until application has started:

    >>> app_client = app.connect()

    Get the connection information:

    >>> import json
    >>> info = json.loads(app_client.kv.wait('notebook.info'))

    Use the connection info as you see fit for your application. Information
    provided includes:

    - protocol (http or https)
    - host
    - port
    - base_url
    - token
    """
    ip = '0.0.0.0'
    open_browser = False

    def initialize(self, argv=None):
        super(SkeinNotebookApp, self).initialize(argv=argv)
        self.skein_app_client = ApplicationClient.from_current()

    def write_server_info_file(self):
        super(SkeinNotebookApp, self).write_server_info_file()

        data = {'protocol': 'https' if self.certfile else 'http',
                'host': socket.gethostname(),
                'port': self.port,
                'base_url': self.base_url,
                'token': self.token}

        self.skein_app_client.kv['notebook.info'] = json.dumps(data)


def start_notebook_application(argv=None):
    if os.environ.get('JUPYTER_RUNTIME_DIR') is None:
        if not os.path.exists('.jupyter'):
            os.mkdir('.jupyter')
        os.environ['JUPYTER_RUNTIME_DIR'] = './.jupyter'
    SkeinNotebookApp.launch_instance(argv=argv)


if __name__ == '__main__':
    start_notebook_application()
