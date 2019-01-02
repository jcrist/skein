from __future__ import absolute_import, print_function, division

import os
import json
import socket
from copy import copy

from notebook.notebookapp import NotebookApp, aliases
from traitlets import Unicode, Dict

from ..core import ApplicationClient


skein_notebook_aliases = dict(aliases)
skein_notebook_aliases.update({
    'notebook-info-key': 'SkeinNotebookApp.notebook_info_key'
})


class SkeinNotebookApp(NotebookApp):
    """A jupyter notebook server setup to run in a YARN container
    and publish its address to the skein application master.

    Examples
    --------
    Create an application specification:

    >>> from skein import Client, ApplicationSpec, Service, Resources
    >>> notebook = Service(resources=Resources(memory=2048, vcores=1),
    ...                    files={'environment': 'environment.tar.gz'},
    ...                    script=('source environment/bin/activate\n'
    ...                            'python -m skein.recipes.jupyter_notebook'))
    >>> spec = ApplicationSpec(name='jupyter-notebook',
    ...                        services={'notebook': notebook})

    Launch the application:

    >>> client = Client()
    >>> app = client.submit_and_connect(spec)

    Get the connection information:

    >>> import json
    >>> info = json.loads(app.kv.wait('jupyter.notebook.info'))

    Use the connection info as you see fit for your application. Information
    provided includes:

    - protocol (http or https)
    - host
    - port
    - base_url
    - token
    """
    name = 'python -m skein.recipes.jupyter_notebook'

    aliases = Dict(skein_notebook_aliases)

    ip = copy(NotebookApp.ip)
    ip.default_value = '0.0.0.0'

    open_browser = copy(NotebookApp.open_browser)
    open_browser.default_value = False

    notebook_info_key = Unicode("jupyter.notebook.info",
                                config=True,
                                help=("Skein key in which to store the "
                                      "server information"))

    def write_server_info_file(self):
        super(SkeinNotebookApp, self).write_server_info_file()

        data = {'protocol': 'https' if self.certfile else 'http',
                'host': socket.gethostname(),
                'port': self.port,
                'base_url': self.base_url,
                'token': self.token}

        app = ApplicationClient.from_current()
        app.kv[self.notebook_info_key] = json.dumps(data).encode()


def start_notebook_application(argv=None):
    for var in ['JUPYTER_RUNTIME_DIR', 'JUPYTER_DATA_DIR']:
        if os.environ.get(var) is None:
            if not os.path.exists('.jupyter'):
                os.mkdir('.jupyter')
            os.environ[var] = './.jupyter'
    SkeinNotebookApp.launch_instance(argv=argv)


if __name__ == '__main__':
    start_notebook_application()
