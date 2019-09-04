import argparse
import os
import tempfile
from getpass import getuser

import skein
from skein.tornado import SimpleAuthMixin, KerberosAuthMixin, init_kerberos
from tornado import web, ioloop


# An argument parser for configuring the application
parser = argparse.ArgumentParser(
    description="A web service for submitting python scripts to YARN."
)
parser.add_argument(
    "--keytab", default=None,
    help=("The location of a keytab file. If not specified, 'simple' "
          "authentication will be used")
)
parser.add_argument(
    "--principal", default=None,
    help=("The principal to use if using kerberos. Defaults to the "
          "current user name.")
)
parser.add_argument(
    "--port", default=8888, type=int,
    help="The port to serve from. Default is 8888."
)

args = parser.parse_args()


if args.keytab:
    # Use the kerberos auth mixin, and initialize kerberos for HTTP auth
    AuthMixin = KerberosAuthMixin
    init_kerberos(keytab=args.keytab)
    # Also create the skein client with keytab and principal specified
    skein_client = skein.Client(
        keytab=args.keytab,
        principal=args.principal or getuser()
    )
else:
    # Use the simple auth mixin
    AuthMixin = SimpleAuthMixin
    skein_client = skein.Client()


# Read in the `index.html` source
thisdir = os.path.dirname(__file__)
with open(os.path.join(thisdir, "index.html")) as f:
    INDEX_HTML = f.read()


class LaunchHandler(AuthMixin, web.RequestHandler):
    @property
    def client(self):
        return self.settings['client']

    @web.authenticated
    def get(self):
        # Main page just displays the web form
        self.write(INDEX_HTML)

    @web.authenticated
    async def post(self):
        # Extract request parameters
        queue = self.get_argument('queue') or 'default'
        memory = float(self.get_argument('memory'))
        vcores = int(self.get_argument('vcores'))
        try:
            script = self.request.files['script'][0]
        except (IndexError, KeyError):
            raise web.HTTPError(400, reason="Missing script")

        # Check memory and vcores are in bounds
        if memory < 0.5 or memory > 8:
            raise web.HTTPError("0.5 <= memory <= 8 required")
        if vcores < 1 or vcores > 4:
            raise web.HTTPError("1 <= vcores <= 4 required")

        # We need to write the script temporarily to disk so Skein can upload it
        with tempfile.NamedTemporaryFile() as f:
            f.write(script['body'])
            f.file.flush()

            # ** Construct the application specification **
            # Note that we specify the user as user logged in to the web page.
            # If kerberos authentication was used, this would match the user's
            # principal.
            spec = skein.ApplicationSpec(
                name="pyscript",
                queue=queue,
                user=self.current_user,
                master=skein.Master(
                    resources=skein.Resources(
                        memory="%f GiB" % memory,
                        vcores=vcores
                    ),
                    files={script['filename']: f.name},
                    script="python %s" % script['filename']
                )
            )

            # Submit the application and get a report
            report = await ioloop.IOLoop.current().run_in_executor(
                None, self.submit_and_report, spec
            )

        # Redirect the user to the application's tracking url
        self.redirect(report.tracking_url)

    def submit_and_report(self, spec):
        app_id = self.client.submit(spec)
        report = self.client.application_report(app_id)
        return report


# Start the application and serve on the specified port
app = web.Application([("/", LaunchHandler)], client=skein_client)
app.listen(args.port)
ioloop.IOLoop.current().start()
