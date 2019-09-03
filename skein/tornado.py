import logging
import os
from socket import gethostname

from .exceptions import FileNotFoundError


logger = logging.getLogger("tornado.application")


__all__ = ("KerberosAuthMixin", "init_kerberos", "SimpleAuthMixin")


# The global service name, set by `init_kerberos`
_SERVICE_NAME = None


def init_kerberos(keytab=None, service="HTTP", hostname=None):
    """Initialize Kerberos authentication settings.

    Should be called once on process startup, sets global settings.

    Parameters
    ----------
    keytab : str, optional
        Path to the keytab file. If not set, will check the ``KRB5_KTNAME``
        environment variable, and error otherwise.
    hostname : str, optional
        The hostname. If not provided, will be inferred from the system.
    service : str, optional
        The service name. The default of ``"HTTP"`` is usually sufficient.
    """
    # Ensure kerberos installed
    try:
        import kerberos  # noqa
    except ImportError as exc:
        raise type(exc)(
            "Failed to import `kerberos`. Please install `pykerberos` via:\n"
            "  conda install pykerberos\n"
            "or\n"
            "  pip install pykerberos"
        )

    # Ensure keytab set, set environment variable if needed
    if keytab is not None:
        keytab = os.path.abspath(os.path.expanduser(keytab))
        os.environ["KRB5_KTNAME"] = keytab
    elif "KRB5_KTNAME" in os.environ:
        keytab = os.environ["KRB5_KTNAME"]
    else:
        raise ValueError(
            "keytab not specified, and `KRB5_KTNAME` environment variable not set"
        )

    # Ensure keytab exists
    if not os.path.exists(keytab):
        raise FileNotFoundError("No keytab found at %s" % keytab)

    # Infer hostname if needed
    if hostname is None:
        hostname = gethostname()

    # Set global service name
    global _SERVICE_NAME
    _SERVICE_NAME = "%s@%s" % (service, hostname)


class KerberosAuthMixin(object):
    """A ``tornado.web.RequestHandler`` mixin for authenticating with Kerberos.

    Examples
    --------
    A simple hello-world application:

    .. code-block:: python

      from skein.tornado import init_kerberos, KerberosAuthMixin
      from tornado import web, ioloop

      # Create a handler with the KerberosAuthMixin as a base class
      class HelloHandler(KerberosAuthMixin, web.RequestHandler):
          @web.authenticated
          def get(self):
              self.write("Hello %s" % self.current_user)

      if __name__ == "__main__":
          # Initialize kerberos once per application
          init_kerberos(keytab="/path/to/my/service.keytab")

          # Serve web application
          app = web.Application([("/", HelloHandler)])
          app.listen(8888)
          ioloop.IOLoop.current().start()
    """

    def _raise_auth_error(self, err):
        from tornado import web
        logger.error("Kerberos failure: %s", err)
        raise web.HTTPError(500, "Error during kerberos authentication")

    def _raise_auth_required(self):
        from tornado import web
        self.set_status(401)
        self.write("Authentication required")
        self.set_header("WWW-Authenticate", "Negotiate")
        raise web.Finish()

    def get_current_user(self):
        """An implementation of ``get_current_user`` using kerberos.

        Calls out to ``get_current_user_kerberos``, override if you want to
        support multiple authentication methods.
        """
        return self.get_current_user_kerberos()

    def get_current_user_kerberos(self):
        """Authenticate the current user using kerberos.

        Returns
        -------
        user : str
            The current user name.
        """
        import kerberos

        auth_header = self.request.headers.get("Authorization")
        if not auth_header:
            return self._raise_auth_required()

        auth_type, auth_key = auth_header.split(" ", 1)
        if auth_type != "Negotiate":
            return self._raise_auth_required()

        if _SERVICE_NAME is None:
            raise RuntimeError(
                "Kerberos not initialized, must call `init_kerberos` before "
                "serving requests"
            )

        gss_context = None
        try:
            # Initialize kerberos context
            rc, gss_context = kerberos.authGSSServerInit(_SERVICE_NAME)

            # NOTE: Per the pykerberos documentation, the return code should be
            # checked after each step. However, after reading the pykerberos
            # code no method used here will ever return anything but
            # AUTH_GSS_COMPLETE (all other cases will raise an exception).  We
            # keep these checks in just in case pykerberos changes its behavior
            # to match its docs, but they likely never will trigger.

            if rc != kerberos.AUTH_GSS_COMPLETE:
                return self._raise_auth_error(
                    "GSS server init failed, return code = %r" % rc
                )

            # Challenge step
            rc = kerberos.authGSSServerStep(gss_context, auth_key)
            if rc != kerberos.AUTH_GSS_COMPLETE:
                return self._raise_auth_error(
                    "GSS server step failed, return code = %r" % rc
                )
            gss_key = kerberos.authGSSServerResponse(gss_context)

            # Retrieve user name
            fulluser = kerberos.authGSSServerUserName(gss_context)
            user = fulluser.split("@", 1)[0]

            # Complete the protocol by responding with the Negotiate header
            self.set_header("WWW-Authenticate", "Negotiate %s" % gss_key)
        except kerberos.GSSError as err:
            return self._raise_auth_error(err)
        finally:
            if gss_context is not None:
                kerberos.authGSSServerClean(gss_context)

        return user


class SimpleAuthMixin(object):
    """A ``tornado.web.RequestHandler`` mixin for authenticating using Hadoop's
    "simple" protocol.

    In Hadoop, "simple" authentication uses a URL query parameter to specify
    the user, and isn't secure at all. This mixin class exists for parity with
    Hadoop, but kerberos authentication is advised instead.

    Examples
    --------
    A simple hello-world application:

    .. code-block:: python

      from skein.tornado import SimpleAuthMixin
      from tornado import web, ioloop

      # Create a handler with the SimpleAuthMixin as a base class
      class HelloHandler(SimpleAuthMixin, web.RequestHandler):
          @web.authenticated
          def get(self):
              self.write("Hello %s" % self.current_user)

      if __name__ == "__main__":
          # Serve web application
          app = web.Application([("/", HelloHandler)])
          app.listen(8888)
          ioloop.IOLoop.current().start()
    """
    def _raise_auth_required(self):
        from tornado import web
        self.set_status(401)
        self.write("Authentication required")
        self.set_header("WWW-Authenticate", "PseudoAuth")
        raise web.Finish()

    def get_current_user(self):
        """An implementation of ``get_current_user`` using simple auth.

        Calls out to ``get_current_user_simple``, override if you want to
        support multiple authentication methods.
        """
        return self.get_current_user_simple()

    def get_current_user_simple(self):
        """Authenticate the current user using simple auth.

        Returns
        -------
        user : str
            The current user name.
        """
        user = self.get_argument("user.name", "")
        if not user:
            self._raise_auth_required()
        return user
