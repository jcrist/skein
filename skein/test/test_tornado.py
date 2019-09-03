import os
import sys

import pytest
pytest.importorskip("kerberos")
pytest.importorskip("tornado")
requests_kerberos = pytest.importorskip("requests_kerberos")

import requests
from tornado import ioloop, web

import skein.tornado
from skein.tornado import init_kerberos, KerberosMixin


def test_tornado_init_kerberos(has_kerberos_enabled, http_keytab, monkeypatch):
    # Kerberos not installed
    with monkeypatch.context() as m:
        m.setitem(sys.modules, "kerberos", None)
        with pytest.raises(ImportError):
            init_kerberos()

    # Keytab not specified
    with monkeypatch.context() as m:
        m.delenv("KRB5_KTNAME", raising=False)
        with pytest.raises(ValueError):
            init_kerberos()

    # Keytab doesn't exist
    with monkeypatch.context() as m:
        m.delenv("KRB5_KTNAME", raising=False)
        with pytest.raises(FileNotFoundError):
            init_kerberos(keytab="/path/to/missing/file")

    # Keytab specified via environment variable
    with monkeypatch.context() as m:
        m.setenv("KRB5_KTNAME", http_keytab)
        init_kerberos()
        assert os.environ["KRB5_KTNAME"] == http_keytab

    # Keytab specified manually
    with monkeypatch.context() as m:
        m.delenv("KRB5_KTNAME", raising=False)
        init_kerberos(keytab=http_keytab, hostname="foo.bar.baz")
        assert os.environ["KRB5_KTNAME"] == http_keytab
        # Service name set appropriately
        assert skein.tornado._SERVICE_NAME == "HTTP@foo.bar.baz"


class HelloHandler(KerberosMixin, web.RequestHandler):
    @web.authenticated
    def get(self):
        self.write("Hello %s" % self.current_user)


def test_tornado_kerberos(has_kerberos_enabled, http_keytab):
    init_kerberos(keytab=http_keytab, hostname="master.example.com")

    # Serve web application
    app = web.Application([("/", HelloHandler)])
    app.listen(8888, "edge.example.com")

    async def test():
        auth = requests_kerberos.HTTPKerberosAuth(hostname_override="master.example.com")
        return await ioloop.IOLoop.current().run_in_executor(
            None, lambda: requests.get("http://edge.example.com:8888", auth=auth)
        )

    loop = ioloop.IOLoop.current()
    resp = loop.run_sync(test)
    assert resp.text == "Hello testuser"
