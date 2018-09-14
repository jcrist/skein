from __future__ import absolute_import, print_function, division

import random
from collections import namedtuple

from . import proto
from .compatibility import urlsplit
from .utils import cached_property


__all__ = ('ProxiedPage', 'WebUI')


class ProxiedPage(object):
    """A page proxied by the Skein Web UI.

    Attributes
    ----------
    prefix : string
        The prefix used in the address to the proxied page.
    name : string or None
        The name of the linked page in the Web UI. If ``None``, the page is
        still proxied but isn't linked to in the Web UI.
    address : string
        The full proxied address to this page.
    target : string
        The target address of the proxy.
    """
    __slots__ = ('prefix', 'name', 'target', '_ui_address', '_proxy_prefix')

    def __init__(self, prefix, name, target, ui_address, proxy_prefix):
        self.prefix = prefix
        self.target = target
        self.name = name
        self._ui_address = ui_address
        self._proxy_prefix = proxy_prefix

    def __repr__(self):
        return 'ProxiedPage<prefix=%r>' % self.prefix

    @property
    def address(self):
        """The full proxied address to this page"""
        suffix = '' if urlsplit(self.target).path else '/'
        return '%s%s/%s%s' % (self._ui_address[:-1], self._proxy_prefix,
                              self.prefix, suffix)


UIInfo = namedtuple('UIInfo', ['ui_addresses', 'proxy_prefix'])


class WebUI(object):
    """The Skein WebUI."""
    def __init__(self, client):
        # The application client
        self._client = client

    @cached_property
    def _ui_info(self):
        resp = self._client._call('UiInfo', proto.UIInfoRequest())
        return UIInfo(tuple(resp.ui_address), resp.proxy_prefix)

    @property
    def proxy_prefix(self):
        return self._ui_info.proxy_prefix

    @property
    def addresses(self):
        return self._ui_info.ui_addresses

    @property
    def address(self):
        return random.choice(self._ui_info.ui_addresses)

    def __repr__(self):
        return "WebUI<address=%r>" % self.address

    def get_proxies(self):
        resp = self._client._call('GetProxies', proto.GetProxiesRequest())
        return [ProxiedPage(i.prefix, i.name if i.name else None, i.target,
                            self.address, self.proxy_prefix)
                for i in resp.proxy]

    def get_proxies_by_name(self):
        return {p.name: p for p in self.get_proxies() if p.name is not None}

    def get_proxies_by_prefix(self):
        return {p.prefix: p for p in self.get_proxies()}

    def add_proxy(self, prefix, target, name=None):
        req = proto.Proxy(prefix=prefix, target=target, name=name)
        self._client._call('AddProxy', req)

    def remove_proxy(self, prefix):
        req = proto.RemoveProxyRequest(prefix=prefix)
        self._client._call('RemoveProxy', req)
