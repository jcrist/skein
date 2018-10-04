from __future__ import absolute_import, print_function, division

import random
from collections import namedtuple

from . import proto
from .compatibility import urlsplit
from .objects import Base
from .utils import cached_property


__all__ = ('ProxiedPage', 'WebUI')


class ProxiedPage(Base):
    """A page proxied by the Skein Web UI.

    Attributes
    ----------
    route : string
        The route used in the address to the proxied page.
    target : string
        The target address of the proxy.
    link_name : string or None
        The name of the linked page in the Web UI. If ``None``, the page is
        still proxied but isn't linked to in the Web UI.
    address : string
        The full proxied address to this page.
    """
    __slots__ = ('route', 'target', 'link_name', '_ui_address', '_proxy_prefix')

    def __init__(self, route, target, link_name, _ui_address, _proxy_prefix):
        self.route = route
        self.target = target
        self.link_name = link_name
        self._ui_address = _ui_address
        self._proxy_prefix = _proxy_prefix

    def __repr__(self):
        return 'ProxiedPage<route=%r>' % self.route

    @property
    def address(self):
        """The full proxied address to this page"""
        path = urlsplit(self.target).path
        suffix = '/' if not path or path.endswith('/') else ''
        return '%s%s/%s%s' % (self._ui_address[:-1], self._proxy_prefix,
                              self.route, suffix)


UIInfo = namedtuple('UIInfo', ['addresses', 'address', 'proxy_prefix'])


class WebUI(object):
    """The Skein WebUI."""
    def __init__(self, client):
        self._client = client

    @cached_property
    def _ui_info(self):
        resp = self._client._call('UiInfo', proto.UIInfoRequest())
        addresses = tuple(resp.ui_address)
        return UIInfo(addresses, random.choice(addresses), resp.proxy_prefix)

    @property
    def proxy_prefix(self):
        """The path between the Web UI address and the proxied pages."""
        return self._ui_info.proxy_prefix

    @property
    def addresses(self):
        """All known addresses of the Web UI.

        In most cases this will be a list of length 1. If YARN is running in
        high availability mode, there may be several available addresses. Use
        the ``address`` attribute to choose one at random."""
        return self._ui_info.addresses

    @property
    def address(self):
        """The address of the Web UI.

        If YARN is running in high availability mode, a single address is
        chosen at random on first access."""
        return self._ui_info.address

    def __repr__(self):
        return "WebUI<address=%r>" % self.address

    def add_page(self, route, target, link_name=None):
        """Add a new proxied page to the Web UI.

        Parameters
        ----------
        route : str
            The route for the proxied page. Must be a valid path *segment* in a
            url (e.g. ``foo`` in ``/foo/bar/baz``). Routes must be unique
            across the application.
        target : str
            The target address to be proxied to this page. Must be a valid url.
        link_name : str, optional
            If provided, will be the link text used in the Web UI. If not
            provided, the page will still be proxied, but no link will be added
            to the Web UI. Link names must be unique across the application.

        Returns
        -------
        ProxiedPage
        """
        req = proto.Proxy(route=route, target=target, link_name=link_name)
        self._client._call('AddProxy', req)
        return ProxiedPage(route, target, link_name, self.address,
                           self.proxy_prefix)

    def remove_page(self, route):
        """Remove a proxied page from the Web UI.

        Parameters
        ----------
        route : str
            The route for the proxied page. Must be a valid path *segment* in a
            url (e.g. ``foo`` in ``/foo/bar/baz``). Routes must be unique
            across the application.
        """
        req = proto.RemoveProxyRequest(route=route)
        self._client._call('RemoveProxy', req)

    def get_pages(self):
        """Get all registered pages.

        Returns
        -------
        pages : dict
            A ``dict`` of ``route`` to ``ProxiedPage`` for all pages.
        """
        resp = self._client._call('GetProxies', proto.GetProxiesRequest())
        return {i.route: ProxiedPage(i.route, i.target,
                                     i.link_name if i.link_name else None,
                                     self.address, self.proxy_prefix)
                for i in resp.proxy}
