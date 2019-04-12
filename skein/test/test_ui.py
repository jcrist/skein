import pytest

import skein
from skein.exceptions import ConnectionError
from skein.test.conftest import run_application, wait_for_containers

requests = pytest.importorskip('requests')


simplehttp = skein.Service(resources=skein.Resources(memory=32, vcores=1),
                           script='/usr/bin/python -m SimpleHTTPServer 8888')
master = skein.Master(resources=skein.Resources(memory=256, vcores=1),
                      script="sleep infinity")
spec = skein.ApplicationSpec(name="test_webui",
                             queue="default",
                             master=master,
                             services={'simplehttp': simplehttp})

LOGIN = '?user.name=testuser'
PAGES = [('route-1', 'http://worker.example.com:8888', 'link 1'),
         ('route-2', 'http://worker.example.com:8888/tmp/', 'link 2'),
         ('route-3', 'http://worker.example.com:8888/container_tokens', 'link 3'),
         ('route-4', 'http://worker.example.com:8888', None)]


def get_page(address, **kwargs):
    # Set timeout on all requests so tests don't hang on error
    kwargs.setdefault('timeout', 5)
    return requests.get(address, **kwargs)


@pytest.fixture
def ui_pages(ui_test_app):
    pages = tuple(ui_test_app.ui.add_page(*args) for args in PAGES)
    try:
        yield pages
    finally:
        for page in ui_test_app.ui.get_pages():
            ui_test_app.ui.remove_page(page)


@pytest.fixture(scope="module")
def ui_test_app(client, has_kerberos_enabled):
    if has_kerberos_enabled:
        pytest.skip("Testing only implemented for simple authentication")
    with run_application(client, spec=spec) as app:
        # Wait for a single container
        wait_for_containers(app, 1, states=['RUNNING'])
        try:
            yield app
        finally:
            try:
                app.shutdown()
            except ConnectionError:
                client.kill_application(app.id)


@pytest.mark.parametrize('target, suffix', [
    ('http://example.com:8888', '/'),
    ('http://example.com:8888/', '/'),
    ('http://example.com:8888/foo', ''),
    ('http://example.com:8888/foo/', '/')
])
def test_webui_proxied_pages(target, suffix):
    base = 'http://master.example.com:8088/proxy/application_1526497750451_0001/'
    p = skein.ui.ProxiedPage('test', target, None, base, '/pages')
    assert p.address == base + 'pages/test' + suffix
    assert repr(p) == "ProxiedPage<route='test'>"


def test_webui_basics(ui_test_app):
    assert ui_test_app.ui.proxy_prefix == '/pages'

    assert len(ui_test_app.ui.addresses) == 1

    address = 'http://master.example.com:8088/proxy/%s/' % ui_test_app.id
    assert ui_test_app.ui.address == address

    assert repr(ui_test_app.ui) == "WebUI<address=%r>" % address


def test_webui_authentication(ui_test_app):
    # Fails without authentication
    resp = get_page(ui_test_app.ui.address)
    assert resp.status_code == 401

    # With authentication
    resp = get_page(ui_test_app.ui.address + LOGIN)
    assert resp.ok

    # Cookies set, no longer need authentication
    cookies = resp.cookies
    resp = get_page(ui_test_app.ui.address, cookies=cookies)
    assert resp.ok


def test_webui_home_and_overview(ui_test_app):
    # / and /overview are the same
    for suffix in ['', 'overview']:
        resp = get_page(ui_test_app.ui.address + suffix + LOGIN)
        assert resp.ok
        # Application Metrics
        assert "Memory:</b> 288.0 MiB" in resp.text
        assert "Cores:</b> 2" in resp.text
        assert "Progress:</b> N/A" in resp.text
        # Logs
        assert "testuser/application.master.log" in resp.text  # master
        assert "testuser/application.driver.log" in resp.text  # driver
        assert '/testuser/simplehttp.log' in resp.text  # service
        # Services
        assert 'simplehttp_0' in resp.text  # list of containers


def test_webui_kv(ui_test_app):
    ui_test_app.kv['foo'] = b'bar'
    ui_test_app.kv['bad'] = b'\255\255\255'  # non-unicode

    resp = get_page(ui_test_app.ui.address + 'kv' + LOGIN)
    assert resp.ok
    assert 'foo' in resp.text
    assert 'bar' in resp.text
    assert 'bad' in resp.text
    assert '&lt;binary value&gt;' in resp.text


def test_webui_resources_reachable(ui_test_app):
    resp = get_page(ui_test_app.ui.address + 'favicon.ico' + LOGIN)
    assert resp.ok


def test_webui_404(ui_test_app):
    resp = get_page(ui_test_app.ui.address + 'fake' + LOGIN)
    assert resp.status_code == 404


def test_webui_pages(ui_test_app, ui_pages):
    for page, (route, target, link_name) in zip(ui_pages, PAGES):
        assert page.route == route
        assert page.target == target
        assert page.link_name == link_name

    res = ui_test_app.ui.get_pages()
    sol = {p.route: p for p in ui_pages}
    assert res == sol

    ui_test_app.ui.remove_page(ui_pages[0].route)

    res = ui_test_app.ui.get_pages()
    sol = {p.route: p for p in ui_pages[1:]}
    assert res == sol


def test_webui_pages_errors(ui_test_app, ui_pages):
    route, target, link_name = PAGES[0]

    # Route already used
    with pytest.raises(ValueError):
        ui_test_app.ui.add_page(route, target)

    # Link name already used
    with pytest.raises(ValueError):
        ui_test_app.ui.add_page('new-link', target, link_name=link_name)

    # Invalid url
    with pytest.raises(ValueError):
        ui_test_app.ui.add_page('new-link', 'invalid url')

    # Page doesn't exist
    with pytest.raises(ValueError):
        ui_test_app.ui.remove_page('new-link')


def test_webui_pages_access(ui_test_app, ui_pages):
    root_page, dir_page, file_page, no_link_page = ui_pages

    # Get cookies
    resp = get_page(ui_test_app.ui.address + LOGIN)
    assert resp.ok
    cookies = resp.cookies

    resp = get_page(root_page.address, cookies=cookies)
    assert resp.ok

    resp = get_page(dir_page.address, cookies=cookies)
    assert resp.ok

    resp = get_page(file_page.address, cookies=cookies)
    assert resp.ok

    resp = get_page(root_page.address + "/tmp/", cookies=cookies)
    assert resp.ok

    resp = get_page(root_page.address + "/this-doesnt-exist", cookies=cookies)
    assert resp.status_code == 404

    # 404 after page is removed
    ui_test_app.ui.remove_page(dir_page.route)

    resp = get_page(dir_page.address, cookies=cookies)
    assert resp.status_code == 404


def test_webui_pages_dropdown(ui_test_app, ui_pages):
    root_page, dir_page, file_page, no_link_page = ui_pages

    resp = get_page(ui_test_app.ui.address + LOGIN)
    assert resp.ok

    assert 'href="./pages/%s/"' % root_page.route in resp.text
    assert root_page.link_name in resp.text
    assert 'href="./pages/%s/"' % dir_page.route in resp.text
    assert dir_page.link_name in resp.text
    assert 'href="./pages/%s"' % file_page.route in resp.text
    assert file_page.link_name in resp.text

    assert 'href-"./pages/%s"' % no_link_page.route not in resp.text

    for page in [root_page, dir_page, file_page]:
        ui_test_app.ui.remove_page(page.route)

    resp = get_page(ui_test_app.ui.address + LOGIN)
    assert resp.ok

    assert "No service pages available" in resp.text


def test_webui_no_acls(ui_test_app):
    # No ACLs mean anyone has access
    resp = get_page(ui_test_app.ui.address + "?user.name=baduser")
    assert resp.ok


@pytest.mark.parametrize('ui_users, checks', [
    (['*'], [('testuser', True), ('testuser2', True)]),
    ([], [('testuser', True), ('testuser2', False)]),
    (['testuser', 'testuser2'], [('testuser', True),
                                 ('testuser2', True),
                                 ('testuser3', False)])
])
def test_webui_acls(client, has_kerberos_enabled, ui_users, checks):
    if has_kerberos_enabled:
        pytest.skip("Testing only implemented for simple authentication")

    spec = skein.ApplicationSpec(name="test_webui_acls",
                                 queue="default",
                                 master=master,
                                 acls=skein.ACLs(enable=True, ui_users=ui_users))

    with run_application(client, spec=spec) as app:
        # Base url of web ui
        base = 'http://master.example.com:8088/proxy/%s' % app.id

        # Check proper subset of users allowed
        for user, ok in checks:
            resp = get_page(base + "?user.name=%s" % user)
            assert resp.ok == ok

        app.shutdown()
