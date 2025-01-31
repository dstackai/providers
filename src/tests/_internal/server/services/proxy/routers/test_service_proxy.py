from typing import Generator, Optional, Tuple
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

from dstack._internal.proxy.gateway.repo.repo import GatewayProxyRepo
from dstack._internal.proxy.lib.auth import BaseProxyAuthProvider
from dstack._internal.proxy.lib.repo import BaseProxyRepo
from dstack._internal.proxy.lib.services.service_connection import ServiceClient
from dstack._internal.proxy.lib.testing.auth import ProxyTestAuthProvider
from dstack._internal.proxy.lib.testing.common import (
    ProxyTestDependencyInjector,
    make_project,
    make_service,
)
from dstack._internal.server.services.proxy.routers.service_proxy import router

MOCK_REPLICA_CLIENT_TIMEOUT = 8

# Using GatewayProxyRepo for tests because it is easier to populate than ServerProxyRepo
ProxyTestRepo = GatewayProxyRepo


@pytest.fixture
def mock_replica_client_httpbin(httpbin) -> Generator[None, None, None]:
    """Mocks deployed services. Replaces them with httpbin"""

    with patch(
        "dstack._internal.proxy.lib.services.service_connection.ServiceConnectionPool.get_or_add"
    ) as add_connection_mock:
        add_connection_mock.return_value.client.return_value = ServiceClient(
            base_url=httpbin.url, timeout=MOCK_REPLICA_CLIENT_TIMEOUT
        )
        yield


@pytest.fixture
def mock_replica_client_path_reporter() -> Generator[None, None, None]:
    """Mocks deployed services. Replaces them with an app that returns the requested path"""

    app = FastAPI()
    app.get("{path:path}")(lambda path: PlainTextResponse(path))
    client = ServiceClient(base_url="http://test/", transport=httpx.ASGITransport(app))
    with patch(
        "dstack._internal.proxy.lib.services.service_connection.ServiceConnectionPool.get_or_add"
    ) as add_connection_mock:
        add_connection_mock.return_value.client.return_value = client
        yield


def make_app(
    repo: BaseProxyRepo, auth: BaseProxyAuthProvider = ProxyTestAuthProvider()
) -> FastAPI:
    app = FastAPI()
    app.state.proxy_dependency_injector = ProxyTestDependencyInjector(repo=repo, auth=auth)
    app.include_router(router, prefix="/proxy/services")
    return app


def make_client(app: FastAPI) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app))


def make_app_client(
    repo: BaseProxyRepo = GatewayProxyRepo(), auth: BaseProxyAuthProvider = ProxyTestAuthProvider()
) -> Tuple[FastAPI, httpx.AsyncClient]:
    app = make_app(repo, auth)
    client = make_client(app)
    return app, client


@pytest.mark.asyncio
@pytest.mark.parametrize("method", ["get", "post", "put", "patch", "delete"])
async def test_proxy(mock_replica_client_httpbin, method: str) -> None:
    methods_without_body = "get", "delete"
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    _, client = make_app_client(repo)
    req_body = "." * 20 * 2**20 if method not in methods_without_body else None
    resp = await client.request(
        method,
        f"http://test-host:8888/proxy/services/test-proj/httpbin/{method}?a=b&c=",
        headers={"User-Agent": "test-ua", "Connection": "keep-alive"},
        content=req_body,
    )
    assert resp.status_code == 200
    assert resp.headers["server"].startswith("Pytest-HTTPBIN")
    resp_body = resp.json()
    assert resp_body["url"] == f"http://test-host:8888/{method}?a=b&c="
    assert resp_body["args"] == {"a": "b", "c": ""}
    assert resp_body["headers"]["Host"] == "test-host:8888"
    assert resp_body["headers"]["User-Agent"] == "test-ua"
    assert resp_body["headers"]["Connection"] == "keep-alive"
    if method not in methods_without_body:
        assert resp_body["data"] == req_body


@pytest.mark.asyncio
async def test_proxy_method_head(mock_replica_client_httpbin) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    _, client = make_app_client(repo)
    url = "http://test-host/proxy/services/test-proj/httpbin/"
    get_resp = await client.get(url)
    head_resp = await client.head(url)
    assert get_resp.status_code == head_resp.status_code == 200
    assert head_resp.headers["Content-Length"] == get_resp.headers["Content-Length"]
    assert int(head_resp.headers["Content-Length"]) > 0
    assert head_resp.content == b""


@pytest.mark.asyncio
async def test_proxy_method_options(mock_replica_client_httpbin) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    _, client = make_app_client(repo)
    resp = await client.options("http://test-host/proxy/services/test-proj/httpbin/get")
    assert resp.status_code == 200
    assert set(resp.headers["Allow"].split(", ")) == {"HEAD", "GET", "OPTIONS"}
    assert resp.content == b""


@pytest.mark.asyncio
@pytest.mark.parametrize("code", [204, 304, 418, 503])
async def test_proxy_status_codes(mock_replica_client_httpbin, code: int) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    _, client = make_app_client(repo)
    resp = await client.get(f"http://test-host/proxy/services/test-proj/httpbin/status/{code}")
    assert resp.status_code == code


@pytest.mark.asyncio
async def test_proxy_not_leaks_cookies(mock_replica_client_httpbin) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    app = make_app(repo)
    client1 = make_client(app)
    client2 = make_client(app)
    cookies_url = "http://test-host/proxy/services/test-proj/httpbin/cookies"
    await client1.get(cookies_url + "/set?a=1")
    await client1.get(cookies_url + "/set?b=2")
    await client2.get(cookies_url + "/set?a=3")
    resp1 = await client1.get(cookies_url)
    resp2 = await client2.get(cookies_url)
    assert resp1.json()["cookies"] == {"a": "1", "b": "2"}
    assert resp2.json()["cookies"] == {"a": "3"}


@pytest.mark.asyncio
async def test_proxy_gateway_timeout(mock_replica_client_httpbin) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    _, client = make_app_client(repo)
    assert MOCK_REPLICA_CLIENT_TIMEOUT < 10
    resp = await client.get("http://test-host/proxy/services/test-proj/httpbin/delay/10")
    assert resp.status_code == 504
    assert resp.json()["detail"] == "Timed out requesting upstream"


@pytest.mark.asyncio
async def test_proxy_run_not_found(mock_replica_client_httpbin) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "test-run"))
    _, client = make_app_client(repo)
    resp = await client.get("http://test-host/proxy/services/test-proj/unknown/")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Service test-proj/unknown not found"


@pytest.mark.asyncio
async def test_proxy_project_not_found(mock_replica_client_httpbin) -> None:
    _, client = make_app_client(ProxyTestRepo())
    resp = await client.get("http://test-host/proxy/services/unknown/test-run/")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Service unknown/test-run not found"


@pytest.mark.asyncio
async def test_redirect_to_service_root(mock_replica_client_httpbin) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin"))
    _, client = make_app_client(repo)
    url = "http://test-host/proxy/services/test-proj/httpbin"
    resp = await client.get(url, follow_redirects=False)
    assert resp.status_code == 308
    assert resp.headers["Location"] == url + "/"
    resp = await client.get(url, follow_redirects=True)
    assert resp.status_code == 200
    assert resp.request.url == url + "/"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("token", "status"), [("correct-token", 200), ("incorrect-token", 403), ("", 403), (None, 403)]
)
async def test_auth(mock_replica_client_httpbin, token: Optional[str], status: int) -> None:
    auth = ProxyTestAuthProvider({"test-proj": {"correct-token"}})
    repo = ProxyTestRepo()
    await repo.set_project(make_project("test-proj"))
    await repo.set_service(make_service("test-proj", "httpbin", auth=True))
    _, client = make_app_client(repo, auth)
    headers = None
    if token is not None:
        headers = {"Authorization": f"Bearer {token}"}
    url = "http://test-host/proxy/services/test-proj/httpbin/"
    resp = await client.get(url, headers=headers)
    assert resp.status_code == status


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("strip", "downstream_path", "upstream_path"),
    [
        (True, "/proxy/services/my-proj/my-run/", "/"),
        (True, "/proxy/services/my-proj/my-run/a/b", "/a/b"),
        (False, "/proxy/services/my-proj/my-run/", "/proxy/services/my-proj/my-run/"),
        (False, "/proxy/services/my-proj/my-run/a/b", "/proxy/services/my-proj/my-run/a/b"),
    ],
)
async def test_strip_prefix(
    mock_replica_client_path_reporter, strip: bool, downstream_path: str, upstream_path: str
) -> None:
    repo = ProxyTestRepo()
    await repo.set_project(make_project("my-proj"))
    await repo.set_service(make_service("my-proj", "my-run", strip_prefix=strip))
    _, client = make_app_client(repo)
    resp = await client.get(f"http://test-host{downstream_path}")
    assert resp.status_code == 200
    assert resp.text == upstream_path
