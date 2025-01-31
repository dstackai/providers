from typing import AsyncGenerator, AsyncIterator, Optional

import fastapi
import httpx
from fastapi import status
from starlette.requests import ClientDisconnect

from dstack._internal.proxy.lib.deps import ProxyAuthContext
from dstack._internal.proxy.lib.errors import ProxyError
from dstack._internal.proxy.lib.repo import BaseProxyRepo
from dstack._internal.proxy.lib.services.service_connection import (
    ServiceConnectionPool,
    get_service_replica_client,
)
from dstack._internal.utils.common import concat_url_path
from dstack._internal.utils.logging import get_logger

logger = get_logger(__name__)


async def proxy(
    project_name: str,
    run_name: str,
    path: str,
    request: fastapi.Request,
    auth: ProxyAuthContext,
    repo: BaseProxyRepo,
    service_conn_pool: ServiceConnectionPool,
) -> fastapi.responses.Response:
    if "Upgrade" in request.headers:
        raise ProxyError("Upgrading connections is not supported", status.HTTP_400_BAD_REQUEST)

    service = await repo.get_service(project_name, run_name)
    if service is None or not service.replicas:
        raise ProxyError(f"Service {project_name}/{run_name} not found", status.HTTP_404_NOT_FOUND)
    if service.auth:
        await auth.enforce()

    client = await get_service_replica_client(service, repo, service_conn_pool)

    if not service.strip_prefix:
        path = concat_url_path(request.scope.get("root_path", "/"), request.url.path)

    try:
        upstream_request = await build_upstream_request(request, path, client)
    except ClientDisconnect:
        logger.debug(
            "Downstream client disconnected before response was sent for %s %s",
            request.method,
            request.url,
        )
        raise ProxyError("Client disconnected")

    try:
        upstream_response = await client.send(upstream_request, stream=True)
    except httpx.RequestError as e:
        logger.debug(
            "Error requesting %s %s: %r", upstream_request.method, upstream_request.url, e
        )
        if isinstance(e, httpx.TimeoutException):
            raise ProxyError("Timed out requesting upstream", status.HTTP_504_GATEWAY_TIMEOUT)
        raise ProxyError("Error requesting upstream", status.HTTP_502_BAD_GATEWAY)

    return fastapi.responses.StreamingResponse(
        stream_response(upstream_response),
        status_code=upstream_response.status_code,
        headers=upstream_response.headers,
    )


async def stream_response(response: httpx.Response) -> AsyncGenerator[bytes, None]:
    try:
        async for chunk in response.aiter_raw():
            yield chunk
    except httpx.RequestError as e:
        logger.debug(
            "Error streaming response %s %s: %r", response.request.method, response.request.url, e
        )

    try:
        await response.aclose()
    except httpx.RequestError as e:
        logger.debug(
            "Error closing response %s %s: %r",
            response.request.method,
            response.request.url,
            e,
        )


async def build_upstream_request(
    downstream_request: fastapi.Request, path: str, client: httpx.AsyncClient
) -> httpx.Request:
    url = httpx.URL(path=path, query=downstream_request.url.query.encode("utf-8"))
    request_stream = await FastAPIToHttpxRequestStreamAdaptor(
        downstream_request.stream(), downstream_request.url
    ).get_stream()
    client.cookies.clear()  # the client is shared by all users, don't leak cookies

    # TODO(#2237): add common proxy headers
    return client.build_request(
        downstream_request.method, url, headers=downstream_request.headers, content=request_stream
    )


class FastAPIToHttpxRequestStreamAdaptor:
    """
    If a FastAPI request has no body, its stream consists of empty byte sequences (b"").
    This adaptor detects such streams and replaces them with None, otherwise httpx will
    considers them actual request bodies, which can lead to unexpected behavior.
    """

    def __init__(self, stream: AsyncIterator[bytes], url: fastapi.datastructures.URL) -> None:
        self._stream = stream
        self._url = url

    async def get_stream(self) -> Optional[AsyncGenerator[bytes, None]]:
        try:
            first_chunk = await self._stream.__anext__()
        except StopAsyncIteration:
            return None
        except ClientDisconnect:
            logger.debug("Downstream client disconnected when requesting %s", self._url)
            return None
        if first_chunk == b"":
            return None
        return self._adaptor(first_chunk)

    async def _adaptor(self, first_chunk: bytes) -> AsyncGenerator[bytes, None]:
        yield first_chunk
        try:
            async for chunk in self._stream:
                yield chunk
        except ClientDisconnect:
            logger.debug("Downstream client disconnected when requesting %s", self._url)
