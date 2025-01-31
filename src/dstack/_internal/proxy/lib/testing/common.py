from typing import AsyncGenerator, Optional

from dstack._internal.proxy.lib.auth import BaseProxyAuthProvider
from dstack._internal.proxy.lib.deps import ProxyDependencyInjector
from dstack._internal.proxy.lib.models import Project, Replica, Service
from dstack._internal.proxy.lib.repo import BaseProxyRepo


class ProxyTestDependencyInjector(ProxyDependencyInjector):
    def __init__(self, repo: BaseProxyRepo, auth: BaseProxyAuthProvider) -> None:
        super().__init__()
        self._repo = repo
        self._auth = auth

    async def get_repo(self) -> AsyncGenerator[BaseProxyRepo, None]:
        yield self._repo

    async def get_auth_provider(self) -> AsyncGenerator[BaseProxyAuthProvider, None]:
        yield self._auth


def make_project(name: str) -> Project:
    return Project(name=name, ssh_private_key="secret")


def make_service(
    project_name: str,
    run_name: str,
    domain: Optional[str] = None,
    https: Optional[bool] = None,
    auth: bool = False,
    strip_prefix: bool = True,
) -> Service:
    return Service(
        project_name=project_name,
        run_name=run_name,
        domain=domain,
        https=https,
        auth=auth,
        client_max_body_size=2**20,
        strip_prefix=strip_prefix,
        replicas=(
            Replica(
                id="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
                app_port=80,
                ssh_destination="ubuntu@server",
                ssh_port=22,
                ssh_proxy=None,
            ),
        ),
    )
