"""Things stored in BaseProxyRepo implementations."""

from datetime import datetime
from typing import Iterable, Literal, Optional, Union

from pydantic import BaseModel, Field
from typing_extensions import Annotated

from dstack._internal.core.models.instances import SSHConnectionParams
from dstack._internal.proxy.lib.errors import UnexpectedProxyError


# Models should be immutable so that they can be stored in memory and safely shared by
# coroutines without copying on every read operation.
class ImmutableModel(BaseModel):
    class Config:
        frozen = True


class Replica(ImmutableModel):
    id: str
    app_port: int
    ssh_destination: str
    ssh_port: int
    ssh_proxy: Optional[SSHConnectionParams]


class Service(ImmutableModel):
    project_name: str
    run_name: str
    domain: Optional[str]  # only used on gateways
    https: Optional[bool]  # only used on gateways
    auth: bool
    client_max_body_size: int  # only enforced on gateways
    strip_prefix: bool = True  # only used in-server
    replicas: tuple[Replica, ...]

    @property
    def domain_safe(self) -> str:
        if self.domain is None:
            raise UnexpectedProxyError(f"domain unexpectedly missing for service {self.fmt()}")
        return self.domain

    @property
    def https_safe(self) -> bool:
        if self.https is None:
            raise UnexpectedProxyError(f"https unexpectedly missing for service {self.fmt()}")
        return self.https

    def with_replicas(self, new_replicas: Iterable[Replica]) -> "Service":
        return Service(
            project_name=self.project_name,
            run_name=self.run_name,
            domain=self.domain,
            https=self.https,
            auth=self.auth,
            client_max_body_size=self.client_max_body_size,
            replicas=tuple(new_replicas),
        )

    def find_replica(self, replica_id: str) -> Optional[Replica]:
        for replica in self.replicas:
            if replica.id == replica_id:
                return replica
        return None

    def fmt(self) -> str:
        return f"{self.project_name}/{self.run_name}"


class Project(ImmutableModel):
    name: str
    ssh_private_key: str


class TGIChatModelFormat(ImmutableModel):
    format: Literal["tgi"] = "tgi"
    chat_template: str
    eos_token: str


class OpenAIChatModelFormat(ImmutableModel):
    format: Literal["openai"] = "openai"
    prefix: str


AnyModelFormat = Union[TGIChatModelFormat, OpenAIChatModelFormat]


class ChatModel(ImmutableModel):
    project_name: str
    name: str
    created_at: datetime
    run_name: str
    format_spec: Annotated[AnyModelFormat, Field(discriminator="format")]
