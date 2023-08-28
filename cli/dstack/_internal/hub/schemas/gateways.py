from typing import List, Optional

from pydantic import BaseModel


class GatewayDelete(BaseModel):
    instance_names: List[str]


class GatewayCreate(BaseModel):
    backend: str
    region: str


class GatewayUpdate(BaseModel):
    wildcard_domain: Optional[str]
    default: Optional[bool]


class GatewayTestDomain(BaseModel):
    domain: str
