import enum
from typing import List, Optional

from pydantic import BaseModel


class BackendType(str, enum.Enum):
    """
    Attributes:
        AWS (BackendType): Amazon Web Services
        AZURE (BackendType): Microsoft Azure
        DSTACK (BackendType): dstack Cloud
        GCP (BackendType): Google Cloud Platform
        DATACRUNCH (BackendType): DataCrunch
        LAMBDA (BackendType): Lambda Cloud
        TENSORDOCK (BackendType): TensorDock Marketplace
        VASTAI (BackendType): Vast.ai Marketplace
    """

    AWS = "aws"
    AZURE = "azure"
    DSTACK = "dstack"
    GCP = "gcp"
    DATACRUNCH = "datacrunch"
    LAMBDA = "lambda"
    LOCAL = "local"
    NEBIUS = "nebius"
    TENSORDOCK = "tensordock"
    VASTAI = "vastai"


class ConfigElementValue(BaseModel):
    value: str
    label: str


class ConfigElement(BaseModel):
    selected: Optional[str] = None
    values: List[ConfigElementValue] = []


class ConfigMultiElement(BaseModel):
    selected: List[str] = []
    values: List[ConfigElementValue] = []
