import re
from enum import Enum
from typing import Any, List, Optional, Union

from pydantic import Field, ValidationError, conint, constr, root_validator, validator
from typing_extensions import Annotated, Literal

from dstack._internal.core.errors import ConfigurationError
from dstack._internal.core.models.common import CoreModel, Duration, RegistryAuth
from dstack._internal.core.models.envs import Env
from dstack._internal.core.models.fleets import FleetConfiguration
from dstack._internal.core.models.gateways import GatewayConfiguration
from dstack._internal.core.models.profiles import ProfileParams
from dstack._internal.core.models.repos.base import Repo
from dstack._internal.core.models.repos.virtual import VirtualRepo
from dstack._internal.core.models.resources import Range, ResourcesSpec
from dstack._internal.core.models.services import AnyModel, OpenAIChatModel
from dstack._internal.core.models.unix import UnixUser
from dstack._internal.core.models.volumes import MountPoint, VolumeConfiguration, parse_mount_point

CommandsList = List[str]
ValidPort = conint(gt=0, le=65536)
SERVICE_HTTPS_DEFAULT = True
STRIP_PREFIX_DEFAULT = True


class RunConfigurationType(str, Enum):
    DEV_ENVIRONMENT = "dev-environment"
    TASK = "task"
    SERVICE = "service"


class PythonVersion(str, Enum):
    PY38 = "3.8"  # TODO(0.19 or earlier): drop 3.8, stop building Docker images with 3.8
    PY39 = "3.9"
    PY310 = "3.10"
    PY311 = "3.11"
    PY312 = "3.12"
    PY313 = "3.13"


class PortMapping(CoreModel):
    local_port: Optional[ValidPort] = None
    container_port: ValidPort

    @classmethod
    def parse(cls, v: str) -> "PortMapping":
        """
        Possible values:
          - 8080
          - 80:8080
          - *:8080
        """
        r = re.search(r"^(?:(\d+|\*):)?(\d+)?$", v)
        if not r:
            raise ValueError(v)
        local_port, container_port = r.groups()
        if local_port is None:  # identity mapping by default
            local_port = int(container_port)
        elif local_port == "*":
            local_port = None
        else:
            local_port = int(local_port)
        return PortMapping(local_port=local_port, container_port=int(container_port))


class ScalingSpec(CoreModel):
    metric: Annotated[
        Literal["rps"],
        Field(
            description="The target metric to track. Currently, the only supported value is `rps` "
            "(meaning requests per second)"
        ),
    ]
    target: Annotated[
        float,
        Field(
            description="The target value of the metric. "
            "The number of replicas is calculated based on this number and automatically adjusts "
            "(scales up or down) as this metric changes"
        ),
    ]
    scale_up_delay: Annotated[
        Duration, Field(description="The delay in seconds before scaling up")
    ] = Duration.parse("5m")
    scale_down_delay: Annotated[
        Duration, Field(description="The delay in seconds before scaling down")
    ] = Duration.parse("10m")


class BaseRunConfiguration(CoreModel):
    type: Literal["none"]
    name: Annotated[
        Optional[str],
        Field(description="The run name. If not specified, a random name is generated"),
    ] = None
    image: Annotated[Optional[str], Field(description="The name of the Docker image to run")]
    user: Annotated[
        Optional[str],
        Field(
            description=(
                "The user inside the container, `user_name_or_id[:group_name_or_id]`"
                " (e.g., `ubuntu`, `1000:1000`). Defaults to the default `image` user"
            )
        ),
    ] = None
    privileged: Annotated[bool, Field(description="Run the container in privileged mode")] = False
    entrypoint: Annotated[Optional[str], Field(description="The Docker entrypoint")]
    working_dir: Annotated[
        Optional[str],
        Field(
            description=(
                "The path to the working directory inside the container."
                " It's specified relative to the repository directory (`/workflow`) and should be inside it."
                ' Defaults to `"."` '
            )
        ),
    ] = None
    # deprecated since 0.18.31; has no effect
    home_dir: str = "/root"
    registry_auth: Annotated[
        Optional[RegistryAuth], Field(description="Credentials for pulling a private Docker image")
    ]
    python: Annotated[
        Optional[PythonVersion],
        Field(description="The major version of Python. Mutually exclusive with `image`"),
    ]
    nvcc: Annotated[
        Optional[bool],
        Field(
            description="Use image with NVIDIA CUDA Compiler (NVCC) included. Mutually exclusive with `image`"
        ),
    ]
    env: Annotated[
        Env,
        Field(description="The mapping or the list of environment variables"),
    ] = Env()
    # deprecated since 0.18.31; task, service -- no effect; dev-environment -- executed right before `init`
    setup: CommandsList = []
    resources: Annotated[
        ResourcesSpec, Field(description="The resources requirements to run the configuration")
    ] = ResourcesSpec()
    volumes: Annotated[
        List[Union[MountPoint, str]], Field(description="The volumes mount points")
    ] = []

    @validator("python", pre=True, always=True)
    def convert_python(cls, v, values) -> Optional[PythonVersion]:
        if v is not None and values.get("image"):
            raise KeyError("`image` and `python` are mutually exclusive fields")
        if isinstance(v, float):
            v = str(v)
            if v == "3.1":
                v = "3.10"
        if isinstance(v, str):
            return PythonVersion(v)
        return v

    @validator("volumes", each_item=True)
    def convert_volumes(cls, v) -> MountPoint:
        if isinstance(v, str):
            return parse_mount_point(v)
        return v

    @validator("user")
    def validate_user(cls, v) -> Optional[str]:
        if v is None:
            return None
        UnixUser.parse(v)
        return v

    def get_repo(self) -> Repo:
        return VirtualRepo()


class BaseRunConfigurationWithPorts(BaseRunConfiguration):
    ports: Annotated[
        List[Union[ValidPort, constr(regex=r"^(?:[0-9]+|\*):[0-9]+$"), PortMapping]],
        Field(description="Port numbers/mapping to expose"),
    ] = []

    @validator("ports", each_item=True)
    def convert_ports(cls, v) -> PortMapping:
        if isinstance(v, int):
            return PortMapping(local_port=v, container_port=v)
        elif isinstance(v, str):
            return PortMapping.parse(v)
        return v


class BaseRunConfigurationWithCommands(BaseRunConfiguration):
    commands: Annotated[CommandsList, Field(description="The bash commands to run")] = []

    @root_validator
    def check_image_or_commands_present(cls, values):
        if not values.get("commands") and not values.get("image"):
            raise ValueError("Either `commands` or `image` must be set")
        return values


class DevEnvironmentConfigurationParams(CoreModel):
    ide: Annotated[Literal["vscode"], Field(description="The IDE to run")]
    version: Annotated[Optional[str], Field(description="The version of the IDE")]
    init: Annotated[CommandsList, Field(description="The bash commands to run on startup")] = []


class DevEnvironmentConfiguration(
    ProfileParams, BaseRunConfigurationWithPorts, DevEnvironmentConfigurationParams
):
    type: Literal["dev-environment"] = "dev-environment"


class TaskConfigurationParams(CoreModel):
    nodes: Annotated[int, Field(description="Number of nodes", ge=1)] = 1


class TaskConfiguration(
    ProfileParams,
    BaseRunConfigurationWithCommands,
    BaseRunConfigurationWithPorts,
    TaskConfigurationParams,
):
    type: Literal["task"] = "task"


class ServiceConfigurationParams(CoreModel):
    port: Annotated[
        Union[ValidPort, constr(regex=r"^[0-9]+:[0-9]+$"), PortMapping],
        Field(description="The port, that application listens on or the mapping"),
    ]
    gateway: Annotated[
        Optional[Union[bool, str]],
        Field(
            description=(
                "The name of the gateway. Specify boolean `false` to run without a gateway."
                " Omit to run with the default gateway"
            ),
        ),
    ] = None
    strip_prefix: Annotated[
        bool,
        Field(
            description=(
                "Strip the `/proxy/services/<project name>/<run name>/` path prefix"
                " when forwarding requests to the service. Only takes effect"
                " when running the service without a gateway"
            )
        ),
    ] = STRIP_PREFIX_DEFAULT
    model: Annotated[
        Optional[Union[AnyModel, str]],
        Field(
            description=(
                "Mapping of the model for the OpenAI-compatible endpoint provided by `dstack`."
                " Can be a full model format definition or just a model name."
                " If it's a name, the service is expected to expose an OpenAI-compatible"
                " API at the `/v1` path"
            )
        ),
    ] = None
    https: Annotated[bool, Field(description="Enable HTTPS if running with a gateway")] = (
        SERVICE_HTTPS_DEFAULT
    )
    auth: Annotated[bool, Field(description="Enable the authorization")] = True
    replicas: Annotated[
        Union[conint(ge=1), constr(regex=r"^[0-9]+..[1-9][0-9]*$"), Range[int]],
        Field(
            description="The number of replicas. Can be a number (e.g. `2`) or a range (`0..4` or `1..8`). "
            "If it's a range, the `scaling` property is required"
        ),
    ] = Range[int](min=1, max=1)
    scaling: Annotated[
        Optional[ScalingSpec],
        Field(description="The auto-scaling rules. Required if `replicas` is set to a range"),
    ] = None

    @validator("port")
    def convert_port(cls, v) -> PortMapping:
        if isinstance(v, int):
            return PortMapping(local_port=80, container_port=v)
        elif isinstance(v, str):
            return PortMapping.parse(v)
        return v

    @validator("model")
    def convert_model(cls, v: Optional[Union[AnyModel, str]]) -> Optional[AnyModel]:
        if isinstance(v, str):
            return OpenAIChatModel(type="chat", name=v, format="openai")
        return v

    @validator("replicas")
    def convert_replicas(cls, v: Any) -> Range[int]:
        if isinstance(v, str) and ".." in v:
            min, max = v.replace(" ", "").split("..")
            v = Range(min=min or 0, max=max or None)
        elif isinstance(v, (int, float)):
            v = Range(min=v, max=v)
        if v.max is None:
            raise ValueError("The maximum number of replicas is required")
        if v.min < 0:
            raise ValueError("The minimum number of replicas must be greater than or equal to 0")
        if v.max < v.min:
            raise ValueError(
                "The maximum number of replicas must be greater than or equal to the minium number of replicas"
            )
        return v

    @validator("gateway")
    def validate_gateway(
        cls, v: Optional[Union[bool, str]]
    ) -> Optional[Union[Literal[False], str]]:
        if v == True:
            raise ValueError(
                "The `gateway` property must be a string or boolean `false`, not boolean `true`"
            )
        return v

    @root_validator()
    def validate_scaling(cls, values):
        scaling = values.get("scaling")
        replicas = values.get("replicas")
        if replicas.min != replicas.max and not scaling:
            raise ValueError("When you set `replicas` to a range, ensure to specify `scaling`.")
        if replicas.min == replicas.max and scaling:
            raise ValueError("To use `scaling`, `replicas` must be set to a range.")
        return values


class ServiceConfiguration(
    ProfileParams, BaseRunConfigurationWithCommands, ServiceConfigurationParams
):
    type: Literal["service"] = "service"


AnyRunConfiguration = Union[DevEnvironmentConfiguration, TaskConfiguration, ServiceConfiguration]


class RunConfiguration(CoreModel):
    __root__: Annotated[
        AnyRunConfiguration,
        Field(discriminator="type"),
    ]


def parse_run_configuration(data: dict) -> AnyRunConfiguration:
    try:
        conf = RunConfiguration.parse_obj(data).__root__
    except ValidationError as e:
        raise ConfigurationError(e)
    return conf


class ApplyConfigurationType(str, Enum):
    DEV_ENVIRONMENT = "dev-environment"
    TASK = "task"
    SERVICE = "service"
    FLEET = "fleet"
    GATEWAY = "gateway"
    VOLUME = "volume"


AnyApplyConfiguration = Union[
    AnyRunConfiguration,
    FleetConfiguration,
    GatewayConfiguration,
    VolumeConfiguration,
]


class ApplyConfiguration(CoreModel):
    __root__: Annotated[
        AnyApplyConfiguration,
        Field(discriminator="type"),
    ]


def parse_apply_configuration(data: dict) -> AnyApplyConfiguration:
    try:
        conf = ApplyConfiguration.parse_obj(data).__root__
    except ValidationError as e:
        raise ConfigurationError(e)
    return conf


AnyDstackConfiguration = AnyApplyConfiguration


class DstackConfiguration(CoreModel):
    __root__: Annotated[
        AnyDstackConfiguration,
        Field(discriminator="type"),
    ]

    class Config:
        schema_extra = {"$schema": "http://json-schema.org/draft-07/schema#"}
