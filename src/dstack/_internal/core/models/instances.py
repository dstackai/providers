from enum import Enum
from typing import List, Literal, Optional, Union

import gpuhunt
from pydantic import root_validator

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.common import CoreModel
from dstack._internal.core.models.envs import Env
from dstack._internal.core.models.volumes import Volume
from dstack._internal.utils.common import pretty_resources


class Gpu(CoreModel):
    name: str
    memory_mib: int
    # Although it's declared as Optional, in fact it always has a value set by the root validator,
    # that is, `assert gpu.vendor is not None` should be a safe type narrowing.
    vendor: Optional[gpuhunt.AcceleratorVendor] = None

    @root_validator(pre=True)
    def validate_name_and_vendor(cls, values):
        is_tpu = False
        name = values.get("name")
        if name and name.startswith("tpu-"):
            is_tpu = True
            values["name"] = name[4:]
        vendor = values.get("vendor")
        if vendor is None:
            if is_tpu:
                values["vendor"] = gpuhunt.AcceleratorVendor.GOOGLE
            else:
                values["vendor"] = gpuhunt.AcceleratorVendor.NVIDIA
        else:
            values["vendor"] = gpuhunt.AcceleratorVendor.cast(vendor)
        return values


class Disk(CoreModel):
    size_mib: int


class Resources(CoreModel):
    cpus: int
    memory_mib: int
    gpus: List[Gpu]
    spot: bool
    disk: Disk = Disk(size_mib=102400)  # the default value (100GB) for backward compatibility
    description: str = ""

    def pretty_format(self, include_spot: bool = False) -> str:
        resources = {}
        if self.cpus > 0:
            resources["cpus"] = self.cpus
        if self.memory_mib > 0:
            resources["memory"] = f"{self.memory_mib / 1024:.0f}GB"
        if self.disk.size_mib > 0:
            resources["disk_size"] = f"{self.disk.size_mib / 1024:.1f}GB"
        if self.gpus:
            gpu = self.gpus[0]
            resources["gpu_name"] = gpu.name
            resources["gpu_count"] = len(self.gpus)
            if gpu.memory_mib > 0:
                resources["gpu_memory"] = f"{gpu.memory_mib / 1024:.0f}GB"
        output = pretty_resources(**resources)
        if include_spot and self.spot:
            output += ", SPOT"
        return output


class InstanceType(CoreModel):
    name: str
    resources: Resources


class SSHConnectionParams(CoreModel):
    hostname: str
    username: str
    port: int

    class Config:
        frozen = True


class SSHKey(CoreModel):
    public: str
    private: Optional[str] = None


class RemoteConnectionInfo(CoreModel):
    host: str
    port: int
    ssh_user: str
    ssh_keys: List[SSHKey]
    env: Env = Env()


class InstanceConfiguration(CoreModel):
    project_name: str
    instance_name: str
    user: str  # dstack user name
    ssh_keys: List[SSHKey]
    instance_id: Optional[str] = None
    availability_zone: Optional[str] = None
    placement_group_name: Optional[str] = None
    reservation: Optional[str] = None
    volumes: Optional[List[Volume]] = None

    def get_public_keys(self) -> List[str]:
        return [ssh_key.public.strip() for ssh_key in self.ssh_keys]


class InstanceSharedInfo(CoreModel):
    total_blocks: Union[Literal["auto"], int]
    busy_blocks: int = 0


class InstanceRuntime(Enum):
    SHIM = "shim"
    RUNNER = "runner"


class InstanceAvailability(Enum):
    UNKNOWN = "unknown"
    AVAILABLE = "available"
    NOT_AVAILABLE = "not_available"
    NO_QUOTA = "no_quota"
    IDLE = "idle"
    BUSY = "busy"

    def is_available(self) -> bool:
        return self in {
            InstanceAvailability.UNKNOWN,
            InstanceAvailability.AVAILABLE,
            InstanceAvailability.IDLE,
        }


class InstanceOffer(CoreModel):
    backend: BackendType
    instance: InstanceType
    region: str
    price: float


class InstanceOfferWithAvailability(InstanceOffer):
    availability: InstanceAvailability
    instance_runtime: InstanceRuntime = InstanceRuntime.SHIM


class InstanceSharedOffer(InstanceOfferWithAvailability):
    blocks: int
    total_blocks: int


class InstanceStatus(str, Enum):
    PENDING = "pending"
    PROVISIONING = "provisioning"
    IDLE = "idle"
    BUSY = "busy"
    TERMINATING = "terminating"
    TERMINATED = "terminated"

    def is_available(self) -> bool:
        return self in (
            self.IDLE,
            self.BUSY,
        )

    def is_active(self) -> bool:
        return self not in self.finished_statuses()

    @classmethod
    def finished_statuses(cls) -> List["InstanceStatus"]:
        return [cls.TERMINATING, cls.TERMINATED]
