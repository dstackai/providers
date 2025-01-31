from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union

from pydantic import UUID4, Field, root_validator
from typing_extensions import Annotated

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.common import ApplyAction, CoreModel, NetworkMode, RegistryAuth
from dstack._internal.core.models.configurations import (
    AnyRunConfiguration,
    RunConfiguration,
)
from dstack._internal.core.models.instances import (
    InstanceOfferWithAvailability,
    InstanceSharedOffer,
    InstanceType,
    SSHConnectionParams,
)
from dstack._internal.core.models.profiles import (
    CreationPolicy,
    Profile,
    ProfileParams,
    ProfileRetryPolicy,
    RetryEvent,
    SpotPolicy,
)
from dstack._internal.core.models.repos import AnyRunRepoData
from dstack._internal.core.models.resources import Memory, ResourcesSpec
from dstack._internal.core.models.unix import UnixUser
from dstack._internal.utils import common as common_utils
from dstack._internal.utils.common import format_pretty_duration


class AppSpec(CoreModel):
    port: int
    map_to_port: Optional[int]
    app_name: str
    url_path: Optional[str] = None
    url_query_params: Optional[Dict[str, str]] = None


class JobStatus(str, Enum):
    SUBMITTED = "submitted"
    PROVISIONING = "provisioning"
    PULLING = "pulling"
    RUNNING = "running"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    ABORTED = "aborted"
    FAILED = "failed"
    DONE = "done"

    @classmethod
    def finished_statuses(cls) -> List["JobStatus"]:
        return [cls.TERMINATED, cls.ABORTED, cls.FAILED, cls.DONE]

    def is_finished(self):
        return self in self.finished_statuses()


class Retry(CoreModel):
    on_events: List[RetryEvent]
    duration: int

    def pretty_format(self) -> str:
        pretty_duration = format_pretty_duration(self.duration)
        events = ", ".join(event.value for event in self.on_events)
        return f"{pretty_duration}[{events}]"


class RunTerminationReason(str, Enum):
    ALL_JOBS_DONE = "all_jobs_done"
    JOB_FAILED = "job_failed"
    RETRY_LIMIT_EXCEEDED = "retry_limit_exceeded"
    STOPPED_BY_USER = "stopped_by_user"
    ABORTED_BY_USER = "aborted_by_user"
    SERVER_ERROR = "server_error"

    def to_job_termination_reason(self) -> "JobTerminationReason":
        mapping = {
            self.ALL_JOBS_DONE: JobTerminationReason.DONE_BY_RUNNER,
            self.JOB_FAILED: JobTerminationReason.TERMINATED_BY_SERVER,
            self.RETRY_LIMIT_EXCEEDED: JobTerminationReason.TERMINATED_BY_SERVER,
            self.STOPPED_BY_USER: JobTerminationReason.TERMINATED_BY_USER,
            self.ABORTED_BY_USER: JobTerminationReason.ABORTED_BY_USER,
            self.SERVER_ERROR: JobTerminationReason.TERMINATED_BY_SERVER,
        }
        return mapping[self]

    def to_status(self) -> "RunStatus":
        mapping = {
            self.ALL_JOBS_DONE: RunStatus.DONE,
            self.JOB_FAILED: RunStatus.FAILED,
            self.RETRY_LIMIT_EXCEEDED: RunStatus.FAILED,
            self.STOPPED_BY_USER: RunStatus.TERMINATED,
            self.ABORTED_BY_USER: RunStatus.TERMINATED,
            self.SERVER_ERROR: RunStatus.FAILED,
        }
        return mapping[self]


class JobTerminationReason(str, Enum):
    # Set by the server
    FAILED_TO_START_DUE_TO_NO_CAPACITY = "failed_to_start_due_to_no_capacity"
    INTERRUPTED_BY_NO_CAPACITY = "interrupted_by_no_capacity"
    WAITING_INSTANCE_LIMIT_EXCEEDED = "waiting_instance_limit_exceeded"
    WAITING_RUNNER_LIMIT_EXCEEDED = "waiting_runner_limit_exceeded"
    TERMINATED_BY_USER = "terminated_by_user"
    VOLUME_ERROR = "volume_error"
    GATEWAY_ERROR = "gateway_error"
    SCALED_DOWN = "scaled_down"
    DONE_BY_RUNNER = "done_by_runner"
    ABORTED_BY_USER = "aborted_by_user"
    TERMINATED_BY_SERVER = "terminated_by_server"
    # Set by the runner
    CONTAINER_EXITED_WITH_ERROR = "container_exited_with_error"
    PORTS_BINDING_FAILED = "ports_binding_failed"
    CREATING_CONTAINER_ERROR = "creating_container_error"
    EXECUTOR_ERROR = "executor_error"
    MAX_DURATION_EXCEEDED = "max_duration_exceeded"

    def to_status(self) -> JobStatus:
        mapping = {
            self.FAILED_TO_START_DUE_TO_NO_CAPACITY: JobStatus.FAILED,
            self.INTERRUPTED_BY_NO_CAPACITY: JobStatus.FAILED,
            self.WAITING_INSTANCE_LIMIT_EXCEEDED: JobStatus.FAILED,
            self.WAITING_RUNNER_LIMIT_EXCEEDED: JobStatus.FAILED,
            self.TERMINATED_BY_USER: JobStatus.TERMINATED,
            self.VOLUME_ERROR: JobStatus.FAILED,
            self.GATEWAY_ERROR: JobStatus.FAILED,
            self.SCALED_DOWN: JobStatus.TERMINATED,
            self.DONE_BY_RUNNER: JobStatus.DONE,
            self.ABORTED_BY_USER: JobStatus.ABORTED,
            self.TERMINATED_BY_SERVER: JobStatus.TERMINATED,
            self.CONTAINER_EXITED_WITH_ERROR: JobStatus.FAILED,
            self.PORTS_BINDING_FAILED: JobStatus.FAILED,
            self.CREATING_CONTAINER_ERROR: JobStatus.FAILED,
            self.EXECUTOR_ERROR: JobStatus.FAILED,
            self.MAX_DURATION_EXCEEDED: JobStatus.TERMINATED,
        }
        return mapping[self]

    def pretty_repr(self) -> str:
        return " ".join(self.value.split("_")).capitalize()


class Requirements(CoreModel):
    # TODO: Make requirements' fields required
    resources: ResourcesSpec
    max_price: Optional[float]
    spot: Optional[bool]
    reservation: Optional[str]

    def pretty_format(self, resources_only: bool = False):
        res = self.resources.pretty_format()
        if not resources_only:
            if self.spot is not None:
                res += f", {'spot' if self.spot else 'on-demand'}"
            if self.max_price is not None:
                res += f" under ${self.max_price:g} per hour"
        return res


class Gateway(CoreModel):
    gateway_name: Optional[str]
    service_port: int
    hostname: Optional[str]
    public_port: int = 80
    secure: bool = False

    auth: bool = True
    options: dict = {}


class JobSpec(CoreModel):
    replica_num: int = 0  # default value for backward compatibility
    job_num: int
    job_name: str
    jobs_per_replica: int = 1  # default value for backward compatibility
    app_specs: Optional[List[AppSpec]]
    user: Optional[UnixUser] = None  # default value for backward compatibility
    commands: List[str]
    env: Dict[str, str]
    home_dir: Optional[str]
    image_name: str
    privileged: bool = False
    max_duration: Optional[int]
    stop_duration: Optional[int] = None
    registry_auth: Optional[RegistryAuth]
    requirements: Requirements
    retry: Optional[Retry]
    # For backward compatibility with 0.18.x when retry_policy was required.
    # TODO: remove in 0.19
    retry_policy: ProfileRetryPolicy = ProfileRetryPolicy(retry=False)
    working_dir: Optional[str]


class JobProvisioningData(CoreModel):
    backend: BackendType
    # In case backend provisions instance in another backend, it may set that backend as base_backend.
    base_backend: Optional[BackendType] = None
    instance_type: InstanceType
    instance_id: str
    # hostname may not be set immediately after instance provisioning.
    # It is set to a public IP or, if public IPs are disabled, to a private IP.
    hostname: Optional[str] = None
    internal_ip: Optional[str] = None
    # public_ip_enabled can used to distinguished instances with and without public IPs.
    # hostname being None is not enough since it can be filled after provisioning.
    public_ip_enabled: bool = True
    # instance_network a network address for multimode installation. Specified as `<ip address>/<netmask>`
    # internal_ip will be selected from the specified network
    instance_network: Optional[str] = None
    region: str
    availability_zone: Optional[str] = None
    reservation: Optional[str] = None
    price: float
    username: str
    # ssh_port be different from 22 for some backends.
    # ssh_port may not be set immediately after instance provisioning
    ssh_port: Optional[int] = None
    dockerized: bool  # True if backend starts shim
    ssh_proxy: Optional[SSHConnectionParams] = None
    backend_data: Optional[str] = None  # backend-specific data in json

    def get_base_backend(self) -> BackendType:
        if self.base_backend is not None:
            return self.base_backend
        return self.backend


class JobRuntimeData(CoreModel):
    network_mode: NetworkMode
    # GPU, CPU, memory resource shares. None means all available (no limit)
    gpu: Optional[int] = None
    cpu: Optional[float] = None
    memory: Optional[Memory] = None
    # container:host port mapping reported by shim. Empty dict if network_mode == NetworkMode.HOST
    # None if data is not yet available (on vm-based backends and ssh instances)
    # or not applicable (container-based backends)
    ports: Optional[dict[int, int]] = None
    # List of volumes used by the job
    volume_names: Optional[list[str]] = None  # None for backward compalibility
    # Virtual shared offer. None if the instance is not shared.
    offer: Optional[InstanceSharedOffer] = None


class ClusterInfo(CoreModel):
    job_ips: List[str]
    master_job_ip: str
    gpus_per_job: int


class JobSubmission(CoreModel):
    id: UUID4
    submission_num: int
    submitted_at: datetime
    last_processed_at: datetime
    finished_at: Optional[datetime]
    status: JobStatus
    termination_reason: Optional[JobTerminationReason]
    termination_reason_message: Optional[str]
    job_provisioning_data: Optional[JobProvisioningData]
    job_runtime_data: Optional[JobRuntimeData]

    @property
    def age(self) -> timedelta:
        return common_utils.get_current_datetime() - self.submitted_at

    @property
    def duration(self) -> timedelta:
        end_time = common_utils.get_current_datetime()
        if self.finished_at is not None:
            end_time = self.finished_at
        return end_time - self.submitted_at


class Job(CoreModel):
    job_spec: JobSpec
    job_submissions: List[JobSubmission]


class RunSpec(CoreModel):
    # TODO: run_name, working_dir are redundant here since they already passed in configuration
    run_name: Annotated[
        Optional[str],
        Field(description="The run name. If not set, the run name is generated automatically."),
    ]
    repo_id: Annotated[
        Optional[str],
        Field(
            description=(
                "Same `repo_id` that is specified when initializing the repo"
                " by calling the `/api/project/{project_name}/repos/init` endpoint."
                " If not specified, a default virtual repo is used."
            )
        ),
    ]
    repo_data: Annotated[
        Optional[AnyRunRepoData],
        Field(
            discriminator="repo_type",
            description="The repo data such as the current branch and commit.",
        ),
    ]
    repo_code_hash: Annotated[Optional[str], Field(description="The hash of the repo diff")]
    working_dir: Annotated[
        Optional[str],
        Field(
            description=(
                "The path to the working directory inside the container."
                " It's specified relative to the repository directory (`/workflow`) and should be inside it."
                ' Defaults to `"."`.'
            )
        ),
    ]
    configuration_path: Annotated[
        Optional[str],
        Field(
            description=(
                "The path to the run configuration YAML file."
                " It can be omitted when using the programmatic API."
            )
        ),
    ]
    configuration: Annotated[AnyRunConfiguration, Field(discriminator="type")]
    profile: Annotated[Optional[Profile], Field(description="The profile parameters")]
    ssh_key_pub: Annotated[
        str,
        Field(
            description="The contents of the SSH public key that will be used to connect to the run."
        ),
    ]
    # TODO: make merged_profile a computed field after migrating to pydanticV2
    merged_profile: Annotated[Profile, Field(exclude=True)] = None

    class Config:
        @staticmethod
        def schema_extra(schema: Dict[str, Any], model: Type) -> None:
            prop = schema.get("properties", {})
            prop.pop("merged_profile", None)

    @root_validator
    def _merged_profile(cls, values) -> Dict:
        if values.get("profile") is None:
            merged_profile = Profile(name="default")
        else:
            merged_profile = Profile.parse_obj(values["profile"])
        try:
            conf = RunConfiguration.parse_obj(values["configuration"]).__root__
        except KeyError:
            raise ValueError("Missing configuration")
        for key in ProfileParams.__fields__:
            conf_val = getattr(conf, key, None)
            if conf_val is not None:
                setattr(merged_profile, key, conf_val)
        if merged_profile.creation_policy is None:
            merged_profile.creation_policy = CreationPolicy.REUSE_OR_CREATE
        values["merged_profile"] = merged_profile
        return values


class ServiceModelSpec(CoreModel):
    name: str
    base_url: Annotated[
        str, Field(description="Full URL or path relative to dstack-server's base URL")
    ]
    type: str


class ServiceSpec(CoreModel):
    url: Annotated[str, Field(description="Full URL or path relative to dstack-server's base URL")]
    model: Optional[ServiceModelSpec] = None
    options: Dict[str, Any] = {}


class RunStatus(str, Enum):
    PENDING = "pending"
    SUBMITTED = "submitted"
    PROVISIONING = "provisioning"
    RUNNING = "running"
    TERMINATING = "terminating"
    TERMINATED = "terminated"
    FAILED = "failed"
    DONE = "done"

    @classmethod
    def finished_statuses(cls) -> List["RunStatus"]:
        return [cls.TERMINATED, cls.FAILED, cls.DONE]

    def is_finished(self):
        return self in self.finished_statuses()


class Run(CoreModel):
    id: UUID4
    project_name: str
    user: str
    submitted_at: datetime
    last_processed_at: datetime
    status: RunStatus
    termination_reason: Optional[RunTerminationReason]
    run_spec: RunSpec
    jobs: List[Job]
    latest_job_submission: Optional[JobSubmission]
    cost: float = 0
    service: Optional[ServiceSpec] = None
    # TODO: make error a computed field after migrating to pydanticV2
    error: Optional[str] = None
    deleted: Optional[bool] = None

    @root_validator
    def _error(cls, values) -> Dict:
        values["error"] = _get_run_error(
            run_termination_reason=values["termination_reason"],
            run_jobs=values["jobs"],
        )
        return values


class JobPlan(CoreModel):
    job_spec: JobSpec
    offers: List[Union[InstanceSharedOffer, InstanceOfferWithAvailability]]
    total_offers: int
    max_price: Optional[float]


class RunPlan(CoreModel):
    project_name: str
    user: str
    run_spec: RunSpec
    job_plans: List[JobPlan]
    current_resource: Optional[Run] = None
    # Optional for backward-compatibility with 0.18.x servers
    # TODO: make required in 0.19
    action: Optional[ApplyAction] = None


class ApplyRunPlanInput(CoreModel):
    run_spec: RunSpec
    current_resource: Annotated[
        Optional[Run],
        Field(
            description=(
                "The expected current resource."
                " If the resource has changed, the apply fails unless `force: true`."
            )
        ),
    ] = None


class PoolInstanceOffers(CoreModel):
    pool_name: str
    instances: List[InstanceOfferWithAvailability]


def get_policy_map(spot_policy: Optional[SpotPolicy], default: SpotPolicy) -> Optional[bool]:
    """
    Map profile.spot_policy[SpotPolicy|None] to requirements.spot[bool|None]
    """
    if spot_policy is None:
        spot_policy = default
    policy_map = {
        SpotPolicy.AUTO: None,
        SpotPolicy.SPOT: True,
        SpotPolicy.ONDEMAND: False,
    }
    return policy_map[spot_policy]


def _get_run_error(
    run_termination_reason: Optional[RunTerminationReason],
    run_jobs: List[Job],
) -> str:
    if run_termination_reason is None:
        return ""
    if len(run_jobs) > 1:
        return run_termination_reason.name
    run_job_termination_reason = _get_run_job_termination_reason(run_jobs)
    # For failed runs, also show termination reason to provide more context.
    # For other run statuses, the job termination reason will duplicate run status.
    if run_job_termination_reason is not None and run_termination_reason in [
        RunTerminationReason.JOB_FAILED,
        RunTerminationReason.SERVER_ERROR,
        RunTerminationReason.RETRY_LIMIT_EXCEEDED,
    ]:
        return f"{run_termination_reason.name}\n({run_job_termination_reason.name})"
    return run_termination_reason.name


def _get_run_job_termination_reason(run_jobs: List[Job]) -> Optional[JobTerminationReason]:
    for job in run_jobs:
        if len(job.job_submissions) > 0:
            if job.job_submissions[-1].termination_reason is not None:
                return job.job_submissions[-1].termination_reason
    return None
