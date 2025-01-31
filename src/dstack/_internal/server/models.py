import uuid
from datetime import datetime
from typing import Callable, List, Optional, Union

from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    String,
    Table,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.sql import false
from sqlalchemy_utils import UUIDType

from dstack._internal.core.errors import DstackError
from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.common import CoreModel
from dstack._internal.core.models.fleets import FleetStatus
from dstack._internal.core.models.gateways import GatewayStatus
from dstack._internal.core.models.instances import InstanceStatus
from dstack._internal.core.models.profiles import (
    DEFAULT_POOL_TERMINATION_IDLE_TIME,
    TerminationPolicy,
)
from dstack._internal.core.models.repos.base import RepoType
from dstack._internal.core.models.runs import (
    JobStatus,
    JobTerminationReason,
    RunStatus,
    RunTerminationReason,
)
from dstack._internal.core.models.users import GlobalRole, ProjectRole
from dstack._internal.core.models.volumes import VolumeStatus
from dstack._internal.server import settings
from dstack._internal.utils.common import get_current_datetime
from dstack._internal.utils.logging import get_logger

logger = get_logger(__name__)


class NaiveDateTime(TypeDecorator):
    """
    A custom type decorator that ensures datetime objects are offset-naive when stored in the database.
    This is needed because we use datetimes in UTC only and store them as offset-naive.
    Some databases (e.g. Postgres) throw an error if the timezone is set.
    """

    impl = DateTime
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is not None and value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value

    def process_result_value(self, value, dialect):
        return value


class DecryptedString(CoreModel):
    """
    A type for representing plaintext strings encrypted with `EncryptedString`.
    Besides the string, stores information if the decryption was successful.
    This is useful so that application code can have custom handling of failed decrypts (e.g. ignoring).
    """

    # Do not read plaintext directly to avoid ignoring errors accidentally.
    # Unpack with get_plaintext_or_error().
    plaintext: Optional[str]
    decrypted: bool = True
    exc: Optional[Exception] = None

    class Config:
        arbitrary_types_allowed = True

    def get_plaintext_or_error(self) -> str:
        if self.decrypted and self.plaintext is not None:
            return self.plaintext
        exc = DstackError("Failed to access plaintext")
        if self.exc is not None:
            raise exc from self.exc
        raise exc


class EncryptedString(TypeDecorator):
    """
    A custom type decorator that encrypts and decrypts strings for storing in the db.
    """

    impl = String
    cache_ok = True

    _encrypt_func: Callable[[str], str]
    _decrypt_func: Callable[[str], str]

    @classmethod
    def set_encrypt_decrypt(
        cls,
        encrypt_func: Callable[[str], str],
        decrypt_func: Callable[[str], str],
    ):
        cls._encrypt_func = encrypt_func
        cls._decrypt_func = decrypt_func

    def process_bind_param(self, value: Union[DecryptedString, str], dialect):
        if isinstance(value, str):
            # Passing string allows binding an encrypted value directly
            # e.g. for comparisons
            return value
        return EncryptedString._encrypt_func(value.get_plaintext_or_error())

    def process_result_value(self, value: Optional[str], dialect) -> Optional[DecryptedString]:
        if value is None:
            return value
        try:
            plaintext = EncryptedString._decrypt_func(value)
            return DecryptedString(plaintext=plaintext, decrypted=True)
        except Exception as e:
            logger.debug("Failed to decrypt encrypted string: %s", repr(e))
            return DecryptedString(plaintext=None, decrypted=False, exc=e)


constraint_naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class BaseModel(DeclarativeBase):
    metadata = MetaData(naming_convention=constraint_naming_convention)


class UserModel(BaseModel):
    __tablename__ = "users"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(50), unique=True)
    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    token: Mapped[DecryptedString] = mapped_column(EncryptedString(200), unique=True)
    # token_hash is needed for fast search by token when stored token is encrypted
    token_hash: Mapped[str] = mapped_column(String(2000), unique=True)
    global_role: Mapped[GlobalRole] = mapped_column(Enum(GlobalRole))
    # deactivated users cannot access API
    active: Mapped[bool] = mapped_column(Boolean, default=True)

    email: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)

    projects_quota: Mapped[int] = mapped_column(
        Integer, default=settings.USER_PROJECT_DEFAULT_QUOTA
    )


class ProjectModel(BaseModel):
    __tablename__ = "projects"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(50), unique=True)
    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    owner_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    owner: Mapped[UserModel] = relationship(lazy="joined")
    members: Mapped[List["MemberModel"]] = relationship(
        back_populates="project", order_by="MemberModel.member_num"
    )

    ssh_private_key: Mapped[str] = mapped_column(Text)
    ssh_public_key: Mapped[str] = mapped_column(Text)

    backends: Mapped[List["BackendModel"]] = relationship(back_populates="project")

    default_gateway_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("gateways.id", use_alter=True, ondelete="SET NULL"), nullable=True
    )
    default_gateway: Mapped[Optional["GatewayModel"]] = relationship(
        foreign_keys=[default_gateway_id]
    )

    default_pool_id: Mapped[Optional[UUIDType]] = mapped_column(
        ForeignKey("pools.id", use_alter=True, ondelete="SET NULL"), nullable=True
    )
    default_pool: Mapped[Optional["PoolModel"]] = relationship(foreign_keys=[default_pool_id])


class MemberModel(BaseModel):
    __tablename__ = "members"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship()
    user_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    user: Mapped[UserModel] = relationship(lazy="joined")
    project_role: Mapped[ProjectRole] = mapped_column(Enum(ProjectRole))
    # member_num defines members ordering
    member_num: Mapped[Optional[int]] = mapped_column(Integer)


class BackendModel(BaseModel):
    __tablename__ = "backends"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship()
    type: Mapped[BackendType] = mapped_column(Enum(BackendType))

    config: Mapped[str] = mapped_column(String(20000))
    auth: Mapped[DecryptedString] = mapped_column(EncryptedString(20000))

    gateways: Mapped[List["GatewayModel"]] = relationship(back_populates="backend")


class RepoModel(BaseModel):
    __tablename__ = "repos"
    __table_args__ = (UniqueConstraint("project_id", "name", name="uq_repos_project_id_name"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship()
    # RepoModel.name stores repo_id
    name: Mapped[str] = mapped_column(String(100))
    type: Mapped[RepoType] = mapped_column(Enum(RepoType))

    info: Mapped[str] = mapped_column(Text)

    # `creds` is deprecated, for newly initialized repos per-user `RepoCredsModel` should be used
    # instead. As of 0.18.25, there is no plan to remove this field, it's used as a fallback when
    # `RepoCredsModel` associated with the user is not found.
    creds: Mapped[Optional[str]] = mapped_column(String(5000))


class RepoCredsModel(BaseModel):
    __tablename__ = "repo_creds"
    __table_args__ = (
        UniqueConstraint("repo_id", "user_id", name="uq_repo_creds_repo_id_user_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    repo_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("repos.id", ondelete="CASCADE"))
    repo: Mapped["RepoModel"] = relationship()
    user_id: Mapped["UserModel"] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    user: Mapped["UserModel"] = relationship()

    creds: Mapped[DecryptedString] = mapped_column(EncryptedString(10000))


class CodeModel(BaseModel):
    __tablename__ = "codes"
    __table_args__ = (UniqueConstraint("repo_id", "blob_hash", name="uq_codes_repo_id_blob_hash"),)

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    repo_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("repos.id", ondelete="CASCADE"))
    repo: Mapped["RepoModel"] = relationship()
    blob_hash: Mapped[str] = mapped_column(String(4000))
    blob: Mapped[Optional[bytes]] = mapped_column(LargeBinary)  # None means blob is stored on s3


class RunModel(BaseModel):
    __tablename__ = "runs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship()

    user_id: Mapped["UserModel"] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    user: Mapped["UserModel"] = relationship()

    repo_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("repos.id", ondelete="CASCADE"))
    repo: Mapped["RepoModel"] = relationship()

    # Runs reference fleets so that fleets cannot be deleted while they are used.
    # A fleet can have no busy instances but still be used by a run (e.g. a service with 0 replicas).
    fleet_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("fleets.id"))
    fleet: Mapped[Optional["FleetModel"]] = relationship(back_populates="runs")

    run_name: Mapped[str] = mapped_column(String(100))
    submitted_at: Mapped[datetime] = mapped_column(NaiveDateTime)
    last_processed_at: Mapped[datetime] = mapped_column(NaiveDateTime)
    status: Mapped[RunStatus] = mapped_column(Enum(RunStatus))
    termination_reason: Mapped[Optional[RunTerminationReason]] = mapped_column(
        Enum(RunTerminationReason)
    )
    run_spec: Mapped[str] = mapped_column(Text)
    service_spec: Mapped[Optional[str]] = mapped_column(Text)

    jobs: Mapped[List["JobModel"]] = relationship(
        back_populates="run", lazy="selectin", order_by="[JobModel.replica_num, JobModel.job_num]"
    )

    gateway_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("gateways.id", ondelete="SET NULL")
    )
    gateway: Mapped[Optional["GatewayModel"]] = relationship()

    __table_args__ = (Index("ix_submitted_at_id", submitted_at.desc(), id),)


class JobModel(BaseModel):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship()
    run_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("runs.id", ondelete="CASCADE"))
    run: Mapped["RunModel"] = relationship()
    run_name: Mapped[str] = mapped_column(String(100))
    job_num: Mapped[int] = mapped_column(Integer)
    job_name: Mapped[str] = mapped_column(String(100))
    submission_num: Mapped[int] = mapped_column(Integer)
    submitted_at: Mapped[datetime] = mapped_column(NaiveDateTime)
    last_processed_at: Mapped[datetime] = mapped_column(NaiveDateTime)
    status: Mapped[JobStatus] = mapped_column(Enum(JobStatus))
    termination_reason: Mapped[Optional[JobTerminationReason]] = mapped_column(
        Enum(JobTerminationReason)
    )
    termination_reason_message: Mapped[Optional[str]] = mapped_column(Text)
    job_spec_data: Mapped[str] = mapped_column(Text)
    job_provisioning_data: Mapped[Optional[str]] = mapped_column(Text)
    runner_timestamp: Mapped[Optional[int]] = mapped_column(BigInteger)
    # `removed` is used to ensure that the instance is killed after the job is finished
    remove_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)
    volumes_detached_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)
    # `instance_assigned` means instance assignment was done.
    # if `instance_assigned` is True and `instance` is None, no instance was assiged.
    instance_assigned: Mapped[bool] = mapped_column(Boolean, default=False)
    instance_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("instances.id", ondelete="CASCADE")
    )
    instance: Mapped[Optional["InstanceModel"]] = relationship(back_populates="jobs")
    used_instance_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUIDType(binary=False))
    replica_num: Mapped[int] = mapped_column(Integer)
    job_runtime_data: Mapped[Optional[str]] = mapped_column(Text)


class GatewayModel(BaseModel):
    __tablename__ = "gateways"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100))
    region: Mapped[str] = mapped_column(String(100))
    wildcard_domain: Mapped[Optional[str]] = mapped_column(String(100))
    # `configuration` is optional for compatibility with pre-0.18.2 gateways.
    # Use `get_gateway_configuration` to construct `configuration` for old gateways.
    configuration: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    status: Mapped[GatewayStatus] = mapped_column(Enum(GatewayStatus))
    status_message: Mapped[Optional[str]] = mapped_column(Text)
    last_processed_at: Mapped[datetime] = mapped_column(NaiveDateTime)

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship(foreign_keys=[project_id])
    backend_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("backends.id", ondelete="CASCADE"))
    backend: Mapped["BackendModel"] = relationship(lazy="selectin")

    gateway_compute_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("gateway_computes.id", ondelete="CASCADE")
    )
    gateway_compute: Mapped[Optional["GatewayComputeModel"]] = relationship(lazy="joined")

    runs: Mapped[List["RunModel"]] = relationship(back_populates="gateway")

    __table_args__ = (UniqueConstraint("project_id", "name", name="uq_gateways_project_id_name"),)


class GatewayComputeModel(BaseModel):
    __tablename__ = "gateway_computes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    instance_id: Mapped[str] = mapped_column(String(100))
    ip_address: Mapped[str] = mapped_column(String(100))
    hostname: Mapped[Optional[str]] = mapped_column(String(100))
    # `configuration` is optional for compatibility with pre-0.18.2 gateways.
    # Use `get_gateway_compute_configuration` to construct `configuration` for old gateways.
    configuration: Mapped[Optional[str]] = mapped_column(Text)
    backend_data: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[str] = mapped_column(String(100))

    backend_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        ForeignKey("backends.id", ondelete="CASCADE")
    )
    backend: Mapped[Optional["BackendModel"]] = relationship()

    # The key to authorize the server with the gateway
    ssh_private_key: Mapped[str] = mapped_column(Text)
    ssh_public_key: Mapped[str] = mapped_column(Text)

    # active means the server should maintain connection to gateway.
    active: Mapped[bool] = mapped_column(Boolean, default=True)
    deleted: Mapped[bool] = mapped_column(Boolean, server_default=false())
    app_updated_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)


class PoolModel(BaseModel):
    __tablename__ = "pools"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship(foreign_keys=[project_id])

    instances: Mapped[List["InstanceModel"]] = relationship(back_populates="pool", lazy="selectin")


class FleetModel(BaseModel):
    __tablename__ = "fleets"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100))

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship(foreign_keys=[project_id])

    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    last_processed_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    status: Mapped[FleetStatus] = mapped_column(Enum(FleetStatus))
    status_message: Mapped[Optional[str]] = mapped_column(Text)

    spec: Mapped[str] = mapped_column(Text)

    runs: Mapped[List["RunModel"]] = relationship(back_populates="fleet")
    instances: Mapped[List["InstanceModel"]] = relationship(back_populates="fleet")


class InstanceModel(BaseModel):
    __tablename__ = "instances"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(50))

    instance_num: Mapped[int] = mapped_column(Integer, default=0)

    # instance
    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    last_processed_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship(foreign_keys=[project_id])

    pool_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("pools.id"))
    pool: Mapped["PoolModel"] = relationship(back_populates="instances")

    fleet_id: Mapped[Optional[uuid.UUID]] = mapped_column(ForeignKey("fleets.id"))
    fleet: Mapped[Optional["FleetModel"]] = relationship(back_populates="instances")

    status: Mapped[InstanceStatus] = mapped_column(Enum(InstanceStatus))
    unreachable: Mapped[bool] = mapped_column(Boolean)

    # VM
    started_at: Mapped[Optional[datetime]] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    # create instance
    # TODO: Introduce a field that would store all resolved instance profile parameters, etc, (similar to job_spec).
    # Currently, profile parameters are parsed every time they are accessed (e.g. see profile.retry).
    profile: Mapped[Optional[str]] = mapped_column(Text)
    requirements: Mapped[Optional[str]] = mapped_column(Text)
    instance_configuration: Mapped[Optional[str]] = mapped_column(Text)

    # temination policy
    termination_policy: Mapped[Optional[TerminationPolicy]] = mapped_column(String(100))
    # TODO: Suggestion: do not assign DEFAULT_POOL_TERMINATION_IDLE_TIME as the default here (make Optional instead; also instead of -1)
    termination_idle_time: Mapped[int] = mapped_column(
        Integer, default=DEFAULT_POOL_TERMINATION_IDLE_TIME
    )

    # retry policy
    last_retry_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    # instance termination handling
    termination_deadline: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)
    termination_reason: Mapped[Optional[str]] = mapped_column(String(4000))
    health_status: Mapped[Optional[str]] = mapped_column(String(4000))
    first_termination_retry_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)
    last_termination_retry_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    # backend
    backend: Mapped[Optional[BackendType]] = mapped_column(Enum(BackendType))
    backend_data: Mapped[Optional[str]] = mapped_column(Text)

    # offer
    offer: Mapped[Optional[str]] = mapped_column(Text)
    region: Mapped[Optional[str]] = mapped_column(String(2000))
    price: Mapped[Optional[float]] = mapped_column(Float)

    job_provisioning_data: Mapped[Optional[str]] = mapped_column(Text)

    remote_connection_info: Mapped[Optional[str]] = mapped_column(Text)

    # If NULL, the instance is not shareable (i.e., cannot be split into blocks)
    shared_info: Mapped[Optional[str]] = mapped_column(Text)

    jobs: Mapped[list["JobModel"]] = relationship(back_populates="instance", lazy="joined")
    last_job_processed_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    # volumes attached to the instance
    volumes: Mapped[List["VolumeModel"]] = relationship(
        secondary="volumes_attachments",
        back_populates="instances",
    )


class VolumeModel(BaseModel):
    __tablename__ = "volumes"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100))

    user_id: Mapped["UserModel"] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    user: Mapped["UserModel"] = relationship()

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship(foreign_keys=[project_id])

    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    last_processed_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    status: Mapped[VolumeStatus] = mapped_column(Enum(VolumeStatus))
    status_message: Mapped[Optional[str]] = mapped_column(Text)

    configuration: Mapped[str] = mapped_column(Text)
    volume_provisioning_data: Mapped[Optional[str]] = mapped_column(Text)
    # FIXME: volume_attachment_data should be in "volumes_attachments"
    # to support multi-attach volumes
    volume_attachment_data: Mapped[Optional[str]] = mapped_column(Text)

    # instances the volume is attached to
    instances: Mapped[List["InstanceModel"]] = relationship(
        secondary="volumes_attachments",
        back_populates="volumes",
    )


volumes_attachments_table = Table(
    "volumes_attachments",
    BackendModel.metadata,
    Column("volume_id", ForeignKey("volumes.id"), primary_key=True),
    Column("instace_id", ForeignKey("instances.id"), primary_key=True),
)


class PlacementGroupModel(BaseModel):
    __tablename__ = "placement_groups"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100))

    project_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("projects.id", ondelete="CASCADE"))
    project: Mapped["ProjectModel"] = relationship(foreign_keys=[project_id])

    fleet_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("fleets.id"))
    fleet: Mapped["FleetModel"] = relationship(foreign_keys=[fleet_id])
    fleet_deleted: Mapped[bool] = mapped_column(Boolean, default=False)

    created_at: Mapped[datetime] = mapped_column(NaiveDateTime, default=get_current_datetime)
    last_processed_at: Mapped[datetime] = mapped_column(
        NaiveDateTime, default=get_current_datetime
    )
    deleted: Mapped[bool] = mapped_column(Boolean, default=False)
    deleted_at: Mapped[Optional[datetime]] = mapped_column(NaiveDateTime)

    configuration: Mapped[str] = mapped_column(Text)
    provisioning_data: Mapped[Optional[str]] = mapped_column(Text)


class JobMetricsPoint(BaseModel):
    __tablename__ = "job_metrics_points"

    id: Mapped[uuid.UUID] = mapped_column(
        UUIDType(binary=False), primary_key=True, default=uuid.uuid4
    )

    job_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("jobs.id"))
    job: Mapped["JobModel"] = relationship()

    timestamp_micro: Mapped[int] = mapped_column(BigInteger)
    cpu_usage_micro: Mapped[int] = mapped_column(BigInteger)
    memory_usage_bytes: Mapped[int] = mapped_column(BigInteger)
    memory_working_set_bytes: Mapped[int] = mapped_column(BigInteger)

    # json-encoded lists of metric values of len(gpus) length
    gpus_memory_usage_bytes: Mapped[str] = mapped_column(Text)
    gpus_util_percent: Mapped[str] = mapped_column(Text)
