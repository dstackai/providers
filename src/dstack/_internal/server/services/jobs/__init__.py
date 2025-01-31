import itertools
import json
from datetime import timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from uuid import UUID

import requests
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

import dstack._internal.server.services.backends as backends_services
from dstack._internal.core.backends.base import Backend
from dstack._internal.core.consts import DSTACK_RUNNER_HTTP_PORT, DSTACK_SHIM_HTTP_PORT
from dstack._internal.core.errors import BackendError, ResourceNotExistsError, SSHError
from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.configurations import RunConfigurationType
from dstack._internal.core.models.instances import InstanceStatus, RemoteConnectionInfo
from dstack._internal.core.models.runs import (
    Job,
    JobProvisioningData,
    JobRuntimeData,
    JobSpec,
    JobStatus,
    JobSubmission,
    JobTerminationReason,
    RunSpec,
)
from dstack._internal.server.models import (
    InstanceModel,
    JobModel,
    ProjectModel,
    RunModel,
    VolumeModel,
)
from dstack._internal.server.services import services
from dstack._internal.server.services.jobs.configurators.base import JobConfigurator
from dstack._internal.server.services.jobs.configurators.dev import DevEnvironmentJobConfigurator
from dstack._internal.server.services.jobs.configurators.service import ServiceJobConfigurator
from dstack._internal.server.services.jobs.configurators.task import TaskJobConfigurator
from dstack._internal.server.services.logging import fmt
from dstack._internal.server.services.pools import get_instance_shared_info
from dstack._internal.server.services.runner import client
from dstack._internal.server.services.runner.ssh import runner_ssh_tunnel
from dstack._internal.server.services.volumes import (
    list_project_volume_models,
    volume_model_to_volume,
)
from dstack._internal.utils import common
from dstack._internal.utils.common import get_or_error, run_async
from dstack._internal.utils.logging import get_logger

logger = get_logger(__name__)


async def get_jobs_from_run_spec(run_spec: RunSpec, replica_num: int) -> List[Job]:
    return [
        Job(job_spec=s, job_submissions=[])
        for s in await get_job_specs_from_run_spec(run_spec, replica_num)
    ]


async def get_job_specs_from_run_spec(run_spec: RunSpec, replica_num: int) -> List[JobSpec]:
    job_configurator = _get_job_configurator(run_spec)
    job_specs = await job_configurator.get_job_specs(replica_num=replica_num)
    return job_specs


def find_job(jobs: List[Job], replica_num: int, job_num: int) -> Job:
    for job in jobs:
        if job.job_spec.replica_num == replica_num and job.job_spec.job_num == job_num:
            return job
    raise ResourceNotExistsError(
        f"Job with replica_num={replica_num} and job_num={job_num} not found"
    )


async def get_run_job_model(
    session: AsyncSession, project: ProjectModel, run_name: str, replica_num: int, job_num: int
) -> Optional[JobModel]:
    res = await session.execute(
        select(JobModel)
        .join(JobModel.run)
        .where(
            RunModel.project_id == project.id,
            # assuming run_name is unique for non-deleted runs
            RunModel.run_name == run_name,
            RunModel.deleted == False,
            JobModel.replica_num == replica_num,
            JobModel.job_num == job_num,
        )
        .order_by(JobModel.submission_num.desc())
        .limit(1)
    )
    return res.scalar_one_or_none()


def job_model_to_job_submission(job_model: JobModel) -> JobSubmission:
    job_provisioning_data = get_job_provisioning_data(job_model)
    if job_provisioning_data is not None:
        # TODO remove after transitioning to computed fields
        job_provisioning_data.instance_type.resources.description = (
            job_provisioning_data.instance_type.resources.pretty_format()
        )
        # TODO do we really still need this magic? See https://github.com/dstackai/dstack/pull/1682
        # i.e., replacing `jpd.backend` with `jpd.get_base_backend()` should give the same result
        if (
            job_provisioning_data.backend == BackendType.DSTACK
            and job_provisioning_data.backend_data is not None
        ):
            backend_data = json.loads(job_provisioning_data.backend_data)
            job_provisioning_data.backend = backend_data["base_backend"]
    last_processed_at = job_model.last_processed_at.replace(tzinfo=timezone.utc)
    finished_at = None
    if job_model.status.is_finished():
        finished_at = last_processed_at
    return JobSubmission(
        id=job_model.id,
        submission_num=job_model.submission_num,
        submitted_at=job_model.submitted_at.replace(tzinfo=timezone.utc),
        last_processed_at=last_processed_at,
        finished_at=finished_at,
        status=job_model.status,
        termination_reason=job_model.termination_reason,
        termination_reason_message=job_model.termination_reason_message,
        job_provisioning_data=job_provisioning_data,
        job_runtime_data=get_job_runtime_data(job_model),
    )


def get_job_provisioning_data(job_model: JobModel) -> Optional[JobProvisioningData]:
    if job_model.job_provisioning_data is None:
        return None
    return JobProvisioningData.__response__.parse_raw(job_model.job_provisioning_data)


def get_job_runtime_data(job_model: JobModel) -> Optional[JobRuntimeData]:
    if job_model.job_runtime_data is None:
        return None
    return JobRuntimeData.__response__.parse_raw(job_model.job_runtime_data)


def delay_job_instance_termination(job_model: JobModel):
    job_model.remove_at = common.get_current_datetime() + timedelta(seconds=15)


def _get_job_configurator(run_spec: RunSpec) -> JobConfigurator:
    configuration_type = RunConfigurationType(run_spec.configuration.type)
    configurator_class = _configuration_type_to_configurator_class_map[configuration_type]
    return configurator_class(run_spec)


_job_configurator_classes = [
    DevEnvironmentJobConfigurator,
    TaskJobConfigurator,
    ServiceJobConfigurator,
]

_configuration_type_to_configurator_class_map = {c.TYPE: c for c in _job_configurator_classes}


async def stop_runner(session: AsyncSession, job_model: JobModel):
    project = await session.get(ProjectModel, job_model.project_id)
    ssh_private_key = project.ssh_private_key

    res = await session.execute(
        select(InstanceModel).where(
            InstanceModel.project_id == job_model.project_id,
            InstanceModel.id == job_model.instance_id,
        )
    )
    instance: Optional[InstanceModel] = res.scalar()

    # TODO: Drop this logic and always use project key once it's safe to assume that most on-prem
    # fleets are (re)created after this change: https://github.com/dstackai/dstack/pull/1716
    if instance and instance.remote_connection_info is not None:
        remote_conn_info: RemoteConnectionInfo = RemoteConnectionInfo.__response__.parse_raw(
            instance.remote_connection_info
        )
        ssh_private_key = remote_conn_info.ssh_keys[0].private
    try:
        jpd = get_job_provisioning_data(job_model)
        if jpd is not None:
            jrd = get_job_runtime_data(job_model)
            await run_async(_stop_runner, ssh_private_key, jpd, jrd, job_model)
    except SSHError:
        logger.debug("%s: failed to stop runner", fmt(job_model))


@runner_ssh_tunnel(ports=[DSTACK_RUNNER_HTTP_PORT])
def _stop_runner(
    ports: dict[int, int],
    job_model: JobModel,
):
    logger.debug("%s: stopping runner", fmt(job_model))
    runner_client = client.RunnerClient(port=ports[DSTACK_RUNNER_HTTP_PORT])
    try:
        runner_client.stop()
    except requests.RequestException:
        logger.exception("%s: failed to stop runner gracefully", fmt(job_model))


async def process_terminating_job(
    session: AsyncSession,
    job_model: JobModel,
    instance_model: Optional[InstanceModel],
):
    """
    Stops the job: tells shim to stop the container, detaches the job from the instance,
    and detaches volumes from the instance.
    Graceful stop should already be done by `process_terminating_run`.
    Caller must acquire the locks on the job and the job's instance.
    """
    if (
        job_model.remove_at is not None
        and job_model.remove_at.replace(tzinfo=timezone.utc) > common.get_current_datetime()
    ):
        # it's too early to terminate the instance
        return

    if instance_model is None:
        # Possible if the job hasn't been assigned an instance yet
        await services.unregister_replica(session, job_model)
        _set_job_termination_status(job_model)
        return

    all_volumes_detached: bool = True
    jrd = get_job_runtime_data(job_model)
    jpd = get_job_provisioning_data(job_model)
    if jpd is not None:
        logger.debug("%s: stopping container", fmt(job_model))
        ssh_private_key = instance_model.project.ssh_private_key
        # TODO: Drop this logic and always use project key once it's safe to assume that
        # most on-prem fleets are (re)created after this change:
        # https://github.com/dstackai/dstack/pull/1716
        if instance_model and instance_model.remote_connection_info is not None:
            remote_conn_info: RemoteConnectionInfo = RemoteConnectionInfo.__response__.parse_raw(
                instance_model.remote_connection_info
            )
            ssh_private_key = remote_conn_info.ssh_keys[0].private
        await stop_container(job_model, jpd, ssh_private_key)
        volume_models: list[VolumeModel]
        if jrd is not None and jrd.volume_names is not None:
            volume_models = await list_project_volume_models(
                session=session, project=instance_model.project, names=jrd.volume_names
            )
        else:
            volume_models = instance_model.volumes
        if len(volume_models) > 0:
            logger.info("Detaching volumes: %s", [v.name for v in volume_models])
            all_volumes_detached = await _detach_volumes_from_job_instance(
                project=instance_model.project,
                job_model=job_model,
                jpd=jpd,
                instance_model=instance_model,
                volume_models=volume_models,
            )

    if instance_model.status == InstanceStatus.BUSY:
        # no other jobs besides this one
        if not [j for j in instance_model.jobs if j.id != job_model.id]:
            instance_model.status = InstanceStatus.IDLE
        if jrd is not None and jrd.offer is not None:
            shared_info = get_instance_shared_info(instance_model)
            if shared_info is not None:
                shared_info.busy_blocks -= jrd.offer.blocks
                instance_model.shared_info = shared_info.json()
    elif instance_model.status != InstanceStatus.TERMINATED:
        # instance was PROVISIONING (specially for the job)
        # schedule for termination
        instance_model.status = InstanceStatus.TERMINATING

    if jpd is None or not jpd.dockerized:
        # do not reuse vastai/k8s instances
        instance_model.status = InstanceStatus.TERMINATING

    # The instance should be released even if detach fails
    # so that stuck volumes don't prevent the instance from terminating.
    job_model.instance_id = None
    instance_model.last_job_processed_at = common.get_current_datetime()
    logger.info(
        "%s: instance '%s' has been released, new status is %s",
        fmt(job_model),
        instance_model.name,
        instance_model.status.name,
    )
    await services.unregister_replica(session, job_model)
    if all_volumes_detached:
        # Do not terminate while some volumes are not detached.
        _set_job_termination_status(job_model)


async def process_volumes_detaching(
    session: AsyncSession,
    job_model: JobModel,
    instance_model: InstanceModel,
):
    """
    Called after job's volumes have been soft detached to check if they are detached.
    Terminates the job when all the volumes are detached.
    If the volumes fail to detach, force detaches them.
    """
    jpd = get_or_error(get_job_provisioning_data(job_model))
    jrd = get_job_runtime_data(job_model)
    if jrd is not None and jrd.volume_names is not None:
        volume_models = await list_project_volume_models(
            session=session, project=instance_model.project, names=jrd.volume_names
        )
    else:
        volume_models = instance_model.volumes
    logger.info("Detaching volumes: %s", [v.name for v in volume_models])
    all_volumes_detached = await _detach_volumes_from_job_instance(
        project=instance_model.project,
        job_model=job_model,
        jpd=jpd,
        instance_model=instance_model,
        volume_models=volume_models,
    )
    if all_volumes_detached:
        # Do not terminate the job while some volumes are not detached.
        # If force detach never succeeds, the job will be stuck terminating.
        # The job releases the instance when soft detaching, so the instance won't be stuck.
        _set_job_termination_status(job_model)


def _set_job_termination_status(job_model: JobModel):
    if job_model.termination_reason is not None:
        job_model.status = job_model.termination_reason.to_status()
        termination_reason_name = job_model.termination_reason.name
    else:
        job_model.status = JobStatus.FAILED
        termination_reason_name = None
    logger.info(
        "%s: job status is %s, reason: %s",
        fmt(job_model),
        job_model.status.name,
        termination_reason_name,
    )


async def stop_container(
    job_model: JobModel, job_provisioning_data: JobProvisioningData, ssh_private_key: str
):
    if job_provisioning_data.dockerized:
        # send a request to the shim to terminate the docker container
        # SSHError and RequestException are caught in the `runner_ssh_tunner` decorator
        await run_async(
            _shim_submit_stop,
            ssh_private_key,
            job_provisioning_data,
            None,
            job_model,
        )


@runner_ssh_tunnel(ports=[DSTACK_SHIM_HTTP_PORT])
def _shim_submit_stop(ports: Dict[int, int], job_model: JobModel):
    shim_client = client.ShimClient(port=ports[DSTACK_SHIM_HTTP_PORT])

    resp = shim_client.healthcheck()
    if resp is None:
        logger.debug("%s: can't stop container, shim is not available yet", fmt(job_model))
        return False  # shim is not available yet

    # we force-kill container because the runner had time to gracefully stop the job
    if shim_client.is_api_v2_supported():
        if job_model.termination_reason is None:
            reason = None
        else:
            reason = job_model.termination_reason.value
        shim_client.terminate_task(
            task_id=job_model.id,
            reason=reason,
            message=job_model.termination_reason_message,
            timeout=0,
        )
        # maybe somehow postpone removing old tasks to allow inspecting failed jobs?
        shim_client.remove_task(task_id=job_model.id)
    else:
        shim_client.stop(force=True)


def group_jobs_by_replica_latest(jobs: List[JobModel]) -> Iterable[Tuple[int, List[JobModel]]]:
    """
    Args:
        jobs: unsorted list of jobs

    Yields:
        latest jobs in each replica (replica_num, jobs)
    """
    jobs = sorted(jobs, key=lambda j: (j.replica_num, j.job_num, j.submission_num))
    for replica_num, all_replica_jobs in itertools.groupby(jobs, key=lambda j: j.replica_num):
        replica_jobs: List[JobModel] = []
        for job_num, job_submissions in itertools.groupby(
            all_replica_jobs, key=lambda j: j.job_num
        ):
            # take only the latest submission
            # the latest `submission_num` doesn't have to be the same for all jobs
            *_, latest_job_submission = job_submissions
            replica_jobs.append(latest_job_submission)
        yield replica_num, replica_jobs


async def _detach_volumes_from_job_instance(
    project: ProjectModel,
    job_model: JobModel,
    jpd: JobProvisioningData,
    instance_model: InstanceModel,
    volume_models: list[VolumeModel],
) -> bool:
    job_spec = JobSpec.__response__.parse_raw(job_model.job_spec_data)
    backend = await backends_services.get_project_backend_by_type(
        project=project,
        backend_type=jpd.backend,
    )
    if backend is None:
        logger.error(
            "Failed to detach volumes from %s. Backend not available.", instance_model.name
        )
        return False

    all_detached = True
    detached_volumes = []
    for volume_model in volume_models:
        detached = await _detach_volume_from_job_instance(
            backend=backend,
            job_model=job_model,
            jpd=jpd,
            job_spec=job_spec,
            instance_model=instance_model,
            volume_model=volume_model,
        )
        if detached:
            detached_volumes.append(volume_model)
        else:
            all_detached = False

    if job_model.volumes_detached_at is None:
        job_model.volumes_detached_at = common.get_current_datetime()
    detached_volumes_ids = {v.id for v in detached_volumes}
    instance_model.volumes = [
        v for v in instance_model.volumes if v.id not in detached_volumes_ids
    ]
    return all_detached


async def _detach_volume_from_job_instance(
    backend: Backend,
    job_model: JobModel,
    jpd: JobProvisioningData,
    job_spec: JobSpec,
    instance_model: InstanceModel,
    volume_model: VolumeModel,
) -> bool:
    detached = True
    volume = volume_model_to_volume(volume_model)
    if volume.provisioning_data is None or not volume.provisioning_data.detachable:
        # Backends without `detach_volume` detach volumes automatically
        return detached
    try:
        if job_model.volumes_detached_at is None:
            # We haven't tried detaching volumes yet, try soft detach first
            await run_async(
                backend.compute().detach_volume,
                volume=volume,
                instance_id=jpd.instance_id,
                force=False,
            )
            # For some backends, the volume may be detached immediately
            detached = await run_async(
                backend.compute().is_volume_detached,
                volume=volume,
                instance_id=jpd.instance_id,
            )
        else:
            detached = await run_async(
                backend.compute().is_volume_detached,
                volume=volume,
                instance_id=jpd.instance_id,
            )
            if not detached and _should_force_detach_volume(job_model, job_spec.stop_duration):
                logger.info(
                    "Force detaching volume %s from %s",
                    volume_model.name,
                    instance_model.name,
                )
                await run_async(
                    backend.compute().detach_volume,
                    volume=volume,
                    instance_id=jpd.instance_id,
                    force=True,
                )
                # Let the next iteration check if force detach worked
    except BackendError as e:
        logger.error(
            "Failed to detach volume %s from %s: %s",
            volume_model.name,
            instance_model.name,
            repr(e),
        )
    except Exception:
        logger.exception(
            "Got exception when detaching volume %s from instance %s",
            volume_model.name,
            instance_model.name,
        )
    return detached


MIN_FORCE_DETACH_WAIT_PERIOD = timedelta(seconds=60)


def _should_force_detach_volume(job_model: JobModel, stop_duration: Optional[int]) -> bool:
    return (
        job_model.volumes_detached_at is not None
        and common.get_current_datetime()
        > job_model.volumes_detached_at.replace(tzinfo=timezone.utc) + MIN_FORCE_DETACH_WAIT_PERIOD
        and (
            job_model.termination_reason == JobTerminationReason.ABORTED_BY_USER
            or stop_duration is not None
            and common.get_current_datetime()
            > job_model.volumes_detached_at.replace(tzinfo=timezone.utc)
            + timedelta(seconds=stop_duration)
        )
    )


async def get_instances_ids_with_detaching_volumes(session: AsyncSession) -> List[UUID]:
    res = await session.execute(
        select(JobModel).where(
            JobModel.status == JobStatus.TERMINATING,
            JobModel.used_instance_id.is_not(None),
            JobModel.volumes_detached_at.is_not(None),
        )
    )
    job_models = res.scalars().all()
    return [jm.used_instance_id for jm in job_models if jm.used_instance_id]
