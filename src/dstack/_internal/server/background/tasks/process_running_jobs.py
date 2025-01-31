import asyncio
from datetime import timedelta
from typing import Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from dstack._internal.core.consts import DSTACK_RUNNER_HTTP_PORT, DSTACK_SHIM_HTTP_PORT
from dstack._internal.core.errors import GatewayError
from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.common import NetworkMode, RegistryAuth, is_core_model_instance
from dstack._internal.core.models.instances import InstanceStatus, RemoteConnectionInfo
from dstack._internal.core.models.repos import RemoteRepoCreds
from dstack._internal.core.models.runs import (
    ClusterInfo,
    Job,
    JobProvisioningData,
    JobSpec,
    JobStatus,
    JobTerminationReason,
    Run,
)
from dstack._internal.core.models.volumes import InstanceMountPoint, Volume, VolumeMountPoint
from dstack._internal.server.db import get_session_ctx
from dstack._internal.server.models import (
    JobModel,
    ProjectModel,
    RepoModel,
    RunModel,
)
from dstack._internal.server.schemas.runner import TaskStatus
from dstack._internal.server.services import logs as logs_services
from dstack._internal.server.services import services
from dstack._internal.server.services.jobs import (
    find_job,
    get_job_runtime_data,
    job_model_to_job_submission,
)
from dstack._internal.server.services.locking import get_locker
from dstack._internal.server.services.logging import fmt
from dstack._internal.server.services.repos import (
    get_code_model,
    get_repo_creds,
    repo_model_to_repo_head_with_creds,
)
from dstack._internal.server.services.runner import client
from dstack._internal.server.services.runner.ssh import runner_ssh_tunnel
from dstack._internal.server.services.runs import (
    get_job_volumes,
    run_model_to_run,
)
from dstack._internal.server.services.storage import get_default_storage
from dstack._internal.utils import common as common_utils
from dstack._internal.utils.interpolator import VariablesInterpolator
from dstack._internal.utils.logging import get_logger

logger = get_logger(__name__)


async def process_running_jobs(batch_size: int = 1):
    tasks = []
    for _ in range(batch_size):
        tasks.append(_process_next_running_job())
    await asyncio.gather(*tasks)


async def _process_next_running_job():
    lock, lockset = get_locker().get_lockset(JobModel.__tablename__)
    async with get_session_ctx() as session:
        async with lock:
            res = await session.execute(
                select(JobModel)
                .where(
                    JobModel.status.in_(
                        [JobStatus.PROVISIONING, JobStatus.PULLING, JobStatus.RUNNING]
                    ),
                    JobModel.id.not_in(lockset),
                )
                .order_by(JobModel.last_processed_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job_model = res.unique().scalar()
            if job_model is None:
                return
            lockset.add(job_model.id)

        try:
            job_model_id = job_model.id
            await _process_running_job(session=session, job_model=job_model)
        finally:
            lockset.difference_update([job_model_id])


async def _process_running_job(session: AsyncSession, job_model: JobModel):
    # Refetch to load related attributes.
    # joinedload produces LEFT OUTER JOIN that can't be used with FOR UPDATE.
    res = await session.execute(
        select(JobModel)
        .where(JobModel.id == job_model.id)
        .options(joinedload(JobModel.instance))
        .execution_options(populate_existing=True)
    )
    job_model = res.unique().scalar_one()
    res = await session.execute(
        select(RunModel)
        .where(RunModel.id == job_model.run_id)
        .options(joinedload(RunModel.project).joinedload(ProjectModel.backends))
        .options(joinedload(RunModel.user))
        .options(joinedload(RunModel.repo))
        .options(joinedload(RunModel.jobs))
    )
    run_model = res.unique().scalar_one()
    repo_model = run_model.repo
    project = run_model.project
    run = run_model_to_run(run_model)
    job_submission = job_model_to_job_submission(job_model)
    job_provisioning_data = job_submission.job_provisioning_data
    if job_provisioning_data is None:
        logger.error("%s: job_provisioning_data of an active job is None", fmt(job_model))
        job_model.status = JobStatus.TERMINATING
        job_model.termination_reason = JobTerminationReason.TERMINATED_BY_SERVER
        job_model.last_processed_at = common_utils.get_current_datetime()
        return

    job = find_job(run.jobs, job_model.replica_num, job_model.job_num)

    # Wait until all other jobs in the replica are provisioned
    for other_job in run.jobs:
        if (
            other_job.job_spec.replica_num == job.job_spec.replica_num
            and other_job.job_submissions[-1].status == JobStatus.SUBMITTED
        ):
            job_model.last_processed_at = common_utils.get_current_datetime()
            await session.commit()
            return

    cluster_info = _get_cluster_info(
        jobs=run.jobs,
        replica_num=job.job_spec.replica_num,
        job_provisioning_data=job_provisioning_data,
    )

    volumes = await get_job_volumes(
        session=session,
        project=project,
        run_spec=run.run_spec,
        job_provisioning_data=job_provisioning_data,
    )

    server_ssh_private_key = project.ssh_private_key
    # TODO: Drop this logic and always use project key once it's safe to assume that most on-prem
    # fleets are (re)created after this change: https://github.com/dstackai/dstack/pull/1716
    if (
        job_model.instance is not None
        and job_model.instance.remote_connection_info is not None
        and job_provisioning_data.dockerized
    ):
        remote_conn_info: RemoteConnectionInfo = RemoteConnectionInfo.__response__.parse_raw(
            job_model.instance.remote_connection_info
        )
        server_ssh_private_key = remote_conn_info.ssh_keys[0].private

    secrets = {}  # TODO secrets

    repo_creds_model = await get_repo_creds(session=session, repo=repo_model, user=run_model.user)
    repo_creds = repo_model_to_repo_head_with_creds(repo_model, repo_creds_model).repo_creds

    initial_status = job_model.status
    if initial_status == JobStatus.PROVISIONING:
        if job_provisioning_data.hostname is None:
            await _wait_for_instance_provisioning_data(job_model=job_model)
        else:
            # Wait until all other jobs in the replica have IPs assigned.
            # This is needed to ensure cluster_info has all IPs set.
            for other_job in run.jobs:
                if (
                    other_job.job_spec.replica_num == job.job_spec.replica_num
                    and other_job.job_submissions[-1].status == JobStatus.PROVISIONING
                    and other_job.job_submissions[-1].job_provisioning_data is not None
                    and other_job.job_submissions[-1].job_provisioning_data.hostname is None
                ):
                    job_model.last_processed_at = common_utils.get_current_datetime()
                    await session.commit()
                    return

            # fails are acceptable until timeout is exceeded
            if job_provisioning_data.dockerized:
                logger.debug(
                    "%s: process provisioning job with shim, age=%s",
                    fmt(job_model),
                    job_submission.age,
                )
                ssh_user = job_provisioning_data.username
                user_ssh_key = run.run_spec.ssh_key_pub.strip()
                public_keys = [project.ssh_public_key.strip(), user_ssh_key]
                if job_provisioning_data.backend == BackendType.LOCAL:
                    # No need to update ~/.ssh/authorized_keys when running shim localy
                    user_ssh_key = ""
                success = await common_utils.run_async(
                    _process_provisioning_with_shim,
                    server_ssh_private_key,
                    job_provisioning_data,
                    None,
                    run,
                    job_model,
                    volumes,
                    secrets,
                    job.job_spec.registry_auth,
                    public_keys,
                    ssh_user,
                    user_ssh_key,
                )
            else:
                logger.debug(
                    "%s: process provisioning job without shim, age=%s",
                    fmt(job_model),
                    job_submission.age,
                )
                code = await _get_job_code(
                    session=session,
                    project=project,
                    repo=repo_model,
                    code_hash=run.run_spec.repo_code_hash,
                )
                success = await common_utils.run_async(
                    _submit_job_to_runner,
                    server_ssh_private_key,
                    job_provisioning_data,
                    None,
                    run,
                    job_model,
                    job,
                    cluster_info,
                    code,
                    secrets,
                    repo_creds,
                    success_if_not_available=False,
                )

            if not success:
                # check timeout
                if job_submission.age > _get_runner_timeout_interval(
                    backend_type=job_provisioning_data.get_base_backend(),
                    instance_type_name=job_provisioning_data.instance_type.name,
                ):
                    logger.warning(
                        "%s: failed because runner has not become available in time, age=%s",
                        fmt(job_model),
                        job_submission.age,
                    )
                    job_model.status = JobStatus.TERMINATING
                    job_model.termination_reason = (
                        JobTerminationReason.WAITING_RUNNER_LIMIT_EXCEEDED
                    )
                    # instance will be emptied by process_terminating_jobs

    else:  # fails are not acceptable
        if initial_status == JobStatus.PULLING:
            logger.debug(
                "%s: process pulling job with shim, age=%s", fmt(job_model), job_submission.age
            )
            code = await _get_job_code(
                session=session,
                project=project,
                repo=repo_model,
                code_hash=run.run_spec.repo_code_hash,
            )
            success = await common_utils.run_async(
                _process_pulling_with_shim,
                server_ssh_private_key,
                job_provisioning_data,
                None,
                run,
                job_model,
                job,
                cluster_info,
                code,
                secrets,
                repo_creds,
                server_ssh_private_key,
                job_provisioning_data,
            )
        elif initial_status == JobStatus.RUNNING:
            logger.debug("%s: process running job, age=%s", fmt(job_model), job_submission.age)
            success = await common_utils.run_async(
                _process_running,
                server_ssh_private_key,
                job_provisioning_data,
                job_submission.job_runtime_data,
                run_model,
                job_model,
            )
            if not success:
                job_model.termination_reason = JobTerminationReason.INTERRUPTED_BY_NO_CAPACITY

        if not success:  # kill the job
            logger.warning(
                "%s: failed because runner is not available or return an error,  age=%s",
                fmt(job_model),
                job_submission.age,
            )
            job_model.status = JobStatus.TERMINATING
            if not job_model.termination_reason:
                job_model.termination_reason = JobTerminationReason.INTERRUPTED_BY_NO_CAPACITY
            # job will be terminated and instance will be emptied by process_terminating_jobs

    if (
        initial_status != job_model.status
        and job_model.status == JobStatus.RUNNING
        and job_model.job_num == 0  # gateway connects only to the first node
        and run.run_spec.configuration.type == "service"
    ):
        try:
            await services.register_replica(session, run_model.gateway_id, run, job_model)
        except GatewayError as e:
            logger.warning(
                "%s: failed to register service replica: %s, age=%s",
                fmt(job_model),
                e,
                job_submission.age,
            )
            job_model.status = JobStatus.TERMINATING
            job_model.termination_reason = JobTerminationReason.GATEWAY_ERROR

    job_model.last_processed_at = common_utils.get_current_datetime()
    await session.commit()


async def _wait_for_instance_provisioning_data(job_model: JobModel):
    """
    This function will be called until instance IP address appears
    in `job_model.instance.job_provisioning_data` or instance is terminated on timeout.
    """
    if job_model.instance is None:
        logger.error(
            "%s: cannot update job_provisioning_data. job_model.instance is None.",
            fmt(job_model),
        )
        return
    if job_model.instance.job_provisioning_data is None:
        logger.error(
            "%s: cannot update job_provisioning_data. job_model.job_provisioning_data is None.",
            fmt(job_model),
        )
        return

    if job_model.instance.status == InstanceStatus.TERMINATED:
        job_model.status = JobStatus.TERMINATING
        # TODO use WAITING_INSTANCE_LIMIT_EXCEEDED after 0.19.x
        job_model.termination_reason = JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY
        return

    job_model.job_provisioning_data = job_model.instance.job_provisioning_data


@runner_ssh_tunnel(ports=[DSTACK_SHIM_HTTP_PORT], retries=1)
def _process_provisioning_with_shim(
    ports: Dict[int, int],
    run: Run,
    job_model: JobModel,
    volumes: List[Volume],
    secrets: Dict[str, str],
    registry_auth: Optional[RegistryAuth],
    public_keys: List[str],
    ssh_user: str,
    ssh_key: str,
) -> bool:
    """
    Possible next states:
    - JobStatus.PULLING if shim is available
    - JobStatus.TERMINATING if timeout is exceeded

    Returns:
        is successful
    """
    job_spec = JobSpec.__response__.parse_raw(job_model.job_spec_data)

    shim_client = client.ShimClient(port=ports[DSTACK_SHIM_HTTP_PORT])

    resp = shim_client.healthcheck()
    if resp is None:
        logger.debug("%s: shim is not available yet", fmt(job_model))
        return False  # shim is not available yet

    registry_username = ""
    registry_password = ""
    if registry_auth is not None:
        logger.debug("%s: authenticating to the registry...", fmt(job_model))
        interpolate = VariablesInterpolator({"secrets": secrets}).interpolate
        registry_username = interpolate(registry_auth.username)
        registry_password = interpolate(registry_auth.password)

    volume_mounts: List[VolumeMountPoint] = []
    instance_mounts: List[InstanceMountPoint] = []
    for mount in run.run_spec.configuration.volumes:
        if is_core_model_instance(mount, VolumeMountPoint):
            volume_mounts.append(mount.copy())
        elif is_core_model_instance(mount, InstanceMountPoint):
            instance_mounts.append(mount)
        else:
            assert False, f"unexpected mount point: {mount!r}"

    # Run configuration may specify list of possible volume names.
    # We should resolve in to the actual volume attached.
    for volume, volume_mount in zip(volumes, volume_mounts):
        volume_mount.name = volume.name

    container_user = "root"

    job_runtime_data = get_job_runtime_data(job_model)
    # should check for None, as there may be older jobs submitted before
    # JobRuntimeData was introduced
    if job_runtime_data is not None:
        gpu = job_runtime_data.gpu
        cpu = job_runtime_data.cpu
        memory = job_runtime_data.memory
        network_mode = job_runtime_data.network_mode
    else:
        gpu = None
        cpu = None
        memory = None
        network_mode = NetworkMode.HOST

    if shim_client.is_api_v2_supported():
        shim_client.submit_task(
            task_id=job_model.id,
            name=job_model.job_name,
            registry_username=registry_username,
            registry_password=registry_password,
            image_name=job_spec.image_name,
            container_user=container_user,
            privileged=job_spec.privileged,
            gpu=gpu,
            cpu=cpu,
            memory=memory,
            shm_size=job_spec.requirements.resources.shm_size,
            network_mode=network_mode,
            volumes=volumes,
            volume_mounts=volume_mounts,
            instance_mounts=instance_mounts,
            host_ssh_user=ssh_user,
            host_ssh_keys=[ssh_key] if ssh_key else [],
            container_ssh_keys=public_keys,
        )
    else:
        submitted = shim_client.submit(
            username=registry_username,
            password=registry_password,
            image_name=job_spec.image_name,
            privileged=job_spec.privileged,
            container_name=job_model.job_name,
            container_user=container_user,
            shm_size=job_spec.requirements.resources.shm_size,
            public_keys=public_keys,
            ssh_user=ssh_user,
            ssh_key=ssh_key,
            mounts=volume_mounts,
            volumes=volumes,
            instance_mounts=instance_mounts,
        )
        if not submitted:
            # This can happen when we lost connection to the runner (e.g., network issues), marked
            # the job as failed, released the instance (status=BUSY->IDLE, job_id={id}->None),
            # but the job container is in fact alive, running the previous job. As we force-stop
            # the container via shim API when cancelling the current job anyway (when either the
            # user aborts the submission process or the submission deadline is reached), it's safe
            # to kill the previous job container now, making the shim available
            # (state=running->pending) for the next try.
            logger.warning(
                "%s: failed to submit, shim is already running a job, stopping it now, retry later",
                fmt(job_model),
            )
            shim_client.stop(force=True)
            return False

    job_model.status = JobStatus.PULLING
    logger.info("%s: now is %s", fmt(job_model), job_model.status.name)
    return True


@runner_ssh_tunnel(ports=[DSTACK_SHIM_HTTP_PORT])
def _process_pulling_with_shim(
    ports: Dict[int, int],
    run: Run,
    job_model: JobModel,
    job: Job,
    cluster_info: ClusterInfo,
    code: bytes,
    secrets: Dict[str, str],
    repo_credentials: Optional[RemoteRepoCreds],
    server_ssh_private_key: str,
    job_provisioning_data: JobProvisioningData,
) -> bool:
    """
    Possible next states:
    - JobStatus.RUNNING if runner is available
    - JobStatus.TERMINATING if shim is not available

    Returns:
        is successful
    """
    shim_client = client.ShimClient(port=ports[DSTACK_SHIM_HTTP_PORT])
    if shim_client.is_api_v2_supported():  # raises error if shim is down, causes retry
        task = shim_client.get_task(job_model.id)

        # If task goes to terminated before the job is submitted to runner, then an error occured
        if task.status == TaskStatus.TERMINATED:
            logger.warning(
                "shim failed to execute job %s: %s (%s)",
                job_model.job_name,
                task.termination_reason,
                task.termination_message,
            )
            logger.debug("task status: %s", task.dict())
            job_model.termination_reason = JobTerminationReason(task.termination_reason.lower())
            job_model.termination_reason_message = task.termination_message
            return False

        if task.status != TaskStatus.RUNNING:
            return True

        job_runtime_data = get_job_runtime_data(job_model)
        # should check for None, as there may be older jobs submitted before
        # JobRuntimeData was introduced
        if job_runtime_data is not None:
            # port mapping is not yet available, waiting
            if task.ports is None:
                return True
            job_runtime_data.ports = {pm.container: pm.host for pm in task.ports}
            job_model.job_runtime_data = job_runtime_data.json()

    else:
        shim_status = shim_client.pull()  # raises error if shim is down, causes retry

        # If shim goes to pending before the job is submitted to runner, then an error occured
        if (
            shim_status.state == "pending"
            and shim_status.result is not None
            and shim_status.result.reason != ""
        ):
            logger.warning(
                "shim failed to execute job %s: %s (%s)",
                job_model.job_name,
                shim_status.result.reason,
                shim_status.result.reason_message,
            )
            logger.debug("shim status: %s", shim_status.dict())
            job_model.termination_reason = JobTerminationReason(shim_status.result.reason.lower())
            job_model.termination_reason_message = shim_status.result.reason_message
            return False

        if shim_status.state in ("pulling", "creating"):
            return True

    return _submit_job_to_runner(
        server_ssh_private_key,
        job_provisioning_data,
        job_runtime_data,
        run=run,
        job_model=job_model,
        job=job,
        cluster_info=cluster_info,
        code=code,
        secrets=secrets,
        repo_credentials=repo_credentials,
        success_if_not_available=True,
    )


@runner_ssh_tunnel(ports=[DSTACK_RUNNER_HTTP_PORT])
def _process_running(
    ports: Dict[int, int],
    run_model: RunModel,
    job_model: JobModel,
) -> bool:
    """
    Possible next states:
    - JobStatus.TERMINATING if runner is not available
    - Any status received from runner

    Returns:
        is successful
    """
    runner_client = client.RunnerClient(port=ports[DSTACK_RUNNER_HTTP_PORT])
    timestamp = 0
    if job_model.runner_timestamp is not None:
        timestamp = job_model.runner_timestamp
    resp = runner_client.pull(timestamp)  # raises error if runner is down, causes retry
    job_model.runner_timestamp = resp.last_updated
    # may raise LogStorageError, causing a retry
    logs_services.write_logs(
        project=run_model.project,
        run_name=run_model.run_name,
        job_submission_id=job_model.id,
        runner_logs=resp.runner_logs,
        job_logs=resp.job_logs,
    )
    if len(resp.job_states) > 0:
        latest_state_event = resp.job_states[-1]
        latest_status = latest_state_event.state
        if latest_status == JobStatus.DONE:
            job_model.status = JobStatus.TERMINATING
            job_model.termination_reason = JobTerminationReason.DONE_BY_RUNNER
        elif latest_status in {JobStatus.FAILED, JobStatus.TERMINATED}:
            job_model.status = JobStatus.TERMINATING
            job_model.termination_reason = JobTerminationReason.CONTAINER_EXITED_WITH_ERROR
            if latest_state_event.termination_reason:
                job_model.termination_reason = JobTerminationReason(
                    latest_state_event.termination_reason.lower()
                )
            if latest_state_event.termination_message:
                job_model.termination_reason_message = latest_state_event.termination_message
        logger.info("%s: now is %s", fmt(job_model), job_model.status.name)
    return True


def _get_cluster_info(
    jobs: List[Job],
    replica_num: int,
    job_provisioning_data: JobProvisioningData,
) -> ClusterInfo:
    job_ips = []
    for job in jobs:
        if job.job_spec.replica_num == replica_num:
            job_ips.append(
                common_utils.get_or_error(
                    job.job_submissions[-1].job_provisioning_data
                ).internal_ip
                or ""
            )
    cluster_info = ClusterInfo(
        job_ips=job_ips,
        master_job_ip=job_ips[0],
        gpus_per_job=len(job_provisioning_data.instance_type.resources.gpus),
    )
    return cluster_info


async def _get_job_code(
    session: AsyncSession, project: ProjectModel, repo: RepoModel, code_hash: str
) -> bytes:
    code_model = await get_code_model(session=session, repo=repo, code_hash=code_hash)
    if code_model is None:
        return b""
    storage = get_default_storage()
    if storage is None or code_model.blob is not None:
        return code_model.blob
    blob = await common_utils.run_async(
        storage.get_code,
        project.name,
        repo.name,
        code_hash,
    )
    return blob


@runner_ssh_tunnel(ports=[DSTACK_RUNNER_HTTP_PORT], retries=1)
def _submit_job_to_runner(
    ports: Dict[int, int],
    run: Run,
    job_model: JobModel,
    job: Job,
    cluster_info: ClusterInfo,
    code: bytes,
    secrets: Dict[str, str],
    repo_credentials: Optional[RemoteRepoCreds],
    success_if_not_available: bool,
) -> bool:
    """
    Possible next states:
    - JobStatus.RUNNING if runner is available
    - JobStatus.TERMINATING if timeout is exceeded

    Returns:
        is successful
    """
    logger.debug("%s: submitting job spec", fmt(job_model))
    logger.debug(
        "%s: repo clone URL is %s",
        fmt(job_model),
        None if repo_credentials is None else repo_credentials.clone_url,
    )
    instance = job_model.instance
    if instance is not None and instance.remote_connection_info is not None:
        remote_info = RemoteConnectionInfo.__response__.parse_raw(instance.remote_connection_info)
        instance_env = remote_info.env
    else:
        instance_env = None

    runner_client = client.RunnerClient(port=ports[DSTACK_RUNNER_HTTP_PORT])
    resp = runner_client.healthcheck()
    if resp is None:
        # runner is not available yet
        return success_if_not_available

    runner_client.submit_job(
        run_spec=run.run_spec,
        job_spec=job.job_spec,
        cluster_info=cluster_info,
        secrets=secrets,
        repo_credentials=repo_credentials,
        instance_env=instance_env,
    )
    logger.debug("%s: uploading code", fmt(job_model))
    runner_client.upload_code(code)
    logger.debug("%s: starting job", fmt(job_model))
    runner_client.run_job()

    job_model.status = JobStatus.RUNNING
    # do not log here, because the runner will send a new status

    return True


def _get_runner_timeout_interval(backend_type: BackendType, instance_type_name: str) -> timedelta:
    # when changing timeouts, also consider process_instances._get_instance_timeout_interval
    if backend_type == BackendType.LAMBDA:
        return timedelta(seconds=1200)
    if backend_type == BackendType.KUBERNETES:
        return timedelta(seconds=1200)
    if backend_type == BackendType.OCI and instance_type_name.startswith("BM."):
        return timedelta(seconds=1200)
    if backend_type == BackendType.VULTR and instance_type_name.startswith("vbm"):
        return timedelta(seconds=3300)
    return timedelta(seconds=600)
