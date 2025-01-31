import asyncio
import uuid
from typing import List, Optional, Tuple

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, lazyload, selectinload

from dstack._internal.core.backends.base import Backend
from dstack._internal.core.errors import BackendError, ServerClientError
from dstack._internal.core.models.common import NetworkMode
from dstack._internal.core.models.fleets import (
    FleetConfiguration,
    FleetSpec,
    FleetStatus,
    InstanceGroupPlacement,
)
from dstack._internal.core.models.instances import (
    InstanceOfferWithAvailability,
    InstanceSharedOffer,
    InstanceStatus,
)
from dstack._internal.core.models.profiles import (
    DEFAULT_POOL_NAME,
    DEFAULT_RUN_TERMINATION_IDLE_TIME,
    CreationPolicy,
    Profile,
    TerminationPolicy,
)
from dstack._internal.core.models.resources import Memory
from dstack._internal.core.models.runs import (
    Job,
    JobProvisioningData,
    JobRuntimeData,
    JobStatus,
    JobTerminationReason,
    Run,
    RunSpec,
)
from dstack._internal.core.models.volumes import Volume
from dstack._internal.core.services.profiles import get_termination
from dstack._internal.server.db import get_db, get_session_ctx
from dstack._internal.server.models import (
    FleetModel,
    InstanceModel,
    JobModel,
    PoolModel,
    ProjectModel,
    RunModel,
    VolumeModel,
)
from dstack._internal.server.services.backends import get_project_backend_by_type_or_error
from dstack._internal.server.services.fleets import (
    fleet_model_to_fleet,
)
from dstack._internal.server.services.jobs import (
    find_job,
    get_instances_ids_with_detaching_volumes,
    get_job_runtime_data,
)
from dstack._internal.server.services.locking import get_locker
from dstack._internal.server.services.logging import fmt
from dstack._internal.server.services.offers import get_offers_by_requirements
from dstack._internal.server.services.pools import (
    filter_pool_instances,
    get_instance_provisioning_data,
    get_instance_shared_info,
    get_shared_pool_instances_with_offers,
)
from dstack._internal.server.services.runs import (
    check_can_attach_run_volumes,
    check_run_spec_has_instance_mounts,
    get_offer_volumes,
    get_run_volume_models,
    get_run_volumes,
    run_model_to_run,
)
from dstack._internal.server.services.volumes import (
    volume_model_to_volume,
)
from dstack._internal.utils import common as common_utils
from dstack._internal.utils.logging import get_logger

logger = get_logger(__name__)


async def process_submitted_jobs(batch_size: int = 1):
    tasks = []
    for _ in range(batch_size):
        tasks.append(_process_next_submitted_job())
    await asyncio.gather(*tasks)


async def _process_next_submitted_job():
    lock, lockset = get_locker().get_lockset(JobModel.__tablename__)
    async with get_session_ctx() as session:
        async with lock:
            res = await session.execute(
                select(JobModel)
                .where(
                    JobModel.status == JobStatus.SUBMITTED,
                    JobModel.id.not_in(lockset),
                )
                .order_by(JobModel.last_processed_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job_model = res.scalar()
            if job_model is None:
                return
            lockset.add(job_model.id)
        try:
            job_model_id = job_model.id
            await _process_submitted_job(session=session, job_model=job_model)
        finally:
            lockset.difference_update([job_model_id])


async def _process_submitted_job(session: AsyncSession, job_model: JobModel):
    logger.debug("%s: provisioning has started", fmt(job_model))
    # Refetch to load related attributes.
    # joinedload produces LEFT OUTER JOIN that can't be used with FOR UPDATE.
    res = await session.execute(
        select(JobModel).where(JobModel.id == job_model.id).options(joinedload(JobModel.instance))
    )
    job_model = res.unique().scalar_one()
    res = await session.execute(
        select(RunModel)
        .where(RunModel.id == job_model.run_id)
        .options(joinedload(RunModel.project).joinedload(ProjectModel.backends))
        .options(joinedload(RunModel.user))
        .options(joinedload(RunModel.fleet).joinedload(FleetModel.instances))
    )
    run_model = res.unique().scalar_one()
    project = run_model.project
    run_spec = RunSpec.__response__.parse_raw(run_model.run_spec)
    profile = run_spec.merged_profile

    run = run_model_to_run(run_model)
    job = find_job(run.jobs, job_model.replica_num, job_model.job_num)

    master_job = find_job(run.jobs, job_model.replica_num, 0)
    master_job_provisioning_data = None
    if job.job_spec.job_num != 0:
        if master_job.job_submissions[-1].job_provisioning_data is None:
            logger.debug("%s: waiting for master job to be provisioned", fmt(job_model))
            job_model.last_processed_at = common_utils.get_current_datetime()
            await session.commit()
            return
        master_job_provisioning_data = JobProvisioningData.__response__.parse_obj(
            master_job.job_submissions[-1].job_provisioning_data
        )
    if job.job_spec.job_num != 0 or job.job_spec.replica_num != 0:
        if run_model.fleet_id is None:
            logger.debug("%s: waiting for the run to be assigned to the fleet", fmt(job_model))
            job_model.last_processed_at = common_utils.get_current_datetime()
            await session.commit()
            return
    try:
        volume_models = await get_run_volume_models(
            session=session,
            project=project,
            run_spec=run_spec,
        )
        volumes = await get_run_volumes(
            session=session,
            project=project,
            run_spec=run_spec,
        )
        check_can_attach_run_volumes(run_spec=run_spec, volumes=volumes)
    except ServerClientError as e:
        logger.warning("%s: failed to prepare run volumes: %s", fmt(job_model), repr(e))
        job_model.status = JobStatus.TERMINATING
        # TODO: Replace with JobTerminationReason.VOLUME_ERROR in 0.19
        job_model.termination_reason = JobTerminationReason.TERMINATED_BY_SERVER
        job_model.termination_reason_message = e.msg
        job_model.last_processed_at = common_utils.get_current_datetime()
        await session.commit()
        return

    pool = await _get_pool(session=session, project=project, profile=profile)

    # Submitted jobs processing happens in two steps (transactions).
    # First, the jobs gets an instance assigned (or no instance).
    # Then, the job runs on the assigned instance or a new instance is provisioned.
    # This is needed to avoid holding instances lock for a long time.
    if not job_model.instance_assigned:
        # Try assigning instances from the pool.
        res = await session.execute(
            select(InstanceModel)
            .where(
                InstanceModel.pool_id == pool.id,
                InstanceModel.deleted == False,
                or_(
                    ~InstanceModel.jobs.any(),
                    InstanceModel.shared_info.is_not(None),
                ),
            )
            .options(lazyload(InstanceModel.jobs))
            .with_for_update()
        )
        pool_instances = list(res.unique().scalars().all())
        instances_ids = sorted([i.id for i in pool_instances])
        if get_db().dialect_name == "sqlite":
            # Start new transaction to see commited changes after lock
            await session.commit()
        async with get_locker().lock_ctx(InstanceModel.__tablename__, instances_ids):
            # If another job freed the instance but is still trying to detach volumes,
            # do not provision on it to prevent attaching volumes that are currently detaching.
            detaching_instances_ids = await get_instances_ids_with_detaching_volumes(session)
            # Refetch after lock
            res = await session.execute(
                select(InstanceModel).where(
                    InstanceModel.id.not_in(detaching_instances_ids),
                    InstanceModel.id.in_(instances_ids),
                    InstanceModel.deleted == False,
                    or_(
                        ~InstanceModel.jobs.any(),
                        InstanceModel.shared_info.is_not(None),
                    ),
                )
            )
            pool_instances = list(res.unique().scalars().all())
            instance = await _assign_job_to_pool_instance(
                session=session,
                pool_instances=pool_instances,
                run_spec=run_spec,
                job_model=job_model,
                job=job,
                fleet_model=run_model.fleet,
                master_job_provisioning_data=master_job_provisioning_data,
                volumes=volumes,
            )
            job_model.instance_assigned = True
            job_model.last_processed_at = common_utils.get_current_datetime()
            await session.commit()
            return

    if job_model.instance is not None:
        res = await session.execute(
            select(InstanceModel)
            .where(InstanceModel.id == job_model.instance.id)
            .options(selectinload(InstanceModel.volumes))
            .execution_options(populate_existing=True)
        )
        instance = res.unique().scalar_one()
        job_model.status = JobStatus.PROVISIONING
    else:
        # Assigned no instance, create a new one
        if profile.creation_policy == CreationPolicy.REUSE:
            logger.debug("%s: reuse instance failed", fmt(job_model))
            job_model.status = JobStatus.TERMINATING
            job_model.termination_reason = JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY
            job_model.last_processed_at = common_utils.get_current_datetime()
            await session.commit()
            return

        # Create a new cloud instance
        run_job_result = await _run_job_on_new_instance(
            project=project,
            fleet_model=run_model.fleet,
            job_model=job_model,
            run=run,
            job=job,
            project_ssh_public_key=project.ssh_public_key,
            project_ssh_private_key=project.ssh_private_key,
            master_job_provisioning_data=master_job_provisioning_data,
            volumes=volumes,
        )
        if run_job_result is None:
            logger.debug("%s: provisioning failed", fmt(job_model))
            job_model.status = JobStatus.TERMINATING
            job_model.termination_reason = JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY
            job_model.last_processed_at = common_utils.get_current_datetime()
            await session.commit()
            return

        logger.info("%s: now is provisioning a new instance", fmt(job_model))
        job_provisioning_data, offer = run_job_result
        job_model.job_provisioning_data = job_provisioning_data.json()
        job_model.status = JobStatus.PROVISIONING
        fleet_model = _get_or_create_fleet_model_for_job(
            project=project,
            run_model=run_model,
            run=run,
        )
        instance_num = await _get_next_instance_num(
            session=session,
            fleet_model=fleet_model,
        )
        instance = _create_instance_model_for_job(
            project=project,
            pool=pool,
            fleet_model=fleet_model,
            run_spec=run_spec,
            job_model=job_model,
            job=job,
            job_provisioning_data=job_provisioning_data,
            offer=offer,
            instance_num=instance_num,
        )
        job_model.job_runtime_data = _prepare_job_runtime_data().json()
        instance.fleet_id = fleet_model.id
        logger.info(
            "The job %s created the new instance %s",
            job_model.job_name,
            instance.name,
            extra={
                "instance_name": instance.name,
                "instance_status": InstanceStatus.PROVISIONING.value,
            },
        )
        session.add(instance)
        session.add(fleet_model)
        job_model.used_instance_id = instance.id

    volumes_ids = sorted([v.id for vs in volume_models for v in vs])
    # TODO: lock instances for attaching volumes?
    # Take lock to prevent attaching volumes that are to be deleted.
    # If the volume was deleted before the lock, the volume will fail to attach and the job will fail.
    await session.execute(
        select(VolumeModel)
        .where(VolumeModel.id.in_(volumes_ids))
        .options(selectinload(VolumeModel.user))
        .with_for_update()
    )
    async with get_locker().lock_ctx(VolumeModel.__tablename__, volumes_ids):
        if len(volume_models) > 0:
            await _attach_volumes(
                session=session,
                project=project,
                job_model=job_model,
                instance=instance,
                volume_models=volume_models,
            )
        job_model.last_processed_at = common_utils.get_current_datetime()
        await session.commit()


async def _get_pool(session: AsyncSession, project: ProjectModel, profile: Profile) -> PoolModel:
    res = await session.execute(
        select(PoolModel)
        .where(
            PoolModel.project_id == project.id,
            PoolModel.name == (profile.pool_name or DEFAULT_POOL_NAME),
            PoolModel.deleted == False,
        )
        .options(lazyload(PoolModel.instances))
    )
    return res.scalar_one()


async def _assign_job_to_pool_instance(
    session: AsyncSession,
    pool_instances: List[InstanceModel],
    run_spec: RunSpec,
    job_model: JobModel,
    job: Job,
    fleet_model: Optional[FleetModel],
    master_job_provisioning_data: Optional[JobProvisioningData] = None,
    volumes: Optional[List[List[Volume]]] = None,
) -> Optional[InstanceModel]:
    instances_with_offers: list[tuple[InstanceModel, Optional[InstanceSharedOffer]]]
    profile = run_spec.merged_profile
    multinode = job.job_spec.jobs_per_replica > 1
    nonshared_instances = filter_pool_instances(
        pool_instances=pool_instances,
        profile=profile,
        requirements=job.job_spec.requirements,
        status=InstanceStatus.IDLE,
        fleet_model=fleet_model,
        multinode=multinode,
        master_job_provisioning_data=master_job_provisioning_data,
        volumes=volumes,
        shared=False,
    )
    instances_with_offers = [(instance, None) for instance in nonshared_instances]
    if not multinode:
        shared_instances_with_offers = get_shared_pool_instances_with_offers(
            pool_instances=pool_instances,
            profile=profile,
            requirements=job.job_spec.requirements,
            idle_only=True,
            fleet_model=fleet_model,
            volumes=volumes,
        )
        instances_with_offers.extend(shared_instances_with_offers)

    if len(instances_with_offers) == 0:
        return None

    instances_with_offers.sort(key=lambda instance_with_offer: instance_with_offer[0].price or 0)
    instance, offer = instances_with_offers[0]
    # Reload InstanceModel with volumes
    res = await session.execute(
        select(InstanceModel)
        .where(InstanceModel.id == instance.id)
        .options(joinedload(InstanceModel.volumes))
    )
    instance = res.unique().scalar_one()
    instance.status = InstanceStatus.BUSY
    if offer is not None:
        # instance is shared (split into blocks)
        shared_info = common_utils.get_or_error(get_instance_shared_info(instance))
        shared_info.busy_blocks += offer.blocks
        instance.shared_info = shared_info.json()

    logger.info(
        "The job %s switched instance %s status to BUSY",
        job_model.job_name,
        instance.name,
        extra={
            "instance_name": instance.name,
            "instance_status": InstanceStatus.BUSY.value,
        },
    )
    logger.info("%s: now is provisioning on '%s'", fmt(job_model), instance.name)
    job_model.instance = instance
    job_model.used_instance_id = instance.id
    job_model.job_provisioning_data = instance.job_provisioning_data
    job_model.job_runtime_data = _prepare_job_runtime_data(offer).json()
    return instance


async def _run_job_on_new_instance(
    project: ProjectModel,
    job_model: JobModel,
    run: Run,
    job: Job,
    project_ssh_public_key: str,
    project_ssh_private_key: str,
    master_job_provisioning_data: Optional[JobProvisioningData] = None,
    volumes: Optional[List[List[Volume]]] = None,
    fleet_model: Optional[FleetModel] = None,
) -> Optional[Tuple[JobProvisioningData, InstanceOfferWithAvailability]]:
    if volumes is None:
        volumes = []
    fleet = None
    if fleet_model is not None:
        fleet = fleet_model_to_fleet(fleet_model)
    multinode = job.job_spec.jobs_per_replica > 1 or (
        fleet is not None and fleet.spec.configuration.placement == InstanceGroupPlacement.CLUSTER
    )
    offers = await get_offers_by_requirements(
        project=project,
        profile=run.run_spec.merged_profile,
        requirements=job.job_spec.requirements,
        exclude_not_available=True,
        multinode=multinode,
        master_job_provisioning_data=master_job_provisioning_data,
        volumes=volumes,
        privileged=job.job_spec.privileged,
        instance_mounts=check_run_spec_has_instance_mounts(run.run_spec),
    )
    # Limit number of offers tried to prevent long-running processing
    # in case all offers fail.
    for backend, offer in offers[:15]:
        logger.debug(
            "%s: trying %s in %s/%s for $%0.4f per hour",
            fmt(job_model),
            offer.instance.name,
            offer.backend.value,
            offer.region,
            offer.price,
        )
        offer_volumes = get_offer_volumes(volumes, offer)
        try:
            job_provisioning_data = await common_utils.run_async(
                backend.compute().run_job,
                run,
                job,
                offer,
                project_ssh_public_key,
                project_ssh_private_key,
                offer_volumes,
            )
            return job_provisioning_data, offer
        except BackendError as e:
            logger.warning(
                "%s: %s launch in %s/%s failed: %s",
                fmt(job_model),
                offer.instance.name,
                offer.backend.value,
                offer.region,
                repr(e),
            )
            continue
        except Exception:
            logger.exception(
                "%s: got exception when launching %s in %s/%s",
                fmt(job_model),
                offer.instance.name,
                offer.backend.value,
                offer.region,
            )
            continue
    return None


def _get_or_create_fleet_model_for_job(
    project: ProjectModel,
    run_model: RunModel,
    run: Run,
) -> FleetModel:
    if run_model.fleet is not None:
        return run_model.fleet
    placement = InstanceGroupPlacement.ANY
    if run.run_spec.configuration.type == "task" and run.run_spec.configuration.nodes > 1:
        placement = InstanceGroupPlacement.CLUSTER
    spec = FleetSpec(
        configuration=FleetConfiguration(
            name=run.run_spec.run_name,
            placement=placement,
            reservation=run.run_spec.configuration.reservation,
        ),
        profile=run.run_spec.merged_profile,
        autocreated=True,
    )
    fleet_model = FleetModel(
        id=uuid.uuid4(),
        name=run.run_spec.run_name,
        project=project,
        status=FleetStatus.ACTIVE,
        spec=spec.json(),
        instances=[],
    )
    return fleet_model


async def _get_next_instance_num(session: AsyncSession, fleet_model: FleetModel) -> int:
    if len(fleet_model.instances) == 0:
        # No instances means the fleet is not in the db yet, so don't lock.
        return 0
    async with get_locker().lock_ctx(FleetModel.__tablename__, [fleet_model.id]):
        fleet_model = (
            (
                await session.execute(
                    select(FleetModel)
                    .where(FleetModel.id == fleet_model.id)
                    .options(joinedload(FleetModel.instances))
                    .execution_options(populate_existing=True)
                )
            )
            .unique()
            .scalar_one()
        )
        return len(fleet_model.instances)


def _create_instance_model_for_job(
    project: ProjectModel,
    pool: PoolModel,
    fleet_model: FleetModel,
    run_spec: RunSpec,
    job_model: JobModel,
    job: Job,
    job_provisioning_data: JobProvisioningData,
    offer: InstanceOfferWithAvailability,
    instance_num: int,
) -> InstanceModel:
    profile = run_spec.merged_profile
    if not job_provisioning_data.dockerized:
        # terminate vastai/k8s instances immediately
        termination_policy = TerminationPolicy.DESTROY_AFTER_IDLE
        termination_idle_time = 0
    else:
        termination_policy, termination_idle_time = get_termination(
            profile, DEFAULT_RUN_TERMINATION_IDLE_TIME
        )
    instance = InstanceModel(
        id=uuid.uuid4(),
        name=f"{fleet_model.name}-{instance_num}",
        instance_num=instance_num,
        project=project,
        pool=pool,
        created_at=common_utils.get_current_datetime(),
        started_at=common_utils.get_current_datetime(),
        status=InstanceStatus.PROVISIONING,
        unreachable=False,
        job_provisioning_data=job_provisioning_data.json(),
        offer=offer.json(),
        termination_policy=termination_policy,
        termination_idle_time=termination_idle_time,
        jobs=[job_model],
        backend=offer.backend,
        price=offer.price,
        region=offer.region,
        volumes=[],
    )
    return instance


def _prepare_job_runtime_data(
    shared_offer: Optional[InstanceSharedOffer] = None,
) -> JobRuntimeData:
    if shared_offer is None:
        return JobRuntimeData(network_mode=NetworkMode.HOST)
    return JobRuntimeData(
        network_mode=NetworkMode.BRIDGE,
        offer=shared_offer,
        cpu=shared_offer.instance.resources.cpus,
        gpu=len(shared_offer.instance.resources.gpus),
        memory=Memory(shared_offer.instance.resources.memory_mib / 1024),
    )


async def _attach_volumes(
    session: AsyncSession,
    project: ProjectModel,
    job_model: JobModel,
    instance: InstanceModel,
    volume_models: List[List[VolumeModel]],
):
    job_provisioning_data = common_utils.get_or_error(get_instance_provisioning_data(instance))
    backend = await get_project_backend_by_type_or_error(
        project=project,
        backend_type=job_provisioning_data.backend,
    )
    job_runtime_data = common_utils.get_or_error(get_job_runtime_data(job_model))
    job_runtime_data.volume_names = []
    logger.info("Attaching volumes: %s", [[v.name for v in vs] for vs in volume_models])
    for mount_point_volume_models in volume_models:
        for volume_model in mount_point_volume_models:
            volume = volume_model_to_volume(volume_model)
            try:
                if (
                    job_provisioning_data.get_base_backend() != volume.configuration.backend
                    or job_provisioning_data.region != volume.configuration.region
                ):
                    continue
                if volume.provisioning_data is not None and volume.provisioning_data.attachable:
                    await _attach_volume(
                        session=session,
                        backend=backend,
                        volume_model=volume_model,
                        instance=instance,
                        instance_id=job_provisioning_data.instance_id,
                    )
                    job_runtime_data.volume_names.append(volume.name)
                    break  # attach next mount point
            except (ServerClientError, BackendError) as e:
                logger.warning("%s: failed to attached volume: %s", fmt(job_model), repr(e))
                job_model.status = JobStatus.TERMINATING
                # TODO: Replace with JobTerminationReason.VOLUME_ERROR in 0.19
                job_model.termination_reason = JobTerminationReason.TERMINATED_BY_SERVER
                job_model.termination_reason_message = "Failed to attach volume"
            except Exception:
                logger.exception(
                    "%s: got exception when attaching volume",
                    fmt(job_model),
                )
                job_model.status = JobStatus.TERMINATING
                # TODO: Replace with JobTerminationReason.VOLUME_ERROR in 0.19
                job_model.termination_reason = JobTerminationReason.TERMINATED_BY_SERVER
                job_model.termination_reason_message = "Failed to attach volume"
            finally:
                job_model.job_runtime_data = job_runtime_data.json()


async def _attach_volume(
    session: AsyncSession,
    backend: Backend,
    volume_model: VolumeModel,
    instance: InstanceModel,
    instance_id: str,
):
    await session.refresh(volume_model)
    if volume_model.deleted:
        raise ServerClientError("Cannot attach a deleted volume")
    volume = volume_model_to_volume(volume_model)
    attachment_data = await common_utils.run_async(
        backend.compute().attach_volume,
        volume=volume,
        instance_id=instance_id,
    )
    volume_model.volume_attachment_data = attachment_data.json()
    instance.volumes.append(volume_model)
