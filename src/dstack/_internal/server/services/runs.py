import itertools
import math
import uuid
from datetime import datetime, timezone
from typing import List, Optional, Union

import pydantic
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, selectinload

import dstack._internal.utils.common as common_utils
from dstack._internal.core.errors import (
    RepoDoesNotExistError,
    ResourceNotExistsError,
    ServerClientError,
)
from dstack._internal.core.models.common import ApplyAction, is_core_model_instance
from dstack._internal.core.models.instances import (
    InstanceAvailability,
    InstanceOfferWithAvailability,
    InstanceSharedOffer,
    InstanceStatus,
)
from dstack._internal.core.models.profiles import (
    CreationPolicy,
)
from dstack._internal.core.models.repos.virtual import DEFAULT_VIRTUAL_REPO_ID, VirtualRunRepoData
from dstack._internal.core.models.runs import (
    ApplyRunPlanInput,
    Job,
    JobPlan,
    JobProvisioningData,
    JobSpec,
    JobStatus,
    JobSubmission,
    JobTerminationReason,
    Run,
    RunPlan,
    RunSpec,
    RunStatus,
    RunTerminationReason,
    ServiceSpec,
)
from dstack._internal.core.models.users import GlobalRole
from dstack._internal.core.models.volumes import (
    InstanceMountPoint,
    Volume,
    VolumeMountPoint,
    VolumeStatus,
)
from dstack._internal.core.services import validate_dstack_resource_name
from dstack._internal.core.services.diff import diff_models
from dstack._internal.server.db import get_db
from dstack._internal.server.models import (
    JobModel,
    PoolModel,
    ProjectModel,
    RepoModel,
    RunModel,
    UserModel,
    VolumeModel,
)
from dstack._internal.server.services import repos as repos_services
from dstack._internal.server.services import services
from dstack._internal.server.services import volumes as volumes_services
from dstack._internal.server.services.docker import is_valid_docker_volume_target
from dstack._internal.server.services.jobs import (
    delay_job_instance_termination,
    get_instances_ids_with_detaching_volumes,
    get_jobs_from_run_spec,
    group_jobs_by_replica_latest,
    job_model_to_job_submission,
    stop_runner,
)
from dstack._internal.server.services.locking import get_locker, string_to_lock_id
from dstack._internal.server.services.logging import fmt
from dstack._internal.server.services.offers import get_offers_by_requirements
from dstack._internal.server.services.pools import (
    filter_pool_instances,
    get_instance_offer,
    get_or_create_pool_by_name,
    get_pool_instances,
    get_shared_pool_instances_with_offers,
)
from dstack._internal.server.services.projects import list_project_models, list_user_project_models
from dstack._internal.server.services.users import get_user_model_by_name
from dstack._internal.utils.logging import get_logger
from dstack._internal.utils.random_names import generate_name

logger = get_logger(__name__)


JOB_TERMINATION_REASONS_TO_RETRY = {
    JobTerminationReason.INTERRUPTED_BY_NO_CAPACITY,
    JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY,
}


async def list_user_runs(
    session: AsyncSession,
    user: UserModel,
    project_name: Optional[str],
    repo_id: Optional[str],
    username: Optional[str],
    only_active: bool,
    prev_submitted_at: Optional[datetime],
    prev_run_id: Optional[uuid.UUID],
    limit: int,
    ascending: bool,
) -> List[Run]:
    if project_name is None and repo_id is not None:
        return []
    if user.global_role == GlobalRole.ADMIN:
        projects = await list_project_models(session=session)
    else:
        projects = await list_user_project_models(session=session, user=user)
    runs_user = None
    if username is not None:
        runs_user = await get_user_model_by_name(session=session, username=username)
        if runs_user is None:
            raise ResourceNotExistsError("User not found")
    repo = None
    if project_name is not None:
        projects = [p for p in projects if p.name == project_name]
        if len(projects) == 0:
            return []
        if repo_id is not None:
            repo = await repos_services.get_repo_model(
                session=session,
                project=projects[0],
                repo_id=repo_id,
            )
            if repo is None:
                raise RepoDoesNotExistError.with_id(repo_id)
    run_models = await list_projects_run_models(
        session=session,
        projects=projects,
        repo=repo,
        runs_user=runs_user,
        only_active=only_active,
        prev_submitted_at=prev_submitted_at,
        prev_run_id=prev_run_id,
        limit=limit,
        ascending=ascending,
    )
    runs = []
    for r in run_models:
        try:
            runs.append(run_model_to_run(r, return_in_api=True))
        except pydantic.ValidationError:
            pass
    if len(run_models) > len(runs):
        logger.debug("Can't load %s runs", len(run_models) - len(runs))
    return runs


async def list_projects_run_models(
    session: AsyncSession,
    projects: List[ProjectModel],
    repo: Optional[RepoModel],
    runs_user: Optional[UserModel],
    only_active: bool,
    prev_submitted_at: Optional[datetime],
    prev_run_id: Optional[uuid.UUID],
    limit: int,
    ascending: bool,
) -> List[RunModel]:
    filters = [RunModel.deleted == False, RunModel.project_id.in_(p.id for p in projects)]
    if repo is not None:
        filters.append(RunModel.repo_id == repo.id)
    if runs_user is not None:
        filters.append(RunModel.user_id == runs_user.id)
    if only_active:
        filters.append(RunModel.status.not_in(RunStatus.finished_statuses()))
    if prev_submitted_at is not None:
        if ascending:
            if prev_run_id is None:
                filters.append(RunModel.submitted_at > prev_submitted_at)
            else:
                filters.append(
                    or_(
                        RunModel.submitted_at > prev_submitted_at,
                        and_(
                            RunModel.submitted_at == prev_submitted_at, RunModel.id < prev_run_id
                        ),
                    )
                )
        else:
            if prev_run_id is None:
                filters.append(RunModel.submitted_at < prev_submitted_at)
            else:
                filters.append(
                    or_(
                        RunModel.submitted_at < prev_submitted_at,
                        and_(
                            RunModel.submitted_at == prev_submitted_at, RunModel.id > prev_run_id
                        ),
                    )
                )
    order_by = (RunModel.submitted_at.desc(), RunModel.id)
    if ascending:
        order_by = (RunModel.submitted_at.asc(), RunModel.id.desc())

    res = await session.execute(
        select(RunModel)
        .where(*filters)
        .order_by(*order_by)
        .limit(limit)
        .options(selectinload(RunModel.user))
    )
    run_models = list(res.scalars().all())
    return run_models


async def get_run(
    session: AsyncSession,
    project: ProjectModel,
    run_name: Optional[str] = None,
    run_id: Optional[uuid.UUID] = None,
) -> Optional[Run]:
    if run_id is not None:
        return await get_run_by_id(
            session=session,
            project=project,
            run_id=run_id,
        )
    elif run_name is not None:
        return await get_run_by_name(
            session=session,
            project=project,
            run_name=run_name,
        )
    raise ServerClientError("run_name or id must be specified")


async def get_run_by_name(
    session: AsyncSession,
    project: ProjectModel,
    run_name: str,
) -> Optional[Run]:
    res = await session.execute(
        select(RunModel)
        .where(
            RunModel.project_id == project.id,
            RunModel.run_name == run_name,
            RunModel.deleted == False,
        )
        .options(joinedload(RunModel.user))
    )
    run_model = res.scalar()
    if run_model is None:
        return None
    return run_model_to_run(run_model, return_in_api=True)


async def get_run_by_id(
    session: AsyncSession,
    project: ProjectModel,
    run_id: uuid.UUID,
) -> Optional[Run]:
    res = await session.execute(
        select(RunModel)
        .where(
            RunModel.project_id == project.id,
            RunModel.id == run_id,
        )
        .options(joinedload(RunModel.user))
    )
    run_model = res.scalar()
    if run_model is None:
        return None
    return run_model_to_run(run_model, return_in_api=True)


async def get_plan(
    session: AsyncSession,
    project: ProjectModel,
    user: UserModel,
    run_spec: RunSpec,
) -> RunPlan:
    _validate_run_spec_and_set_defaults(run_spec)

    profile = run_spec.merged_profile
    creation_policy = profile.creation_policy

    current_resource = None
    action = ApplyAction.CREATE
    if run_spec.run_name is not None:
        current_resource = await get_run_by_name(
            session=session,
            project=project,
            run_name=run_spec.run_name,
        )
        if (
            current_resource is not None
            and not current_resource.status.is_finished()
            and _can_update_run_spec(current_resource.run_spec, run_spec)
        ):
            action = ApplyAction.UPDATE

    # TODO(egor-s): do we need to generate all replicas here?
    jobs = await get_jobs_from_run_spec(run_spec, replica_num=0)

    volumes = await get_run_volumes(
        session=session,
        project=project,
        run_spec=run_spec,
    )

    pool = await get_or_create_pool_by_name(
        session=session, project=project, pool_name=profile.pool_name
    )
    pool_offers = await _get_pool_offers(
        session=session,
        pool=pool,
        run_spec=run_spec,
        job=jobs[0],
        volumes=volumes,
    )
    run_name = run_spec.run_name  # preserve run_name
    run_spec.run_name = "dry-run"  # will regenerate jobs on submission

    # Get offers once for all jobs
    offers = []
    if creation_policy == CreationPolicy.REUSE_OR_CREATE:
        offers = await get_offers_by_requirements(
            project=project,
            profile=profile,
            requirements=jobs[0].job_spec.requirements,
            exclude_not_available=False,
            multinode=jobs[0].job_spec.jobs_per_replica > 1,
            volumes=volumes,
            privileged=jobs[0].job_spec.privileged,
            instance_mounts=check_run_spec_has_instance_mounts(run_spec),
        )

    job_plans = []
    for job in jobs:
        job_offers: List[InstanceOfferWithAvailability] = []
        job_offers.extend(pool_offers)
        job_offers.extend(offer for _, offer in offers)
        job_offers.sort(key=lambda offer: not offer.availability.is_available())

        job_plan = JobPlan(
            job_spec=job.job_spec,
            offers=job_offers[:50],
            total_offers=len(job_offers),
            max_price=max((offer.price for offer in job_offers), default=None),
        )
        job_plans.append(job_plan)

    run_spec.run_name = run_name  # restore run_name
    run_plan = RunPlan(
        project_name=project.name,
        user=user.name,
        run_spec=run_spec,
        job_plans=job_plans,
        current_resource=current_resource,
        action=action,
    )
    return run_plan


async def apply_plan(
    session: AsyncSession,
    user: UserModel,
    project: ProjectModel,
    plan: ApplyRunPlanInput,
    force: bool,
) -> Run:
    _validate_run_spec_and_set_defaults(plan.run_spec)
    if plan.run_spec.run_name is None:
        return await submit_run(
            session=session,
            user=user,
            project=project,
            run_spec=plan.run_spec,
        )
    current_resource = await get_run_by_name(
        session=session,
        project=project,
        run_name=plan.run_spec.run_name,
    )
    if current_resource is None or current_resource.status.is_finished():
        return await submit_run(
            session=session,
            user=user,
            project=project,
            run_spec=plan.run_spec,
        )
    try:
        _check_can_update_run_spec(current_resource.run_spec, plan.run_spec)
    except ServerClientError:
        # The except is only needed to raise an appropriate error if run is active
        if not current_resource.status.is_finished():
            raise ServerClientError("Cannot override active run. Stop the run first.")
        raise
    if not force:
        if (
            plan.current_resource is None
            or plan.current_resource.id != current_resource.id
            or plan.current_resource.run_spec != current_resource.run_spec
        ):
            raise ServerClientError(
                "Failed to apply plan. Resource has been changed. Try again or use force apply."
            )
    # FIXME: potentially long write transaction
    # Avoid getting run_model after update
    await session.execute(
        update(RunModel)
        .where(RunModel.id == current_resource.id)
        .values(run_spec=plan.run_spec.json())
    )
    run = await get_run_by_name(
        session=session,
        project=project,
        run_name=plan.run_spec.run_name,
    )
    return common_utils.get_or_error(run)


async def submit_run(
    session: AsyncSession,
    user: UserModel,
    project: ProjectModel,
    run_spec: RunSpec,
) -> Run:
    _validate_run_spec_and_set_defaults(run_spec)
    repo = await _get_run_repo_or_error(
        session=session,
        project=project,
        run_spec=run_spec,
    )

    lock_namespace = f"run_names_{project.name}"
    if get_db().dialect_name == "sqlite":
        # Start new transaction to see commited changes after lock
        await session.commit()
    elif get_db().dialect_name == "postgresql":
        await session.execute(
            select(func.pg_advisory_xact_lock(string_to_lock_id(lock_namespace)))
        )

    lock, _ = get_locker().get_lockset(lock_namespace)
    async with lock:
        if run_spec.run_name is None:
            run_spec.run_name = await _generate_run_name(
                session=session,
                project=project,
            )
        else:
            await delete_runs(session=session, project=project, runs_names=[run_spec.run_name])

        await validate_run(
            session=session,
            user=user,
            project=project,
            run_spec=run_spec,
        )

        submitted_at = common_utils.get_current_datetime()
        run_model = RunModel(
            id=uuid.uuid4(),
            project_id=project.id,
            project=project,
            repo_id=repo.id,
            user_id=user.id,
            run_name=run_spec.run_name,
            submitted_at=submitted_at,
            status=RunStatus.SUBMITTED,
            run_spec=run_spec.json(),
            last_processed_at=submitted_at,
        )
        session.add(run_model)

        replicas = 1
        if run_spec.configuration.type == "service":
            replicas = run_spec.configuration.replicas.min
            await services.register_service(session, run_model, run_spec)

        for replica_num in range(replicas):
            jobs = await get_jobs_from_run_spec(run_spec, replica_num=replica_num)
            for job in jobs:
                job_model = create_job_model_for_new_submission(
                    run_model=run_model,
                    job=job,
                    status=JobStatus.SUBMITTED,
                )
                session.add(job_model)
        await session.commit()
        await session.refresh(run_model)

        run = run_model_to_run(run_model, return_in_api=True)
        return run


def create_job_model_for_new_submission(
    run_model: RunModel,
    job: Job,
    status: JobStatus,
) -> JobModel:
    now = common_utils.get_current_datetime()
    return JobModel(
        id=uuid.uuid4(),
        project_id=run_model.project_id,
        run_id=run_model.id,
        run_name=run_model.run_name,
        job_num=job.job_spec.job_num,
        job_name=f"{job.job_spec.job_name}",
        replica_num=job.job_spec.replica_num,
        submission_num=len(job.job_submissions),
        submitted_at=now,
        last_processed_at=now,
        status=status,
        termination_reason=None,
        job_spec_data=job.job_spec.json(),
        job_provisioning_data=None,
    )


async def stop_runs(
    session: AsyncSession,
    project: ProjectModel,
    runs_names: List[str],
    abort: bool,
):
    """
    If abort is False, jobs receive a signal to stop and run status will be changed as a reaction to jobs status change.
    If abort is True, run is marked as TERMINATED and process_runs will stop the jobs.
    """
    res = await session.execute(
        select(RunModel).where(
            RunModel.project_id == project.id,
            RunModel.run_name.in_(runs_names),
            RunModel.status.not_in(RunStatus.finished_statuses()),
        )
    )
    run_models = res.scalars().all()
    run_ids = sorted([r.id for r in run_models])
    res = await session.execute(select(JobModel).where(JobModel.run_id.in_(run_ids)))
    job_models = res.scalars().all()
    job_ids = sorted([j.id for j in job_models])
    await session.commit()
    async with (
        get_locker().lock_ctx(RunModel.__tablename__, run_ids),
        get_locker().lock_ctx(JobModel.__tablename__, job_ids),
    ):
        for run_model in run_models:
            await stop_run(session=session, run_model=run_model, abort=abort)


async def stop_run(session: AsyncSession, run_model: RunModel, abort: bool):
    res = await session.execute(
        select(RunModel)
        .where(RunModel.id == run_model.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    run_model = res.scalar_one()
    await session.execute(
        select(JobModel)
        .where(JobModel.run_id == run_model.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run_model.status.is_finished():
        return
    run_model.status = RunStatus.TERMINATING
    if abort:
        run_model.termination_reason = RunTerminationReason.ABORTED_BY_USER
    else:
        run_model.termination_reason = RunTerminationReason.STOPPED_BY_USER
    # process the run out of turn
    logger.debug("%s: terminating because %s", fmt(run_model), run_model.termination_reason.name)
    await process_terminating_run(session, run_model)
    run_model.last_processed_at = common_utils.get_current_datetime()
    await session.commit()


async def delete_runs(
    session: AsyncSession,
    project: ProjectModel,
    runs_names: List[str],
):
    res = await session.execute(
        select(RunModel).where(
            RunModel.project_id == project.id,
            RunModel.run_name.in_(runs_names),
        )
    )
    run_models = res.scalars().all()
    run_ids = sorted([r.id for r in run_models])
    await session.commit()
    async with get_locker().lock_ctx(RunModel.__tablename__, run_ids):
        res = await session.execute(
            select(RunModel).where(RunModel.id.in_(run_ids)).with_for_update()
        )
        run_models = res.scalars().all()
        active_runs = [r for r in run_models if not r.status.is_finished()]
        if len(active_runs) > 0:
            raise ServerClientError(
                msg=f"Cannot delete active runs: {[r.run_name for r in active_runs]}"
            )
        await session.execute(
            update(RunModel)
            .where(
                RunModel.project_id == project.id,
                RunModel.run_name.in_(runs_names),
            )
            .values(deleted=True)
        )
        await session.commit()


def run_model_to_run(
    run_model: RunModel, include_job_submissions: bool = True, return_in_api: bool = False
) -> Run:
    jobs: List[Job] = []
    run_jobs = sorted(run_model.jobs, key=lambda j: (j.replica_num, j.job_num, j.submission_num))
    for replica_num, replica_submissions in itertools.groupby(
        run_jobs, key=lambda j: j.replica_num
    ):
        for job_num, job_submissions in itertools.groupby(
            replica_submissions, key=lambda j: j.job_num
        ):
            job_spec = None
            submissions = []
            for job_model in job_submissions:
                if job_spec is None:
                    job_spec = JobSpec.__response__.parse_raw(job_model.job_spec_data)
                if include_job_submissions:
                    job_submission = job_model_to_job_submission(job_model)
                    if return_in_api:
                        # Set default non-None values for 0.18 backward-compatibility
                        # Remove in 0.19
                        if job_submission.job_provisioning_data is not None:
                            if job_submission.job_provisioning_data.hostname is None:
                                job_submission.job_provisioning_data.hostname = ""
                            if job_submission.job_provisioning_data.ssh_port is None:
                                job_submission.job_provisioning_data.ssh_port = 22
                    submissions.append(job_submission)
            if job_spec is not None:
                jobs.append(Job(job_spec=job_spec, job_submissions=submissions))

    run_spec = RunSpec.__response__.parse_raw(run_model.run_spec)

    latest_job_submission = None
    if include_job_submissions:
        # TODO(egor-s): does it make sense with replicas and multi-node?
        if jobs:
            latest_job_submission = jobs[0].job_submissions[-1]

    service_spec = None
    if run_model.service_spec is not None:
        service_spec = ServiceSpec.__response__.parse_raw(run_model.service_spec)

    run = Run(
        id=run_model.id,
        project_name=run_model.project.name,
        user=run_model.user.name,
        submitted_at=run_model.submitted_at.replace(tzinfo=timezone.utc),
        last_processed_at=run_model.last_processed_at.replace(tzinfo=timezone.utc),
        status=run_model.status,
        termination_reason=run_model.termination_reason,
        run_spec=run_spec,
        jobs=jobs,
        latest_job_submission=latest_job_submission,
        service=service_spec,
        deleted=run_model.deleted,
    )
    run.cost = _get_run_cost(run)
    return run


async def _get_pool_offers(
    session: AsyncSession,
    pool: PoolModel,
    run_spec: RunSpec,
    job: Job,
    volumes: List[List[Volume]],
) -> list[Union[InstanceSharedOffer, InstanceOfferWithAvailability]]:
    pool_offers: list[Union[InstanceSharedOffer, InstanceOfferWithAvailability]] = []

    detaching_instances_ids = await get_instances_ids_with_detaching_volumes(session)
    pool_instances = [i for i in get_pool_instances(pool) if i.id not in detaching_instances_ids]
    multinode = job.job_spec.jobs_per_replica > 1

    if not multinode:
        shared_instances_with_offers = get_shared_pool_instances_with_offers(
            pool_instances=pool_instances,
            profile=run_spec.merged_profile,
            requirements=job.job_spec.requirements,
            volumes=volumes,
        )
        for _, offer in shared_instances_with_offers:
            pool_offers.append(offer)

    nonshared_instances = filter_pool_instances(
        pool_instances=pool_instances,
        profile=run_spec.merged_profile,
        requirements=job.job_spec.requirements,
        multinode=multinode,
        volumes=volumes,
        shared=False,
    )
    for instance in nonshared_instances:
        offer = get_instance_offer(instance)
        if offer is None:
            continue
        offer.availability = InstanceAvailability.BUSY
        if instance.status == InstanceStatus.IDLE:
            offer.availability = InstanceAvailability.IDLE
        pool_offers.append(offer)

    pool_offers.sort(key=lambda offer: offer.price)
    return pool_offers


async def _generate_run_name(
    session: AsyncSession,
    project: ProjectModel,
) -> str:
    run_name_base = generate_name()
    idx = 1
    while True:
        res = await session.execute(
            select(RunModel).where(
                RunModel.project_id == project.id,
                RunModel.run_name == f"{run_name_base}-{idx}",
                RunModel.deleted == False,
            )
        )
        run_model = res.scalar()
        if run_model is None:
            return f"{run_name_base}-{idx}"
        idx += 1


async def validate_run(
    session: AsyncSession,
    user: UserModel,
    project: ProjectModel,
    run_spec: RunSpec,
):
    volumes = await get_run_volumes(
        session=session,
        project=project,
        run_spec=run_spec,
    )
    check_can_attach_run_volumes(
        run_spec=run_spec,
        volumes=volumes,
    )


async def get_run_volumes(
    session: AsyncSession,
    project: ProjectModel,
    run_spec: RunSpec,
) -> List[List[Volume]]:
    """
    Returns list of run volumes grouped by mount points.
    """
    volume_models = await get_run_volume_models(
        session=session,
        project=project,
        run_spec=run_spec,
    )
    return [
        [volumes_services.volume_model_to_volume(v) for v in mount_point_volume_models]
        for mount_point_volume_models in volume_models
    ]


async def get_run_volume_models(
    session: AsyncSession,
    project: ProjectModel,
    run_spec: RunSpec,
) -> List[List[VolumeModel]]:
    """
    Returns list of run volume models grouped by mount points.
    """
    if len(run_spec.configuration.volumes) == 0:
        return []
    volume_models = []
    for mount_point in run_spec.configuration.volumes:
        if not is_core_model_instance(mount_point, VolumeMountPoint):
            continue
        if isinstance(mount_point.name, str):
            names = [mount_point.name]
        else:
            names = mount_point.name
        mount_point_volume_models = []
        for name in names:
            volume_model = await volumes_services.get_project_volume_model_by_name(
                session=session,
                project=project,
                name=name,
            )
            if volume_model is None:
                raise ResourceNotExistsError(f"Volume {mount_point.name} not found")
            mount_point_volume_models.append(volume_model)
        volume_models.append(mount_point_volume_models)
    return volume_models


def check_can_attach_run_volumes(
    run_spec: RunSpec,
    volumes: List[List[Volume]],
):
    """
    Performs basic checks if volumes can be attached.
    This is useful to show error ASAP (when user submits the run).
    If the attachment is to fail anyway, the error will be handled when proccessing submitted jobs.
    """
    if len(volumes) == 0:
        return
    expected_backends = {v.configuration.backend for v in volumes[0]}
    expected_regions = {v.configuration.region for v in volumes[0]}
    for mount_point_volumes in volumes:
        backends = {v.configuration.backend for v in mount_point_volumes}
        regions = {v.configuration.region for v in mount_point_volumes}
        if backends != expected_backends:
            raise ServerClientError(
                "Volumes from different backends specified for different mount points"
            )
        if regions != expected_regions:
            raise ServerClientError(
                "Volumes from different regions specified for different mount points"
            )
        for volume in mount_point_volumes:
            if volume.status != VolumeStatus.ACTIVE:
                raise ServerClientError(f"Cannot mount volumes that are not active: {volume.name}")
    volumes_names = [v.name for vs in volumes for v in vs]
    if len(volumes_names) != len(set(volumes_names)):
        raise ServerClientError("Cannot attach the same volume at different mount points")


async def get_job_volumes(
    session: AsyncSession,
    project: ProjectModel,
    run_spec: RunSpec,
    job_provisioning_data: JobProvisioningData,
) -> List[Volume]:
    """
    Returns volumes attached to the job.
    """
    run_volumes = await get_run_volumes(
        session=session,
        project=project,
        run_spec=run_spec,
    )
    job_volumes = []
    for mount_point_volumes in run_volumes:
        job_volumes.append(get_job_mount_point_volume(mount_point_volumes, job_provisioning_data))
    return job_volumes


def get_job_mount_point_volume(
    volumes: List[Volume],
    job_provisioning_data: JobProvisioningData,
) -> Volume:
    """
    Returns the volume attached to the job among the list of possible mount point volumes.
    """
    for volume in volumes:
        if (
            volume.configuration.backend != job_provisioning_data.get_base_backend()
            or volume.configuration.region != job_provisioning_data.region
        ):
            continue
        if (
            volume.provisioning_data is not None
            and volume.provisioning_data.availability_zone is not None
            and job_provisioning_data.availability_zone is not None
            and volume.provisioning_data.availability_zone
            != job_provisioning_data.availability_zone
        ):
            continue
        return volume
    raise ServerClientError("Failed to find an eligible volume for the mount point")


def get_offer_volumes(
    volumes: List[List[Volume]],
    offer: InstanceOfferWithAvailability,
) -> List[Volume]:
    """
    Returns volumes suitable for the offer for each mount point.
    """
    offer_volumes = []
    for mount_point_volumes in volumes:
        offer_volumes.append(get_offer_mount_point_volume(mount_point_volumes, offer))
    return offer_volumes


def get_offer_mount_point_volume(
    volumes: List[Volume],
    offer: InstanceOfferWithAvailability,
) -> Volume:
    """
    Returns the first suitable volume for the offer among possible mount point volumes.
    """
    for volume in volumes:
        if (
            volume.configuration.backend != offer.backend
            or volume.configuration.region != offer.region
        ):
            continue
        return volume
    raise ServerClientError("Failed to find an eligible volume for the mount point")


def check_run_spec_has_instance_mounts(run_spec: RunSpec) -> bool:
    return any(
        is_core_model_instance(mp, InstanceMountPoint) for mp in run_spec.configuration.volumes
    )


async def _get_run_repo_or_error(
    session: AsyncSession,
    project: ProjectModel,
    run_spec: RunSpec,
) -> RepoModel:
    # Must be set by _validate_run_spec_and_set_defaults()
    repo_id = common_utils.get_or_error(run_spec.repo_id)
    repo_data = common_utils.get_or_error(run_spec.repo_data)
    if repo_data.repo_type == "virtual":
        repo = await repos_services.create_or_update_repo(
            session=session,
            project=project,
            repo_id=repo_id,
            repo_info=repo_data,
        )
    repo = await repos_services.get_repo_model(
        session=session,
        project=project,
        repo_id=repo_id,
    )
    if repo is None:
        raise RepoDoesNotExistError.with_id(repo_id)
    return repo


def _get_run_cost(run: Run) -> float:
    run_cost = math.fsum(
        _get_job_submission_cost(submission)
        for job in run.jobs
        for submission in job.job_submissions
    )
    return round(run_cost, 4)


def _get_job_submission_cost(job_submission: JobSubmission) -> float:
    if job_submission.job_provisioning_data is None:
        return 0
    duration_hours = job_submission.duration.total_seconds() / 3600
    return job_submission.job_provisioning_data.price * duration_hours


def _validate_run_spec_and_set_defaults(run_spec: RunSpec):
    if run_spec.run_name is not None:
        validate_dstack_resource_name(run_spec.run_name)
    for mount_point in run_spec.configuration.volumes:
        if not is_valid_docker_volume_target(mount_point.path):
            raise ServerClientError(f"Invalid volume mount path: {mount_point.path}")
        if mount_point.path.startswith("/workflow"):
            raise ServerClientError("Mounting volumes inside /workflow is not supported")
    if run_spec.repo_id is None and run_spec.repo_data is not None:
        raise ServerClientError("repo_data must not be set if repo_id is not set")
    if run_spec.repo_id is not None and run_spec.repo_data is None:
        raise ServerClientError("repo_id must not be set if repo_data is not set")
    # Some run_spec parameters have to be set here and not in the model defaults since
    # the client may not pass them or pass null, but they must be always present, e.g. for runner.
    if run_spec.repo_id is None:
        run_spec.repo_id = DEFAULT_VIRTUAL_REPO_ID
    if run_spec.repo_data is None:
        run_spec.repo_data = VirtualRunRepoData()


_UPDATABLE_SPEC_FIELDS = ["repo_code_hash", "configuration"]
# Most service fields can be updated via replica redeployment.
# TODO: Allow updating other fields when a rolling deployment is supported.
_UPDATABLE_CONFIGURATION_FIELDS = ["replicas", "scaling"]


def _can_update_run_spec(current_run_spec: RunSpec, new_run_spec: RunSpec) -> bool:
    try:
        _check_can_update_run_spec(current_run_spec, new_run_spec)
    except ServerClientError as e:
        logger.debug("Run cannot be updated: %s", repr(e))
        return False
    return True


def _check_can_update_run_spec(current_run_spec: RunSpec, new_run_spec: RunSpec):
    if (
        current_run_spec.configuration.type != "service"
        or new_run_spec.configuration.type != "service"
    ):
        raise ServerClientError("Can only update service run configuration")
    spec_diff = diff_models(current_run_spec, new_run_spec)
    changed_spec_fields = list(spec_diff.keys())
    for key in changed_spec_fields:
        if key not in _UPDATABLE_SPEC_FIELDS:
            raise ServerClientError(
                f"Failed to update fields {changed_spec_fields}."
                f" Can only update {_UPDATABLE_SPEC_FIELDS}."
            )
    configuration_diff = diff_models(current_run_spec.configuration, new_run_spec.configuration)
    changed_configuration_fields = list(configuration_diff.keys())
    for key in changed_configuration_fields:
        if key not in _UPDATABLE_CONFIGURATION_FIELDS:
            raise ServerClientError(
                f"Failed to update fields {changed_configuration_fields}."
                f" Can only update {_UPDATABLE_CONFIGURATION_FIELDS}"
            )


async def process_terminating_run(session: AsyncSession, run: RunModel):
    """
    Used by both `process_runs` and `stop_run` to process a TERMINATING run.
    Stops the jobs gracefully and marks them as TERMINATING.
    Jobs should be terminated by `process_terminating_jobs`.
    When all jobs are terminated, assigns a finished status to the run.
    Caller must acquire the lock on run.
    """
    assert run.termination_reason is not None
    job_termination_reason = run.termination_reason.to_job_termination_reason()

    unfinished_jobs_count = 0
    for job in run.jobs:
        if job.status.is_finished():
            continue
        unfinished_jobs_count += 1
        if job.status == JobStatus.TERMINATING:
            if job_termination_reason == JobTerminationReason.ABORTED_BY_USER:
                # Override termination reason so that
                # abort actions such as volume force detach are triggered
                job.termination_reason = job_termination_reason
            continue

        if job.status == JobStatus.RUNNING and job_termination_reason not in {
            JobTerminationReason.ABORTED_BY_USER,
            JobTerminationReason.DONE_BY_RUNNER,
        }:
            # Send a signal to stop the job gracefully
            await stop_runner(session, job)
            delay_job_instance_termination(job)
        job.status = JobStatus.TERMINATING
        job.termination_reason = job_termination_reason
        job.last_processed_at = common_utils.get_current_datetime()

    if unfinished_jobs_count == 0:
        if run.service_spec is not None:
            try:
                await services.unregister_service(session, run)
            except Exception as e:
                logger.warning("%s: failed to unregister service: %s", fmt(run), repr(e))
        run.status = run.termination_reason.to_status()
        logger.info(
            "%s: run status has changed TERMINATING -> %s, reason: %s",
            fmt(run),
            run.status.name,
            run.termination_reason.name,
        )


async def scale_run_replicas(session: AsyncSession, run_model: RunModel, replicas_diff: int):
    if replicas_diff == 0:
        # nothing to do
        return

    logger.info(
        "%s: scaling %s %s replica(s)",
        fmt(run_model),
        "UP" if replicas_diff > 0 else "DOWN",
        abs(replicas_diff),
    )

    # lists of (importance, replica_num, jobs)
    active_replicas = []
    inactive_replicas = []

    for replica_num, replica_jobs in group_jobs_by_replica_latest(run_model.jobs):
        statuses = set(job.status for job in replica_jobs)
        if {JobStatus.TERMINATING, *JobStatus.finished_statuses()} & statuses:
            # if there are any terminating or finished jobs, the replica is inactive
            inactive_replicas.append((0, replica_num, replica_jobs))
        elif JobStatus.SUBMITTED in statuses:
            # if there are any submitted jobs, the replica is active and has the importance of 0
            active_replicas.append((0, replica_num, replica_jobs))
        elif {JobStatus.PROVISIONING, JobStatus.PULLING} & statuses:
            # if there are any provisioning or pulling jobs, the replica is active and has the importance of 1
            active_replicas.append((1, replica_num, replica_jobs))
        else:
            # all jobs are running, the replica is active and has the importance of 2
            active_replicas.append((2, replica_num, replica_jobs))

    # sort by importance (desc) and replica_num (asc)
    active_replicas.sort(key=lambda r: (-r[0], r[1]))
    run_spec = RunSpec.__response__.parse_raw(run_model.run_spec)

    if replicas_diff < 0:
        if len(active_replicas) + replicas_diff < run_spec.configuration.replicas.min:
            raise ServerClientError("Can't scale down below the minimum number of replicas")

        for _, _, replica_jobs in reversed(active_replicas[-abs(replicas_diff) :]):
            # scale down the less important replicas first
            for job in replica_jobs:
                if job.status.is_finished() or job.status == JobStatus.TERMINATING:
                    continue
                job.status = JobStatus.TERMINATING
                job.termination_reason = JobTerminationReason.SCALED_DOWN
                # background task will process the job later
    else:
        if len(active_replicas) + replicas_diff > run_spec.configuration.replicas.max:
            raise ServerClientError("Can't scale up above the maximum number of replicas")
        scheduled_replicas = 0

        # rerun inactive replicas
        for _, _, replica_jobs in inactive_replicas:
            if scheduled_replicas == replicas_diff:
                break
            await retry_run_replica_jobs(session, run_model, replica_jobs, only_failed=False)
            scheduled_replicas += 1

        # create new replicas
        for replica_num in range(
            len(active_replicas) + scheduled_replicas, len(active_replicas) + replicas_diff
        ):
            jobs = await get_jobs_from_run_spec(run_spec, replica_num=replica_num)
            for job in jobs:
                job_model = create_job_model_for_new_submission(
                    run_model=run_model,
                    job=job,
                    status=JobStatus.SUBMITTED,
                )
                session.add(job_model)


async def retry_run_replica_jobs(
    session: AsyncSession, run_model: RunModel, latest_jobs: List[JobModel], *, only_failed: bool
):
    for job_model in latest_jobs:
        if not (job_model.status.is_finished() or job_model.status == JobStatus.TERMINATING):
            if only_failed:
                # No need to resubmit, skip
                continue
            # The job is not finished, but we have to retry all jobs. Terminate it
            job_model.status = JobStatus.TERMINATING
            job_model.termination_reason = JobTerminationReason.TERMINATED_BY_SERVER

        new_job_model = create_job_model_for_new_submission(
            run_model=run_model,
            job=Job(
                job_spec=JobSpec.__response__.parse_raw(job_model.job_spec_data),
                job_submissions=[],
            ),
            status=JobStatus.SUBMITTED,
        )
        # dirty hack to avoid passing all job submissions
        new_job_model.submission_num = job_model.submission_num + 1
        session.add(new_job_model)
