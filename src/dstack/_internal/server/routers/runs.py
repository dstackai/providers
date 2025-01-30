from typing import List, Tuple

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from dstack._internal.core.errors import ComputeError, ResourceNotExistsError, ServerClientError
from dstack._internal.core.models.pools import Instance
from dstack._internal.core.models.runs import PoolInstanceOffers, Run, RunPlan
from dstack._internal.server.db import get_session
from dstack._internal.server.models import ProjectModel, UserModel
from dstack._internal.server.schemas.runs import (
    ApplyRunPlanRequest,
    CreateInstanceRequest,
    DeleteRunsRequest,
    GetOffersRequest,
    GetRunPlanRequest,
    GetRunRequest,
    ListRunsRequest,
    StopRunsRequest,
    SubmitRunRequest,
)
from dstack._internal.server.security.permissions import Authenticated, ProjectMember
from dstack._internal.server.services import fleets, runs
from dstack._internal.server.services.pools import (
    get_or_create_pool_by_name,
)
from dstack._internal.server.utils.routers import get_base_api_additional_responses

root_router = APIRouter(
    prefix="/api/runs",
    tags=["runs"],
    responses=get_base_api_additional_responses(),
)
project_router = APIRouter(
    prefix="/api/project/{project_name}/runs",
    tags=["runs"],
    responses=get_base_api_additional_responses(),
)


@root_router.post("/list")
async def list_runs(
    body: ListRunsRequest,
    session: AsyncSession = Depends(get_session),
    user: UserModel = Depends(Authenticated()),
) -> List[Run]:
    """
    Returns all runs visible to user sorted by descending `submitted_at`.
    `project_name`, `repo_id`, `username`, and `only_active` can be specified as filters.
    Setting `only_active` to `true`, excludes finished runs and deleted runs.
    Specifying `repo_id` without `project_name` returns no runs.

    The results are paginated. To get the next page, pass `submitted_at` and `id` of
    the last run from the previous page as `prev_submitted_at` and `prev_run_id`.
    """
    return await runs.list_user_runs(
        session=session,
        user=user,
        project_name=body.project_name,
        repo_id=body.repo_id,
        username=body.username,
        only_active=body.only_active,
        prev_submitted_at=body.prev_submitted_at,
        prev_run_id=body.prev_run_id,
        limit=body.limit,
        ascending=body.ascending,
    )


@project_router.post("/get")
async def get_run(
    body: GetRunRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
) -> Run:
    """
    Returns a run given `run_name` or `id`.
    If given `run_name`, does not return deleted runs.
    If given `id`, returns deleted runs.
    """
    _, project = user_project
    run = await runs.get_run(
        session=session,
        project=project,
        run_name=body.run_name,
        run_id=body.id,
    )
    if run is None:
        raise ResourceNotExistsError("Run not found")
    return run


@project_router.post("/get_plan")
async def get_plan(
    body: GetRunPlanRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
) -> RunPlan:
    """
    Returns a run plan for the given run spec.
    This is an optional step before calling `/apply`.
    """
    user, project = user_project
    run_plan = await runs.get_plan(
        session=session,
        project=project,
        user=user,
        run_spec=body.run_spec,
    )
    return run_plan


@project_router.post("/apply")
async def apply_plan(
    body: ApplyRunPlanRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
) -> Run:
    """
    Creates a new run or updates an existing run.
    Errors if the expected current resource from the plan does not match the current resource.
    Use `force: true` to apply even if the current resource does not match.
    If the existing run is active and cannot be updated, it must be stopped first.
    """
    user, project = user_project
    return await runs.apply_plan(
        session=session,
        user=user,
        project=project,
        plan=body.plan,
        force=body.force,
    )


# apply_plan replaces submit_run since it can create new runs.
# submit_run can be deprecated in the future.
@project_router.post("/submit")
async def submit_run(
    body: SubmitRunRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
) -> Run:
    user, project = user_project
    return await runs.submit_run(
        session=session,
        user=user,
        project=project,
        run_spec=body.run_spec,
    )


@project_router.post("/stop")
async def stop_runs(
    body: StopRunsRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
):
    """
    Stop one or more runs.
    """
    _, project = user_project
    await runs.stop_runs(
        session=session,
        project=project,
        runs_names=body.runs_names,
        abort=body.abort,
    )


@project_router.post("/delete")
async def delete_runs(
    body: DeleteRunsRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
):
    """
    Delete one or more runs. The runs must be stopped before they can be deleted.
    """
    _, project = user_project
    await runs.delete_runs(session=session, project=project, runs_names=body.runs_names)


# FIXME: get_offers and create_instance semantically belong to pools, not runs
@project_router.post("/get_offers", deprecated=True)
async def get_offers(
    body: GetOffersRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
) -> PoolInstanceOffers:
    _, project = user_project
    pool = await get_or_create_pool_by_name(session, project, body.profile.pool_name)
    offers = await fleets.get_create_instance_offers(
        project=project,
        profile=body.profile,
        requirements=body.requirements,
    )
    instances = [instance for _, instance in offers]
    return PoolInstanceOffers(pool_name=pool.name, instances=instances)


# FIXME: get_offers and create_instance semantically belong to pools, not runs
@project_router.post("/create_instance", deprecated=True)
async def create_instance(
    body: CreateInstanceRequest,
    session: AsyncSession = Depends(get_session),
    user_project: Tuple[UserModel, ProjectModel] = Depends(ProjectMember()),
) -> Instance:
    user, project = user_project
    try:
        instance = await fleets.create_instance(
            session=session,
            project=project,
            user=user,
            profile=body.profile,
            requirements=body.requirements,
        )
    except ComputeError as e:
        raise ServerClientError(str(e))
    return instance
