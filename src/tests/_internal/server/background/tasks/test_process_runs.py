import datetime
from unittest.mock import patch

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession

import dstack._internal.server.background.tasks.process_runs as process_runs
from dstack._internal.core.models.profiles import Profile, ProfileRetryPolicy
from dstack._internal.core.models.runs import JobStatus, JobTerminationReason, RunStatus
from dstack._internal.server.models import RunModel
from dstack._internal.server.testing.common import (
    create_instance,
    create_job,
    create_pool,
    create_project,
    create_repo,
    create_run,
    create_user,
    get_run_spec,
)


@pytest_asyncio.fixture
async def run(session: AsyncSession) -> RunModel:
    project = await create_project(session=session)
    user = await create_user(session=session)
    repo = await create_repo(
        session=session,
        project_id=project.id,
    )
    project.default_pool = await create_pool(
        session=session, project=project, pool_name="default-pool"
    )
    run_name = "test-run"
    profile = Profile(
        name="test-profile",
        retry_policy=ProfileRetryPolicy(retry=True),
    )
    run_spec = get_run_spec(repo_id=repo.name, run_name=run_name, profile=profile)
    return await create_run(
        session=session,
        project=project,
        repo=repo,
        user=user,
        run_name=run_name,
        run_spec=run_spec,
    )


class TestProcessRuns:
    @pytest.mark.asyncio
    async def test_submitted_to_starting(self, test_db, session: AsyncSession, run: RunModel):
        run.status = RunStatus.SUBMITTED
        await create_job(session=session, run=run, status=JobStatus.PROVISIONING)

        await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.STARTING

    @pytest.mark.asyncio
    async def test_starting_to_running(self, test_db, session: AsyncSession, run: RunModel):
        run.status = RunStatus.STARTING
        await create_job(session=session, run=run, status=JobStatus.RUNNING)

        await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.RUNNING

    @pytest.mark.asyncio
    async def test_keep_starting(self, test_db, session: AsyncSession, run: RunModel):
        run.status = RunStatus.STARTING
        await create_job(session=session, run=run, status=JobStatus.PULLING)

        await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.STARTING

    @pytest.mark.asyncio
    async def test_running_to_done(self, test_db, session: AsyncSession, run: RunModel):
        run.status = RunStatus.RUNNING
        await create_job(session=session, run=run, status=JobStatus.DONE)

        await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.DONE

    @pytest.mark.asyncio
    async def test_terminate_run_jobs(self, test_db, session: AsyncSession, run: RunModel):
        run.status = RunStatus.TERMINATED
        run.processing_finished = False
        job = await create_job(session=session, run=run, status=JobStatus.RUNNING)

        await process_runs.process_single_run(run.id, [])
        await session.refresh(job)
        assert job.status == JobStatus.TERMINATED
        # TODO(egor-s): assert job.error_code
        await session.refresh(run)
        assert run.processing_finished is True

    @pytest.mark.asyncio
    async def test_retry_running_to_pending(self, test_db, session: AsyncSession, run: RunModel):
        instance = await create_instance(
            session, project=run.project, pool=run.project.default_pool, spot=True
        )
        run.status = RunStatus.RUNNING
        await create_job(
            session=session,
            run=run,
            status=JobStatus.FAILED,
            error_code=JobTerminationReason.INTERRUPTED_BY_NO_CAPACITY,
            instance=instance,
        )

        with patch("dstack._internal.utils.common.get_current_datetime") as datetime_mock:
            datetime_mock.return_value = run.submitted_at + datetime.timedelta(minutes=3)
            await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.PENDING

    @pytest.mark.asyncio
    async def test_retry_running_to_failed(self, test_db, session: AsyncSession, run: RunModel):
        instance = await create_instance(
            session, project=run.project, pool=run.project.default_pool, spot=True
        )
        run.status = RunStatus.RUNNING
        # job exited with non-zero code
        await create_job(
            session=session,
            run=run,
            status=JobStatus.FAILED,
            error_code=None,
            instance=instance,
        )

        with patch("dstack._internal.utils.common.get_current_datetime") as datetime_mock:
            datetime_mock.return_value = run.submitted_at + datetime.timedelta(minutes=3)
            await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.FAILED

    @pytest.mark.asyncio
    async def test_pending_to_submitted(self, test_db, session: AsyncSession, run: RunModel):
        run.status = RunStatus.PENDING
        await create_job(session=session, run=run, status=JobStatus.FAILED)

        await process_runs.process_single_run(run.id, [])
        await session.refresh(run)
        assert run.status == RunStatus.SUBMITTED
        assert len(run.jobs) == 2
        assert run.jobs[0].status == JobStatus.FAILED
        assert run.jobs[1].status == JobStatus.SUBMITTED


# TODO(egor-s): TestProcessRunsMultiNode
# TODO(egor-s): TestProcessRunsReplicas
# TODO(egor-s): TestProcessRunsAutoScaling
