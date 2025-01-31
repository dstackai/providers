from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.instances import (
    InstanceSharedInfo,
    InstanceStatus,
)
from dstack._internal.core.models.runs import JobStatus, JobTerminationReason
from dstack._internal.core.models.volumes import VolumeStatus
from dstack._internal.server.background.tasks.process_terminating_jobs import (
    process_terminating_jobs,
)
from dstack._internal.server.models import InstanceModel, JobModel
from dstack._internal.server.services.volumes import volume_model_to_volume
from dstack._internal.server.testing.common import (
    create_instance,
    create_job,
    create_pool,
    create_project,
    create_repo,
    create_run,
    create_user,
    create_volume,
    get_instance_shared_offer,
    get_job_provisioning_data,
    get_job_runtime_data,
    get_volume_configuration,
    get_volume_provisioning_data,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
@pytest.mark.usefixtures("test_db", "image_config_mock")
class TestProcessTerminatingJobs:
    async def test_terminates_job(self, session: AsyncSession):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.BUSY,
        )
        repo = await create_repo(session=session, project_id=project.id)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job_provisioning_data = get_job_provisioning_data(dockerized=True)
        job = await create_job(
            session=session,
            run=run,
            status=JobStatus.TERMINATING,
            termination_reason=JobTerminationReason.TERMINATED_BY_USER,
            submitted_at=datetime(2023, 1, 2, 5, 12, 30, 5, tzinfo=timezone.utc),
            job_provisioning_data=job_provisioning_data,
            instance=instance,
        )
        with (
            patch("dstack._internal.server.services.runner.ssh.SSHTunnel") as SSHTunnelMock,
            patch("dstack._internal.server.services.runner.client.ShimClient") as ShimClientMock,
        ):
            shim_client_mock = ShimClientMock.return_value
            await process_terminating_jobs()
            SSHTunnelMock.assert_called_once()
            shim_client_mock.healthcheck.assert_called_once()
        await session.refresh(job)
        assert job is not None
        assert job.status == JobStatus.TERMINATED

    async def test_detaches_job_volumes(self, session: AsyncSession):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        volume = await create_volume(
            session=session,
            project=project,
            user=user,
            status=VolumeStatus.ACTIVE,
            backend=BackendType.AWS,
            volume_provisioning_data=get_volume_provisioning_data(),
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.BUSY,
            volumes=[volume],
        )
        repo = await create_repo(session=session, project_id=project.id)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job_provisioning_data = get_job_provisioning_data(dockerized=False)
        job = await create_job(
            session=session,
            run=run,
            status=JobStatus.TERMINATING,
            termination_reason=JobTerminationReason.TERMINATED_BY_USER,
            submitted_at=datetime(2023, 1, 2, 5, 12, 30, 5, tzinfo=timezone.utc),
            job_provisioning_data=job_provisioning_data,
            instance=instance,
        )
        with patch("dstack._internal.server.services.backends.get_project_backend_by_type") as m:
            backend_mock = Mock()
            m.return_value = backend_mock
            backend_mock.compute.return_value.is_volume_detached.return_value = True
            await process_terminating_jobs()
            m.assert_awaited_once()
            backend_mock.compute.return_value.detach_volume.assert_called_once()
            backend_mock.compute.return_value.is_volume_detached.assert_called_once()
        await session.refresh(job)
        assert job.status == JobStatus.TERMINATED

    async def test_force_detaches_job_volumes(self, session: AsyncSession):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        volume = await create_volume(
            session=session,
            project=project,
            user=user,
            status=VolumeStatus.ACTIVE,
            backend=BackendType.AWS,
            volume_provisioning_data=get_volume_provisioning_data(),
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.BUSY,
            volumes=[volume],
        )
        repo = await create_repo(session=session, project_id=project.id)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job_provisioning_data = get_job_provisioning_data(dockerized=False)
        job = await create_job(
            session=session,
            run=run,
            status=JobStatus.TERMINATING,
            termination_reason=JobTerminationReason.TERMINATED_BY_USER,
            submitted_at=datetime(2023, 1, 2, 5, 12, 30, 5, tzinfo=timezone.utc),
            job_provisioning_data=job_provisioning_data,
            instance=instance,
        )

        # First soft detach fails
        with patch("dstack._internal.server.services.backends.get_project_backend_by_type") as m:
            backend_mock = Mock()
            m.return_value = backend_mock
            backend_mock.compute.return_value.is_volume_detached.return_value = False
            await process_terminating_jobs()
            m.assert_awaited_once()
            backend_mock.compute.return_value.detach_volume.assert_called_once()
            backend_mock.compute.return_value.is_volume_detached.assert_called_once()
        await session.refresh(job)
        res = await session.execute(select(JobModel).options(joinedload(JobModel.instance)))
        job = res.unique().scalar_one()
        assert job.status == JobStatus.TERMINATING
        # The instance should be released even if detach fails
        # so that stuck volumes don't prevent the instance from terminating.
        assert job.instance is None
        assert job.volumes_detached_at is not None
        assert len(instance.volumes) == 1

        # Force detach called
        with (
            patch("dstack._internal.server.services.backends.get_project_backend_by_type") as m,
            patch("dstack._internal.utils.common.get_current_datetime") as datetime_mock,
        ):
            datetime_mock.return_value = job.volumes_detached_at.replace(
                tzinfo=timezone.utc
            ) + timedelta(minutes=30)
            backend_mock = Mock()
            m.return_value = backend_mock
            backend_mock.compute.return_value.is_volume_detached.return_value = False
            await process_terminating_jobs()
            m.assert_awaited_once()
            backend_mock.compute.return_value.detach_volume.assert_called_once_with(
                volume=volume_model_to_volume(volume),
                instance_id=job_provisioning_data.instance_id,
                force=True,
            )
            backend_mock.compute.return_value.is_volume_detached.assert_called_once()
        await session.refresh(job)
        # At least one more iteration needed to confirm the volume was force detached
        assert job.status == JobStatus.TERMINATING

        # Check force detach succeeds
        with patch("dstack._internal.server.services.backends.get_project_backend_by_type") as m:
            backend_mock = Mock()
            m.return_value = backend_mock
            backend_mock.compute.return_value.is_volume_detached.return_value = True
            await process_terminating_jobs()
            m.assert_awaited_once()
            backend_mock.compute.return_value.is_volume_detached.assert_called_once()
        await session.refresh(job)
        await session.refresh(instance)
        res = await session.execute(select(JobModel).options(joinedload(JobModel.instance)))
        job = res.unique().scalar_one()
        res = await session.execute(
            select(InstanceModel).options(joinedload(InstanceModel.volumes))
        )
        instance = res.unique().scalar_one()
        assert job.status == JobStatus.TERMINATED
        assert len(instance.volumes) == 0

    async def test_terminates_job_on_shared_instance(self, session: AsyncSession):
        project = await create_project(session)
        user = await create_user(session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.BUSY,
            shared_info=InstanceSharedInfo(total_blocks=4, busy_blocks=3),
        )
        await session.refresh(pool)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        shared_offer = get_instance_shared_offer(blocks=2, total_blocks=4)
        jrd = get_job_runtime_data(offer=shared_offer)
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=True,
            instance=instance,
            job_runtime_data=jrd,
            status=JobStatus.TERMINATING,
            termination_reason=JobTerminationReason.TERMINATED_BY_USER,
        )

        await process_terminating_jobs()

        await session.refresh(job)
        await session.refresh(instance)
        res = await session.execute(select(JobModel).options(joinedload(JobModel.instance)))
        job = res.unique().scalar_one()
        assert job.status == JobStatus.TERMINATED
        assert job.instance_assigned
        assert job.instance is None
        assert InstanceSharedInfo.__response__.parse_raw(
            instance.shared_info
        ) == InstanceSharedInfo(total_blocks=4, busy_blocks=1)

    async def test_detaches_job_volumes_on_shared_instance(self, session: AsyncSession):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        volume_conf_1 = get_volume_configuration(name="vol-1")
        volume_1 = await create_volume(
            session=session,
            project=project,
            user=user,
            status=VolumeStatus.ACTIVE,
            backend=BackendType.AWS,
            configuration=volume_conf_1,
            volume_provisioning_data=get_volume_provisioning_data(),
        )
        volume_conf_2 = get_volume_configuration(name="vol-2")
        volume_2 = await create_volume(
            session=session,
            project=project,
            user=user,
            status=VolumeStatus.ACTIVE,
            backend=BackendType.AWS,
            configuration=volume_conf_2,
            volume_provisioning_data=get_volume_provisioning_data(),
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.BUSY,
            volumes=[volume_1, volume_2],
        )
        repo = await create_repo(session=session, project_id=project.id)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job_provisioning_data = get_job_provisioning_data(dockerized=False)
        job = await create_job(
            session=session,
            run=run,
            status=JobStatus.TERMINATING,
            termination_reason=JobTerminationReason.TERMINATED_BY_USER,
            submitted_at=datetime(2023, 1, 2, 5, 12, 30, 5, tzinfo=timezone.utc),
            job_provisioning_data=job_provisioning_data,
            job_runtime_data=get_job_runtime_data(volume_names=["vol-1"]),
            instance=instance,
        )
        with patch("dstack._internal.server.services.backends.get_project_backend_by_type") as m:
            backend_mock = Mock()
            m.return_value = backend_mock
            backend_mock.compute.return_value.is_volume_detached.return_value = True

            await process_terminating_jobs()

        m.assert_awaited_once()
        backend_mock.compute.return_value.detach_volume.assert_called_once()
        backend_mock.compute.return_value.is_volume_detached.assert_called_once()
        await session.refresh(job)
        await session.refresh(instance)
        assert job.status == JobStatus.TERMINATED
        res = await session.execute(
            select(InstanceModel).options(joinedload(InstanceModel.volumes))
        )
        instance = res.unique().scalar_one()
        assert instance.volumes == [volume_2]
