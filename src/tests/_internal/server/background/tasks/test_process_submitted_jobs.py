from datetime import datetime, timezone
from unittest.mock import Mock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.instances import (
    InstanceAvailability,
    InstanceOfferWithAvailability,
    InstanceSharedInfo,
    InstanceStatus,
    InstanceType,
    Resources,
)
from dstack._internal.core.models.profiles import Profile, ProfileRetryPolicy
from dstack._internal.core.models.runs import (
    JobProvisioningData,
    JobStatus,
    JobTerminationReason,
)
from dstack._internal.core.models.volumes import (
    InstanceMountPoint,
    VolumeAttachmentData,
    VolumeMountPoint,
    VolumeStatus,
)
from dstack._internal.server.background.tasks.process_submitted_jobs import process_submitted_jobs
from dstack._internal.server.models import InstanceModel, JobModel
from dstack._internal.server.testing.common import (
    create_fleet,
    create_instance,
    create_job,
    create_pool,
    create_project,
    create_repo,
    create_run,
    create_user,
    create_volume,
    get_instance_offer_with_availability,
    get_run_spec,
    get_volume_provisioning_data,
)

pytestmark = pytest.mark.usefixtures("image_config_mock")


class TestProcessSubmittedJobs:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_fails_job_when_no_backends(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        user = await create_user(session=session)
        await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=True,
        )
        await process_submitted_jobs()
        await session.refresh(job)
        assert job is not None
        assert job.status == JobStatus.TERMINATING
        assert job.termination_reason == JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ["backend", "privileged"],
        [
            [BackendType.AWS, False],
            [BackendType.AWS, True],
            [BackendType.RUNPOD, False],
        ],
    )
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_provisions_job(
        self,
        test_db,
        session: AsyncSession,
        backend: BackendType,
        privileged: bool,
    ):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        run_spec = get_run_spec(run_name="test-run", repo_id=repo.name)
        run_spec.configuration.privileged = privileged
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
            run_name="test-run",
            run_spec=run_spec,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=True,
        )
        offer = InstanceOfferWithAvailability(
            backend=backend,
            instance=InstanceType(
                name="instance",
                resources=Resources(cpus=1, memory_mib=512, spot=False, gpus=[]),
            ),
            region="us",
            price=1.0,
            availability=InstanceAvailability.AVAILABLE,
        )
        with patch("dstack._internal.server.services.backends.get_project_backends") as m:
            backend_mock = Mock()
            m.return_value = [backend_mock]
            backend_mock.TYPE = backend
            backend_mock.compute.return_value.get_offers_cached.return_value = [offer]
            backend_mock.compute.return_value.run_job.return_value = JobProvisioningData(
                backend=offer.backend,
                instance_type=offer.instance,
                instance_id="instance_id",
                hostname="1.1.1.1",
                internal_ip=None,
                region=offer.region,
                price=offer.price,
                username="ubuntu",
                ssh_port=22,
                ssh_proxy=None,
                dockerized=True,
                backend_data=None,
            )
            await process_submitted_jobs()
            m.assert_called_once()
            backend_mock.compute.return_value.get_offers_cached.assert_called_once()
            backend_mock.compute.return_value.run_job.assert_called_once()

        await session.refresh(job)
        assert job is not None
        assert job.status == JobStatus.PROVISIONING

        await session.refresh(pool)
        instance_offer = InstanceOfferWithAvailability.parse_raw(pool.instances[0].offer)
        assert offer == instance_offer
        pool_job_provisioning_data = pool.instances[0].job_provisioning_data
        assert pool_job_provisioning_data == job.job_provisioning_data

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_fails_job_when_privileged_true_and_no_offers_with_create_instance_support(
        self,
        test_db,
        session: AsyncSession,
    ):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        run_spec = get_run_spec(run_name="test-run", repo_id=repo.name)
        run_spec.configuration.privileged = True
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
            run_name="test-run",
            run_spec=run_spec,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=True,
        )
        offer = InstanceOfferWithAvailability(
            backend=BackendType.RUNPOD,
            instance=InstanceType(
                name="instance",
                resources=Resources(cpus=1, memory_mib=512, spot=False, gpus=[]),
            ),
            region="us",
            price=1.0,
            availability=InstanceAvailability.AVAILABLE,
        )
        with patch("dstack._internal.server.services.backends.get_project_backends") as m:
            backend_mock = Mock()
            m.return_value = [backend_mock]
            backend_mock.TYPE = BackendType.RUNPOD
            backend_mock.compute.return_value.get_offers_cached.return_value = [offer]
            backend_mock.compute.return_value.run_job.return_value = JobProvisioningData(
                backend=offer.backend,
                instance_type=offer.instance,
                instance_id="instance_id",
                hostname="1.1.1.1",
                internal_ip=None,
                region=offer.region,
                price=offer.price,
                username="ubuntu",
                ssh_port=22,
                ssh_proxy=None,
                dockerized=True,
                backend_data=None,
            )
            with patch("dstack._internal.utils.common.get_current_datetime") as datetime_mock:
                datetime_mock.return_value = datetime(2023, 1, 2, 3, 30, 0, tzinfo=timezone.utc)
                await process_submitted_jobs()
            m.assert_called_once()
            backend_mock.compute.return_value.get_offers_cached.assert_not_called()
            backend_mock.compute.return_value.run_job.assert_not_called()

        await session.refresh(job)
        assert job is not None
        assert job.status == JobStatus.TERMINATING
        assert job.termination_reason == JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY

        await session.refresh(pool)
        assert not pool.instances

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_fails_job_when_instance_mounts_and_no_offers_with_create_instance_support(
        self,
        test_db,
        session: AsyncSession,
    ):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        run_spec = get_run_spec(run_name="test-run", repo_id=repo.name)
        run_spec.configuration.volumes = [InstanceMountPoint.parse("/root/.cache:/cache")]
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
            run_name="test-run",
            run_spec=run_spec,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=True,
        )
        offer = InstanceOfferWithAvailability(
            backend=BackendType.RUNPOD,
            instance=InstanceType(
                name="instance",
                resources=Resources(cpus=1, memory_mib=512, spot=False, gpus=[]),
            ),
            region="us",
            price=1.0,
            availability=InstanceAvailability.AVAILABLE,
        )
        with patch("dstack._internal.server.services.backends.get_project_backends") as m:
            backend_mock = Mock()
            m.return_value = [backend_mock]
            backend_mock.TYPE = BackendType.RUNPOD
            backend_mock.compute.return_value.get_offers_cached.return_value = [offer]
            backend_mock.compute.return_value.run_job.return_value = JobProvisioningData(
                backend=offer.backend,
                instance_type=offer.instance,
                instance_id="instance_id",
                hostname="1.1.1.1",
                internal_ip=None,
                region=offer.region,
                price=offer.price,
                username="ubuntu",
                ssh_port=22,
                ssh_proxy=None,
                dockerized=True,
                backend_data=None,
            )
            with patch("dstack._internal.utils.common.get_current_datetime") as datetime_mock:
                datetime_mock.return_value = datetime(2023, 1, 2, 3, 30, 0, tzinfo=timezone.utc)
                await process_submitted_jobs()
            m.assert_called_once()
            backend_mock.compute.return_value.get_offers_cached.assert_not_called()
            backend_mock.compute.return_value.run_job.assert_not_called()

        await session.refresh(job)
        assert job is not None
        assert job.status == JobStatus.TERMINATING
        assert job.termination_reason == JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY

        await session.refresh(pool)
        assert not pool.instances

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_fails_job_when_no_capacity(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        user = await create_user(session=session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
            run_name="test-run",
            run_spec=get_run_spec(
                run_name="test-run",
                repo_id=repo.name,
                profile=Profile(
                    name="default",
                    retry_policy=ProfileRetryPolicy(retry=True, duration=3600),
                ),
            ),
        )
        job = await create_job(
            session=session,
            run=run,
            submitted_at=datetime(2023, 1, 2, 3, 0, 0, tzinfo=timezone.utc),
            instance_assigned=True,
        )
        with patch("dstack._internal.utils.common.get_current_datetime") as datetime_mock:
            datetime_mock.return_value = datetime(2023, 1, 2, 3, 30, 0, tzinfo=timezone.utc)
            await process_submitted_jobs()

        await session.refresh(job)
        assert job is not None
        assert job.status == JobStatus.TERMINATING
        assert job.termination_reason == JobTerminationReason.FAILED_TO_START_DUE_TO_NO_CAPACITY
        await session.refresh(pool)
        assert not pool.instances

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_assignes_job_to_instance(self, test_db, session: AsyncSession):
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
            status=InstanceStatus.IDLE,
        )
        await session.refresh(pool)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=False,
        )
        await process_submitted_jobs()
        await session.refresh(job)
        res = await session.execute(select(JobModel).options(joinedload(JobModel.instance)))
        job = res.unique().scalar_one()
        assert job.status == JobStatus.SUBMITTED
        assert (
            job.instance_assigned and job.instance is not None and job.instance.id == instance.id
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_assigns_job_to_instance_with_volumes(self, test_db, session: AsyncSession):
        project = await create_project(session)
        user = await create_user(session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        volume = await create_volume(
            session=session,
            project=project,
            user=user,
            status=VolumeStatus.ACTIVE,
            volume_provisioning_data=get_volume_provisioning_data(),
            backend=BackendType.AWS,
            region="us-east-1",
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.IDLE,
            backend=BackendType.AWS,
            region="us-east-1",
        )
        await session.refresh(pool)
        run_spec = get_run_spec(run_name="test-run", repo_id=repo.name)
        run_spec.configuration.volumes = [
            VolumeMountPoint(name=volume.name, path="/volume"),
            InstanceMountPoint(instance_path="/root/.cache", path="/cache"),
        ]
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
            run_name="test-run",
            run_spec=run_spec,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=False,
        )

        with patch("dstack._internal.server.services.backends.get_project_backend_by_type") as m:
            backend_mock = Mock()
            m.return_value = backend_mock
            backend_mock.TYPE = BackendType.AWS
            backend_mock.compute.return_value.attach_volume.return_value = VolumeAttachmentData()
            # Submitted jobs processing happens in two steps
            await process_submitted_jobs()
            await process_submitted_jobs()

        await session.refresh(job)
        await session.refresh(instance)
        res = await session.execute(
            select(JobModel).options(
                joinedload(JobModel.instance).selectinload(InstanceModel.volumes)
            )
        )
        job = res.unique().scalar_one()
        assert job.status == JobStatus.PROVISIONING
        assert (
            job.instance_assigned and job.instance is not None and job.instance.id == instance.id
        )
        assert job.instance.volumes == [volume]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_assigns_job_to_shared_instance(self, test_db, session: AsyncSession):
        project = await create_project(session)
        user = await create_user(session)
        pool = await create_pool(session=session, project=project)
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        offer = get_instance_offer_with_availability(gpu_count=8, cpu_count=64, memory_gib=128)
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.IDLE,
            offer=offer,
            shared_info=InstanceSharedInfo(total_blocks=4, busy_blocks=1),
        )
        await session.refresh(pool)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=False,
        )
        await process_submitted_jobs()
        await session.refresh(job)
        await session.refresh(instance)
        res = await session.execute(select(JobModel).options(joinedload(JobModel.instance)))
        job = res.unique().scalar_one()
        assert job.status == JobStatus.SUBMITTED
        assert (
            job.instance_assigned and job.instance is not None and job.instance.id == instance.id
        )
        assert InstanceSharedInfo.__response__.parse_raw(
            instance.shared_info
        ) == InstanceSharedInfo(total_blocks=4, busy_blocks=2)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_creates_new_instance_in_existing_fleet(self, test_db, session: AsyncSession):
        project = await create_project(session)
        user = await create_user(session)
        repo = await create_repo(session=session, project_id=project.id)
        pool = await create_pool(session=session, project=project)
        fleet = await create_fleet(session=session, project=project)
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            instance_num=0,
            status=InstanceStatus.BUSY,
        )
        fleet.instances.append(instance)
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
        )
        run.fleet = fleet
        job = await create_job(
            session=session,
            run=run,
            instance_assigned=True,
        )
        await session.commit()

        offer = InstanceOfferWithAvailability(
            backend=BackendType.AWS,
            instance=InstanceType(
                name="instance",
                resources=Resources(cpus=4, memory_mib=8192, spot=False, gpus=[]),
            ),
            region="us",
            price=1.0,
            availability=InstanceAvailability.AVAILABLE,
        )
        with patch("dstack._internal.server.services.backends.get_project_backends") as m:
            backend_mock = Mock()
            m.return_value = [backend_mock]
            backend_mock.TYPE = BackendType.AWS
            backend_mock.compute.return_value.get_offers_cached.return_value = [offer]
            backend_mock.compute.return_value.run_job.return_value = JobProvisioningData(
                backend=offer.backend,
                instance_type=offer.instance,
                instance_id="instance_id",
                hostname="1.1.1.1",
                internal_ip=None,
                region=offer.region,
                price=offer.price,
                username="ubuntu",
                ssh_port=22,
                ssh_proxy=None,
                dockerized=True,
                backend_data=None,
            )
            await process_submitted_jobs()
            m.assert_called_once()
            backend_mock.compute.return_value.get_offers_cached.assert_called_once()
            backend_mock.compute.return_value.run_job.assert_called_once()

        await session.refresh(job)
        res = await session.execute(select(JobModel).options(joinedload(JobModel.instance)))
        job = res.unique().scalar_one()
        assert job.status == JobStatus.PROVISIONING
        assert job.instance is not None
        assert job.instance.instance_num == 1
        assert job.instance.fleet_id == fleet.id
