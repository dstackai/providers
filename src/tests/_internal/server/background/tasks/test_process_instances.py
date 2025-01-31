import datetime as dt
from contextlib import contextmanager
from typing import Optional
from unittest.mock import Mock, patch

import gpuhunt
import pytest
from freezegun import freeze_time
from sqlalchemy.ext.asyncio import AsyncSession

from dstack._internal.core.errors import BackendError
from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.instances import (
    Gpu,
    InstanceAvailability,
    InstanceOfferWithAvailability,
    InstanceSharedInfo,
    InstanceStatus,
    InstanceType,
    Resources,
)
from dstack._internal.core.models.profiles import TerminationPolicy
from dstack._internal.core.models.runs import (
    JobProvisioningData,
    JobStatus,
)
from dstack._internal.server.background.tasks.process_instances import (
    HealthStatus,
    process_instances,
)
from dstack._internal.server.testing.common import (
    create_instance,
    create_job,
    create_pool,
    create_project,
    create_repo,
    create_run,
    create_user,
    get_remote_connection_info,
)
from dstack._internal.utils.common import get_current_datetime

pytestmark = pytest.mark.usefixtures("image_config_mock")


class TestCheckShim:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_transitions_provisioning_on_ready(
        self, test_db, session: AsyncSession
    ):
        project = await create_project(session=session)
        pool = await create_pool(session, project)

        instance = await create_instance(
            session, project, pool, status=InstanceStatus.PROVISIONING
        )
        instance.termination_deadline = get_current_datetime() + dt.timedelta(days=1)
        instance.health_status = "ssh connect problem"

        await session.commit()

        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=True, reason="OK")
            await process_instances()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.IDLE
        assert instance.termination_deadline is None
        assert instance.health_status is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_transitions_provisioning_on_terminating(
        self, test_db, session: AsyncSession
    ):
        project = await create_project(session=session)
        pool = await create_pool(session, project)

        instance = await create_instance(
            session, project, pool, status=InstanceStatus.PROVISIONING
        )
        instance.started_at = get_current_datetime() + dt.timedelta(minutes=-20)
        instance.health_status = "ssh connect problem"

        await session.commit()

        health_reason = "Shim problem"

        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=False, reason=health_reason)
            await process_instances()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.TERMINATING
        assert instance.termination_deadline is not None
        assert instance.health_status == health_reason

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_transitions_provisioning_on_busy(
        self, test_db, session: AsyncSession
    ):
        user = await create_user(session=session)
        project = await create_project(session=session, owner=user)
        pool = await create_pool(session, project)
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

        instance = await create_instance(
            session, project, pool, status=InstanceStatus.PROVISIONING
        )
        instance.termination_deadline = get_current_datetime().replace(
            tzinfo=dt.timezone.utc
        ) + dt.timedelta(days=1)
        instance.health_status = "ssh connect problem"

        job = await create_job(
            session=session,
            run=run,
            status=JobStatus.SUBMITTED,
            instance=instance,
        )

        await session.commit()

        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=True, reason="OK")
            await process_instances()

        await session.refresh(instance)
        await session.refresh(job)

        assert instance is not None
        assert instance.status == InstanceStatus.BUSY
        assert instance.termination_deadline is None
        assert instance.health_status is None
        assert job.instance == instance

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_start_termination_deadline(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)

        instance = await create_instance(session, project, pool, status=InstanceStatus.IDLE)

        health_status = "SSH connection fail"
        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=False, reason=health_status)
            await process_instances()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.IDLE
        assert instance.termination_deadline is not None
        assert instance.termination_deadline.replace(
            tzinfo=dt.timezone.utc
        ) > get_current_datetime() + dt.timedelta(minutes=19)
        assert instance.health_status == health_status

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_stop_termination_deadline(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)

        instance = await create_instance(session, project, pool, status=InstanceStatus.IDLE)
        instance.termination_deadline = get_current_datetime() + dt.timedelta(minutes=19)
        await session.commit()

        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=True, reason="OK")
            await process_instances()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.IDLE
        assert instance.termination_deadline is None
        assert instance.health_status is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_terminate_instance_by_dedaline(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)

        instance = await create_instance(session, project, pool, status=InstanceStatus.IDLE)
        termination_deadline_time = get_current_datetime() + dt.timedelta(minutes=-19)
        instance.termination_deadline = termination_deadline_time
        await session.commit()

        health_status = "Not ok"
        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=False, reason=health_status)
            await process_instances()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.TERMINATING
        assert (
            instance.termination_deadline.replace(tzinfo=dt.timezone.utc)
            == termination_deadline_time
        )
        assert instance.termination_reason == "Termination deadline"
        assert instance.health_status == health_status

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ["termination_policy", "has_job"],
        [
            pytest.param(TerminationPolicy.DESTROY_AFTER_IDLE, False, id="destroy-no-job"),
            pytest.param(TerminationPolicy.DESTROY_AFTER_IDLE, True, id="destroy-with-job"),
            pytest.param(TerminationPolicy.DONT_DESTROY, False, id="dont-destroy-no-job"),
            pytest.param(TerminationPolicy.DONT_DESTROY, True, id="dont-destroy-with-job"),
        ],
    )
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_check_shim_process_ureachable_state(
        self,
        test_db,
        session: AsyncSession,
        termination_policy: TerminationPolicy,
        has_job: bool,
    ):
        # see https://github.com/dstackai/dstack/issues/2041
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        if has_job:
            user = await create_user(session=session)
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
                status=JobStatus.SUBMITTED,
            )
        else:
            job = None
        instance = await create_instance(
            session,
            project,
            pool,
            created_at=get_current_datetime(),
            termination_policy=termination_policy,
            status=InstanceStatus.IDLE,
            unreachable=True,
            job=job,
        )

        await session.commit()

        with patch(
            "dstack._internal.server.background.tasks.process_instances._instance_healthcheck"
        ) as healthcheck:
            healthcheck.return_value = HealthStatus(healthy=True, reason="OK")
            await process_instances()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.IDLE
        assert not instance.unreachable


class TestTerminateIdleTime:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminate_by_idle_timeout(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(session, project, pool, status=InstanceStatus.IDLE)
        instance.termination_idle_time = 300
        instance.termination_policy = TerminationPolicy.DESTROY_AFTER_IDLE
        instance.last_job_processed_at = get_current_datetime() + dt.timedelta(minutes=-19)
        await session.commit()
        await process_instances()
        await session.refresh(instance)
        assert instance is not None
        assert instance.status == InstanceStatus.TERMINATED
        assert instance.termination_reason == "Idle timeout"


class TestSSHInstanceTerminateProvisionTimeoutExpired:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminate_by_idle_timeout(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(
            session,
            project,
            pool,
            status=InstanceStatus.PENDING,
            created_at=get_current_datetime() - dt.timedelta(days=100),
        )
        instance.remote_connection_info = "{}"
        await session.commit()

        await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATED
        assert instance.termination_reason == "Provisioning timeout expired"


class TestTerminate:
    @staticmethod
    @contextmanager
    def mock_terminate_in_backend(error: Optional[Exception] = None):
        backend = Mock()
        backend.TYPE = BackendType.DATACRUNCH
        terminate_instance = backend.compute.return_value.terminate_instance
        if error is not None:
            terminate_instance.side_effect = error
        with patch(
            "dstack._internal.server.background.tasks.process_instances.backends_services.get_project_backend_by_type"
        ) as get_backend:
            get_backend.return_value = backend
            yield terminate_instance

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminate(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)

        instance = await create_instance(session, project, pool, status=InstanceStatus.TERMINATING)

        reason = "some reason"
        instance.termination_reason = reason
        instance.last_job_processed_at = get_current_datetime() + dt.timedelta(minutes=-19)
        await session.commit()

        with self.mock_terminate_in_backend() as mock:
            await process_instances()
            mock.assert_called_once()

        await session.refresh(instance)

        assert instance is not None
        assert instance.status == InstanceStatus.TERMINATED
        assert instance.termination_reason == "some reason"
        assert instance.deleted == True
        assert instance.deleted_at is not None
        assert instance.finished_at is not None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    @pytest.mark.parametrize("error", [BackendError("err"), RuntimeError("err")])
    async def test_terminate_retry(self, test_db, session: AsyncSession, error: Exception):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(session, project, pool, status=InstanceStatus.TERMINATING)
        instance.termination_reason = "some reason"
        initial_time = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        instance.last_job_processed_at = initial_time
        await session.commit()

        # First attempt fails
        with (
            freeze_time(initial_time + dt.timedelta(minutes=1)),
            self.mock_terminate_in_backend(error=error) as mock,
        ):
            await process_instances()
            mock.assert_called_once()
        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATING

        # Second attempt succeeds
        with (
            freeze_time(initial_time + dt.timedelta(minutes=2)),
            self.mock_terminate_in_backend(error=None) as mock,
        ):
            await process_instances()
            mock.assert_called_once()
        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATED

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminate_not_retries_if_too_early(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(session, project, pool, status=InstanceStatus.TERMINATING)
        instance.termination_reason = "some reason"
        initial_time = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        instance.last_job_processed_at = initial_time
        await session.commit()

        # First attempt fails
        with (
            freeze_time(initial_time + dt.timedelta(minutes=1)),
            self.mock_terminate_in_backend(error=BackendError("err")) as mock,
        ):
            await process_instances()
            mock.assert_called_once()
        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATING

        # 3 seconds later - too early for the second attempt, nothing happens
        with (
            freeze_time(initial_time + dt.timedelta(minutes=1, seconds=3)),
            self.mock_terminate_in_backend(error=None) as mock,
        ):
            await process_instances()
            mock.assert_not_called()
        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminate_on_termination_deadline(self, test_db, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(session, project, pool, status=InstanceStatus.TERMINATING)
        instance.termination_reason = "some reason"
        initial_time = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
        instance.last_job_processed_at = initial_time
        await session.commit()

        # First attempt fails
        with (
            freeze_time(initial_time + dt.timedelta(minutes=1)),
            self.mock_terminate_in_backend(error=BackendError("err")) as mock,
        ):
            await process_instances()
            mock.assert_called_once()
        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATING

        # Second attempt fails too, but it's the last attempt because the deadline is close
        with (
            freeze_time(initial_time + dt.timedelta(minutes=15, seconds=55)),
            self.mock_terminate_in_backend(error=None) as mock,
        ):
            await process_instances()
            mock.assert_called_once()
        await session.refresh(instance)
        assert instance.status == InstanceStatus.TERMINATED


@pytest.mark.asyncio
@pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
@pytest.mark.usefixtures("test_db")
class TestCreateInstance:
    async def test_creates_instance_no_blocks(self, session: AsyncSession):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(session, project, pool, status=InstanceStatus.PENDING)
        with patch(
            "dstack._internal.server.background.tasks.process_instances.get_create_instance_offers"
        ) as get_create_instance_offers:
            offer = InstanceOfferWithAvailability(
                backend=BackendType.AWS,
                instance=InstanceType(
                    name="instance",
                    resources=Resources(cpus=1, memory_mib=512, spot=False, gpus=[]),
                ),
                region="us",
                price=1.0,
                availability=InstanceAvailability.AVAILABLE,
            )

            backend_mock = Mock()
            backend_mock.TYPE = BackendType.AWS
            backend_mock.compute.return_value.get_offers_cached.return_value = [offer]
            backend_mock.compute.return_value.create_instance.return_value = JobProvisioningData(
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
            get_create_instance_offers.return_value = [(backend_mock, offer)]
            await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.PROVISIONING
        assert instance.shared_info is None

    @pytest.mark.parametrize(
        ["blocks", "expected_blocks"],
        [
            [4, 4],
            ["auto", 8],
        ],
    )
    async def test_creates_instance_with_blocks(
        self, session: AsyncSession, blocks, expected_blocks
    ):
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        shared_info = InstanceSharedInfo(total_blocks=blocks)
        instance = await create_instance(
            session, project, pool, status=InstanceStatus.PENDING, shared_info=shared_info
        )
        with patch(
            "dstack._internal.server.background.tasks.process_instances.get_create_instance_offers"
        ) as get_create_instance_offers:
            gpu = Gpu(name="T4", memory_mib=16384, vendor=gpuhunt.AcceleratorVendor.NVIDIA)
            offer = InstanceOfferWithAvailability(
                backend=BackendType.AWS,
                instance=InstanceType(
                    name="instance",
                    resources=Resources(cpus=32, memory_mib=131072, spot=False, gpus=[gpu] * 8),
                ),
                region="us",
                price=1.0,
                availability=InstanceAvailability.AVAILABLE,
            )

            backend_mock = Mock()
            backend_mock.TYPE = BackendType.AWS
            backend_mock.compute.return_value.get_offers_cached.return_value = [offer]
            backend_mock.compute.return_value.create_instance.return_value = JobProvisioningData(
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
            get_create_instance_offers.return_value = [(backend_mock, offer)]
            await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.PROVISIONING
        assert InstanceSharedInfo.parse_raw(instance.shared_info) == InstanceSharedInfo(
            total_blocks=expected_blocks, busy_blocks=0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
@pytest.mark.usefixtures("test_db", "deploy_instance_mock")
class TestAddSSHInstance:
    @pytest.fixture
    def host_info(self) -> dict:
        return {
            "gpu_vendor": "nvidia",
            "gpu_name": "T4",
            "gpu_memory": 16384,
            "gpu_count": 1,
            "addresses": ["192.168.100.100/24"],
            "disk_size": 260976517120,
            "cpus": 32,
            "memory": 33544130560,
        }

    @pytest.fixture
    def deploy_instance_mock(self, monkeypatch: pytest.MonkeyPatch, host_info: dict):
        mock = Mock(return_value=(HealthStatus(healthy=True, reason="OK"), host_info))
        monkeypatch.setattr(
            "dstack._internal.server.background.tasks.process_instances._deploy_instance", mock
        )
        return mock

    async def test_adds_ssh_instance_no_blocks_requested(
        self, session: AsyncSession, host_info: dict
    ):
        host_info["gpu_count"] = 8
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(
            session,
            project,
            pool,
            status=InstanceStatus.PENDING,
            created_at=get_current_datetime(),
            remote_connection_info=get_remote_connection_info(),
            shared_info=None,
        )
        await session.commit()

        await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.IDLE
        assert instance.shared_info is None

    @pytest.mark.parametrize(
        ["gpu_count", "total_blocks"],
        [
            [0, "auto"],
            [1, "auto"],
            [1, 1],
        ],
    )
    async def test_adds_ssh_instance_no_blocks_zero_or_one_gpu(
        self, session: AsyncSession, host_info: dict, gpu_count, total_blocks
    ):
        host_info["gpu_count"] = gpu_count
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        if total_blocks is None:
            shared_info = None
        else:
            shared_info = InstanceSharedInfo(total_blocks=total_blocks)
        instance = await create_instance(
            session,
            project,
            pool,
            status=InstanceStatus.PENDING,
            created_at=get_current_datetime(),
            remote_connection_info=get_remote_connection_info(),
            shared_info=shared_info,
        )
        await session.commit()

        await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.IDLE
        assert instance.shared_info is None

    async def test_adds_ssh_instance_number_blocks(self, session: AsyncSession, host_info: dict):
        host_info["gpu_count"] = 8
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(
            session,
            project,
            pool,
            status=InstanceStatus.PENDING,
            created_at=get_current_datetime(),
            remote_connection_info=get_remote_connection_info(),
            shared_info=InstanceSharedInfo(total_blocks=4),
        )
        await session.commit()

        await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.IDLE
        assert InstanceSharedInfo.parse_raw(instance.shared_info) == InstanceSharedInfo(
            total_blocks=4, busy_blocks=0
        )

    @pytest.mark.usefixtures("deploy_instance_mock")
    async def test_adds_ssh_instance_auto_blocks(self, session: AsyncSession, host_info: dict):
        host_info["gpu_count"] = 8
        project = await create_project(session=session)
        pool = await create_pool(session, project)
        instance = await create_instance(
            session,
            project,
            pool,
            status=InstanceStatus.PENDING,
            created_at=get_current_datetime(),
            remote_connection_info=get_remote_connection_info(),
            shared_info=InstanceSharedInfo(total_blocks="auto"),
        )
        await session.commit()

        await process_instances()

        await session.refresh(instance)
        assert instance.status == InstanceStatus.IDLE
        assert InstanceSharedInfo.parse_raw(instance.shared_info) == InstanceSharedInfo(
            total_blocks=8, busy_blocks=0
        )
