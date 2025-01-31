import json
from datetime import datetime, timezone
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import pytest
from freezegun import freeze_time
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.fleets import FleetConfiguration, FleetStatus, SSHParams
from dstack._internal.core.models.instances import (
    InstanceAvailability,
    InstanceOfferWithAvailability,
    InstanceStatus,
    InstanceType,
    Resources,
    SSHKey,
)
from dstack._internal.core.models.users import GlobalRole, ProjectRole
from dstack._internal.server.models import FleetModel, InstanceModel
from dstack._internal.server.services.permissions import DefaultPermissions
from dstack._internal.server.services.projects import add_project_member
from dstack._internal.server.testing.common import (
    create_fleet,
    create_instance,
    create_job,
    create_pool,
    create_project,
    create_repo,
    create_run,
    create_user,
    default_permissions_context,
    get_auth_headers,
    get_fleet_configuration,
    get_fleet_spec,
    get_private_key_string,
)

pytestmark = pytest.mark.usefixtures("image_config_mock")


class TestListFleets:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/fleets/list")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_lists_fleets_across_projects(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.ADMIN)
        project1 = await create_project(session, name="project1", owner=user)
        fleet1_spec = get_fleet_spec()
        fleet1_spec.configuration.name = "fleet1"
        await create_fleet(
            session=session,
            project=project1,
            created_at=datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc),
            spec=fleet1_spec,
        )
        project2 = await create_project(session, name="project2", owner=user)
        fleet2_spec = get_fleet_spec()
        fleet2_spec.configuration.name = "fleet2"
        await create_fleet(
            session=session,
            project=project2,
            created_at=datetime(2023, 1, 2, 3, 5, tzinfo=timezone.utc),
            spec=fleet2_spec,
        )
        response = await client.post(
            "/api/fleets/list",
            headers=get_auth_headers(user.token),
            json={},
        )
        response_json = response.json()
        assert response.status_code == 200, response_json
        assert len(response_json) == 2
        assert response_json[0]["name"] == "fleet2"
        assert response_json[1]["name"] == "fleet1"
        response = await client.post(
            "/api/fleets/list",
            headers=get_auth_headers(user.token),
            json={"prev_created_at": response_json[0]["created_at"]},
        )
        response_json = response.json()
        assert response.status_code == 200, response_json
        assert len(response_json) == 1
        assert response_json[0]["name"] == "fleet1"

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_non_admin_cannot_see_others_projects(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user1 = await create_user(session, name="user1", global_role=GlobalRole.USER)
        user2 = await create_user(session, name="user2", global_role=GlobalRole.USER)
        project1 = await create_project(session, name="project1", owner=user1)
        project2 = await create_project(session, name="project2", owner=user2)
        await add_project_member(
            session=session, project=project1, user=user1, project_role=ProjectRole.USER
        )
        await add_project_member(
            session=session, project=project2, user=user2, project_role=ProjectRole.USER
        )
        await create_fleet(
            session=session,
            project=project1,
            created_at=datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc),
        )
        await create_fleet(
            session=session,
            project=project2,
            created_at=datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc),
        )
        response = await client.post(
            "/api/fleets/list",
            headers=get_auth_headers(user1.token),
            json={},
        )
        response_json = response.json()
        assert response.status_code == 200, response_json
        assert len(response_json) == 1
        assert response_json[0]["project_name"] == "project1"


class TestListProjectFleets:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/project/main/fleets/list")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_lists_fleets(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        fleet = await create_fleet(
            session=session,
            project=project,
            created_at=datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc),
        )
        response = await client.post(
            f"/api/project/{project.name}/fleets/list",
            headers=get_auth_headers(user.token),
        )
        assert response.status_code == 200
        assert response.json() == [
            {
                "id": str(fleet.id),
                "name": fleet.name,
                "project_name": project.name,
                "spec": json.loads(fleet.spec),
                "created_at": "2023-01-02T03:04:00+00:00",
                "status": fleet.status.value,
                "status_message": None,
                "instances": [],
            }
        ]


class TestGetFleet:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/project/main/fleets/get")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    @pytest.mark.parametrize("deleted", [False, True])
    async def test_returns_fleet_by_id(
        self, test_db, session: AsyncSession, client: AsyncClient, deleted: bool
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        fleet = await create_fleet(
            session=session,
            project=project,
            created_at=datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc),
            deleted=deleted,
        )
        response = await client.post(
            f"/api/project/{project.name}/fleets/get",
            headers=get_auth_headers(user.token),
            json={"id": str(fleet.id)},
        )
        assert response.status_code == 200
        assert response.json() == {
            "id": str(fleet.id),
            "name": fleet.name,
            "project_name": project.name,
            "spec": json.loads(fleet.spec),
            "created_at": "2023-01-02T03:04:00+00:00",
            "status": fleet.status.value,
            "status_message": None,
            "instances": [],
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_not_deleted_fleet_by_name(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        active_fleet = await create_fleet(
            session=session,
            project=project,
            created_at=datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc),
            fleet_id=uuid4(),
        )
        deleted_fleet = await create_fleet(
            session=session,
            project=project,
            created_at=datetime(2023, 1, 2, 3, 5, tzinfo=timezone.utc),
            fleet_id=uuid4(),
            deleted=True,
        )
        assert active_fleet.name == deleted_fleet.name
        assert active_fleet.id != deleted_fleet.id
        response = await client.post(
            f"/api/project/{project.name}/fleets/get",
            headers=get_auth_headers(user.token),
            json={"name": active_fleet.name},
        )
        assert response.status_code == 200
        assert response.json() == {
            "id": str(active_fleet.id),
            "name": active_fleet.name,
            "project_name": project.name,
            "spec": json.loads(active_fleet.spec),
            "created_at": "2023-01-02T03:04:00+00:00",
            "status": active_fleet.status.value,
            "status_message": None,
            "instances": [],
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_not_returns_by_name_if_fleet_deleted(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        fleet = await create_fleet(session=session, project=project, deleted=True)
        response = await client.post(
            f"/api/project/{project.name}/fleets/get",
            headers=get_auth_headers(user.token),
            json={"name": fleet.name},
        )
        assert response.status_code == 400

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_not_returns_by_name_if_fleet_does_not_exist(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        response = await client.post(
            f"/api/project/{project.name}/fleets/get",
            headers=get_auth_headers(user.token),
            json={"name": "some_fleet"},
        )
        assert response.status_code == 400


class TestCreateFleet:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/project/main/fleets/create")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    @freeze_time(datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc))
    async def test_creates_fleet(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        spec = get_fleet_spec(conf=get_fleet_configuration())
        with patch("uuid.uuid4") as m:
            m.return_value = UUID("1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e")
            response = await client.post(
                f"/api/project/{project.name}/fleets/create",
                headers=get_auth_headers(user.token),
                json={"spec": spec.dict()},
            )
        assert response.status_code == 200
        assert response.json() == {
            "id": "1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e",
            "name": spec.configuration.name,
            "project_name": project.name,
            "spec": {
                "configuration_path": spec.configuration_path,
                "configuration": {
                    "nodes": {"min": 1, "max": 1},
                    "placement": None,
                    "env": {},
                    "ssh_config": None,
                    "resources": {
                        "cpu": {"min": 2, "max": None},
                        "memory": {"min": 8.0, "max": None},
                        "shm_size": None,
                        "gpu": None,
                        "disk": {"size": {"min": 100.0, "max": None}},
                    },
                    "backends": None,
                    "regions": None,
                    "instance_types": None,
                    "spot_policy": None,
                    "retry": None,
                    "max_price": None,
                    "idle_duration": None,
                    "termination_policy": None,
                    "termination_idle_time": None,
                    "type": "fleet",
                    "name": "test-fleet",
                    "reservation": None,
                    "blocks": 1,
                },
                "profile": {
                    "backends": None,
                    "regions": None,
                    "instance_types": None,
                    "spot_policy": None,
                    "retry": None,
                    "retry_policy": None,
                    "max_duration": None,
                    "stop_duration": None,
                    "max_price": None,
                    "pool_name": None,
                    "instance_name": None,
                    "creation_policy": None,
                    "idle_duration": None,
                    "termination_policy": None,
                    "termination_idle_time": None,
                    "name": "",
                    "default": False,
                    "reservation": None,
                },
                "autocreated": False,
            },
            "created_at": "2023-01-02T03:04:00+00:00",
            "status": "active",
            "status_message": None,
            "instances": [
                {
                    "id": "1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e",
                    "project_name": project.name,
                    "name": f"{spec.configuration.name}-0",
                    "fleet_id": "1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e",
                    "fleet_name": spec.configuration.name,
                    "instance_num": 0,
                    "job_name": None,
                    "hostname": None,
                    "status": "pending",
                    "unreachable": False,
                    "termination_reason": None,
                    "created": "2023-01-02T03:04:00+00:00",
                    "pool_name": None,
                    "backend": None,
                    "region": None,
                    "instance_type": None,
                    "price": None,
                    "total_blocks": None,
                    "busy_blocks": None,
                }
            ],
        }
        res = await session.execute(select(FleetModel))
        assert res.scalar_one()
        res = await session.execute(select(InstanceModel))
        assert res.unique().scalar_one()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    @freeze_time(datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc))
    async def test_creates_ssh_fleet(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        spec = get_fleet_spec(
            conf=FleetConfiguration(
                name="test-ssh-fleet",
                ssh_config=SSHParams(
                    user="ubuntu",
                    ssh_key=SSHKey(public="", private=get_private_key_string()),
                    hosts=["1.1.1.1"],
                    network=None,
                ),
            )
        )
        with patch("uuid.uuid4") as m:
            m.return_value = UUID("1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e")
            response = await client.post(
                f"/api/project/{project.name}/fleets/create",
                headers=get_auth_headers(user.token),
                json={"spec": spec.dict()},
            )
        assert response.status_code == 200, response.json()
        assert response.json() == {
            "id": "1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e",
            "name": spec.configuration.name,
            "project_name": project.name,
            "spec": {
                "configuration_path": spec.configuration_path,
                "configuration": {
                    "env": {},
                    "ssh_config": {
                        "user": "ubuntu",
                        "port": None,
                        "identity_file": None,
                        "ssh_key": None,  # should not return ssh_key
                        "hosts": ["1.1.1.1"],
                        "network": None,
                    },
                    "nodes": None,
                    "placement": None,
                    "resources": {
                        "cpu": {"min": 2, "max": None},
                        "memory": {"min": 8.0, "max": None},
                        "shm_size": None,
                        "gpu": None,
                        "disk": {"size": {"min": 100.0, "max": None}},
                    },
                    "backends": None,
                    "regions": None,
                    "instance_types": None,
                    "spot_policy": None,
                    "retry": None,
                    "max_price": None,
                    "idle_duration": None,
                    "termination_policy": None,
                    "termination_idle_time": None,
                    "type": "fleet",
                    "name": spec.configuration.name,
                    "reservation": None,
                    "blocks": 1,
                },
                "profile": {
                    "backends": None,
                    "regions": None,
                    "instance_types": None,
                    "spot_policy": None,
                    "retry": None,
                    "retry_policy": None,
                    "max_duration": None,
                    "stop_duration": None,
                    "max_price": None,
                    "pool_name": None,
                    "instance_name": None,
                    "creation_policy": None,
                    "idle_duration": None,
                    "termination_policy": None,
                    "termination_idle_time": None,
                    "name": "",
                    "default": False,
                    "reservation": None,
                },
                "autocreated": False,
            },
            "created_at": "2023-01-02T03:04:00+00:00",
            "status": "active",
            "status_message": None,
            "instances": [
                {
                    "id": "1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e",
                    "project_name": project.name,
                    "backend": "remote",
                    "instance_type": {
                        "name": "ssh",
                        "resources": {
                            "cpus": 2,
                            "memory_mib": 8,
                            "gpus": [],
                            "spot": False,
                            "disk": {"size_mib": 102400},
                            "description": "",
                        },
                    },
                    "name": f"{spec.configuration.name}-0",
                    "fleet_id": "1b0e1b45-2f8c-4ab6-8010-a0d1a3e44e0e",
                    "fleet_name": spec.configuration.name,
                    "instance_num": 0,
                    "pool_name": None,
                    "job_name": None,
                    "hostname": "1.1.1.1",
                    "status": "pending",
                    "unreachable": False,
                    "termination_reason": None,
                    "created": "2023-01-02T03:04:00+00:00",
                    "region": "remote",
                    "price": 0.0,
                    "total_blocks": None,
                    "busy_blocks": None,
                }
            ],
        }
        res = await session.execute(select(FleetModel))
        assert res.scalar_one()
        res = await session.execute(select(InstanceModel))
        instance = res.unique().scalar_one()
        assert instance.remote_connection_info is not None

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    @freeze_time(datetime(2023, 1, 2, 3, 4, tzinfo=timezone.utc))
    async def test_errors_if_ssh_key_is_bad(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        spec = get_fleet_spec(
            conf=FleetConfiguration(
                name="test-ssh-fleet",
                ssh_config=SSHParams(
                    user="ubuntu",
                    ssh_key=SSHKey(public="", private="123"),
                    hosts=["1.1.1.1"],
                    network=None,
                ),
            )
        )
        response = await client.post(
            f"/api/project/{project.name}/fleets/create",
            headers=get_auth_headers(user.token),
            json={"spec": spec.dict()},
        )
        assert response.status_code == 400

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_forbids_if_no_permission_to_manage_ssh_fleets(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        spec = get_fleet_spec(
            conf=FleetConfiguration(
                name="test-ssh-fleet",
                ssh_config=SSHParams(
                    user="ubuntu",
                    ssh_key=SSHKey(public="", private=get_private_key_string()),
                    hosts=["1.1.1.1"],
                    network=None,
                ),
            )
        )
        with default_permissions_context(
            DefaultPermissions(allow_non_admins_manage_ssh_fleets=False)
        ):
            response = await client.post(
                f"/api/project/{project.name}/fleets/create",
                headers=get_auth_headers(user.token),
                json={"spec": spec.dict()},
            )
        assert response.status_code in [401, 403]


class TestDeleteFleets:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/project/main/fleets/delete")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminates_fleet_instances(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        pool = await create_pool(session=session, project=project)
        fleet = await create_fleet(session=session, project=project)
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
        )
        fleet.instances.append(instance)
        await session.commit()
        response = await client.post(
            f"/api/project/{project.name}/fleets/delete",
            headers=get_auth_headers(user.token),
            json={"names": [fleet.name]},
        )
        assert response.status_code == 200
        await session.refresh(fleet)
        await session.refresh(instance)
        assert not fleet.deleted  # should not be deleted yet
        assert instance.status == InstanceStatus.TERMINATING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_400_when_fleets_in_use(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        pool = await create_pool(session=session, project=project)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        fleet = await create_fleet(session=session, project=project)
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
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            status=InstanceStatus.BUSY,
            job=job,
        )
        fleet.instances.append(instance)
        await session.commit()
        response = await client.post(
            f"/api/project/{project.name}/fleets/delete",
            headers=get_auth_headers(user.token),
            json={"names": [fleet.name]},
        )
        assert response.status_code == 400
        await session.refresh(fleet)
        assert not fleet.deleted
        assert instance.status == InstanceStatus.BUSY

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_forbids_if_no_permission_to_manage_ssh_fleets(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        spec = get_fleet_spec(
            conf=FleetConfiguration(
                name="test-ssh-fleet",
                ssh_config=SSHParams(
                    user="ubuntu",
                    ssh_key=SSHKey(public="", private=get_private_key_string()),
                    hosts=["1.1.1.1"],
                    network=None,
                ),
            )
        )
        fleet = await create_fleet(session=session, project=project, spec=spec)
        with default_permissions_context(
            DefaultPermissions(allow_non_admins_manage_ssh_fleets=False)
        ):
            response = await client.post(
                f"/api/project/{project.name}/fleets/delete",
                headers=get_auth_headers(user.token),
                json={"names": [fleet.name]},
            )
        assert response.status_code in [401, 403]


class TestDeleteFleetInstances:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/project/main/fleets/delete_instances")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_terminates_fleet_instances(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        pool = await create_pool(session=session, project=project)
        fleet = await create_fleet(session=session, project=project)
        instance1 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            instance_num=1,
        )
        instance2 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            instance_num=2,
        )
        fleet.instances.append(instance1)
        fleet.instances.append(instance2)
        await session.commit()
        response = await client.post(
            f"/api/project/{project.name}/fleets/delete_instances",
            headers=get_auth_headers(user.token),
            json={"name": fleet.name, "instance_nums": [1]},
        )
        assert response.status_code == 200
        await session.refresh(fleet)
        await session.refresh(instance1)
        await session.refresh(instance2)

        assert instance1.status == InstanceStatus.TERMINATING
        assert instance2.status != InstanceStatus.TERMINATING
        assert fleet.status != FleetStatus.TERMINATING

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_400_when_deleting_busy_instances(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session, global_role=GlobalRole.USER)
        project = await create_project(session)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        pool = await create_pool(session=session, project=project)
        fleet = await create_fleet(session=session, project=project)
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
        )
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            instance_num=1,
            status=InstanceStatus.BUSY,
            job=job,
        )
        fleet.instances.append(instance)
        await session.commit()
        response = await client.post(
            f"/api/project/{project.name}/fleets/delete_instances",
            headers=get_auth_headers(user.token),
            json={"name": fleet.name, "instance_nums": [1]},
        )
        assert response.status_code == 400
        await session.refresh(fleet)
        await session.refresh(instance)

        assert instance.status != InstanceStatus.TERMINATING
        assert fleet.status != FleetStatus.TERMINATING


class TestGetPlan:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_40x_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post("/api/project/main/fleets/get_plan")
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_plan(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        offers = [
            InstanceOfferWithAvailability(
                backend=BackendType.AWS,
                instance=InstanceType(
                    name="instance",
                    resources=Resources(cpus=1, memory_mib=512, spot=False, gpus=[]),
                ),
                region="us",
                price=1.0,
                availability=InstanceAvailability.AVAILABLE,
            )
        ]
        spec = get_fleet_spec()
        with patch("dstack._internal.server.services.backends.get_project_backends") as m:
            backend_mock = Mock()
            m.return_value = [backend_mock]
            backend_mock.TYPE = BackendType.AWS
            backend_mock.compute.return_value.get_offers_cached.return_value = offers
            response = await client.post(
                f"/api/project/{project.name}/fleets/get_plan",
                headers=get_auth_headers(user.token),
                json={"spec": spec.dict()},
            )
            backend_mock.compute.return_value.get_offers_cached.assert_called_once()

        assert response.status_code == 200
        assert response.json() == {
            "project_name": project.name,
            "user": user.name,
            "spec": spec.dict(),
            "current_resource": None,
            "offers": [json.loads(o.json()) for o in offers],
            "total_offers": len(offers),
            "max_offer_price": 1.0,
        }
