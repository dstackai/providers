import datetime as dt

import pytest
from freezegun import freeze_time
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.instances import SSHKey
from dstack._internal.core.models.profiles import DEFAULT_POOL_NAME
from dstack._internal.core.models.users import GlobalRole, ProjectRole
from dstack._internal.server.models import PoolModel
from dstack._internal.server.schemas.pools import (
    CreatePoolRequest,
    DeletePoolRequest,
    RemoveInstanceRequest,
    SetDefaultPoolRequest,
    ShowPoolRequest,
)
from dstack._internal.server.schemas.runs import AddRemoteInstanceRequest
from dstack._internal.server.services.projects import add_project_member
from dstack._internal.server.testing.common import (
    create_instance,
    create_pool,
    create_project,
    create_user,
    get_auth_headers,
)

TEST_POOL_NAME = "test_router_pool_name"


class TestListPools:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        response = await client.post(
            f"/api/project/{project.name}/pool/list",
            json={},
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    @freeze_time(dt.datetime(2023, 10, 4, 12, 0, tzinfo=dt.timezone.utc))
    async def test_creates_and_lists_default_pool(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/list",
            headers=get_auth_headers(user.token),
            json={},
        )
        assert response.status_code == 200
        result = response.json()
        expected = [
            {
                "name": "default-pool",
                "default": True,
                "created_at": "2023-10-04T12:00:00+00:00",
                "total_instances": 0,
                "available_instances": 0,
            }
        ]
        assert result == expected


class TestDeletePool:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        response = await client.post(
            f"/api/project/{project.name}/pool/delete",
            json=DeletePoolRequest(name=TEST_POOL_NAME, force=False).dict(),
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_delete_last_pool(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=TEST_POOL_NAME)
        response = await client.post(
            f"/api/project/{project.name}/pool/delete",
            headers=get_auth_headers(user.token),
            json=DeletePoolRequest(name=TEST_POOL_NAME, force=False).dict(),
        )
        assert response.status_code == 200
        assert response.json() is None

        response = await client.post(
            f"/api/project/{project.name}/pool/list",
            headers=get_auth_headers(user.token),
            json={},
        )
        assert response.status_code == 200

        result = response.json()
        assert len(result) == 1

        default_pool = result[0]
        assert default_pool["name"] == DEFAULT_POOL_NAME
        assert dt.datetime.fromisoformat(default_pool["created_at"]) > pool.created_at

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_deletes_pool(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool1 = await create_pool(session, project, pool_name=f"{TEST_POOL_NAME}-left")
        pool2 = await create_pool(session, project, pool_name=f"{TEST_POOL_NAME}-right")
        response = await client.post(
            f"/api/project/{project.name}/pool/delete",
            headers=get_auth_headers(user.token),
            json=DeletePoolRequest(name=pool1.name, force=False).dict(),
        )
        assert response.status_code == 200
        assert response.json() is None
        res = await session.execute(select(PoolModel).where(PoolModel.deleted == False))
        pool = res.scalar_one()
        assert pool.name == pool2.name

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_400_if_pool_missing(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/delete",
            headers=get_auth_headers(user.token),
            json=DeletePoolRequest(name="missing name", force=False).dict(),
        )
        assert response.status_code == 400


class TestSetDefaultPool:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        response = await client.post(
            f"/api/project/{project.name}/pool/set_default",
            json=SetDefaultPoolRequest(pool_name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_sets_default(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=f"{TEST_POOL_NAME}-right")
        response = await client.post(
            f"/api/project/{project.name}/pool/set_default",
            headers=get_auth_headers(user.token),
            json=SetDefaultPoolRequest(pool_name=pool.name).dict(),
        )
        assert response.status_code == 200
        await session.refresh(project)
        assert project.default_pool_id == pool.id

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_400_if_pool_missing(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/set_default",
            headers=get_auth_headers(user.token),
            json=SetDefaultPoolRequest(pool_name="missing pool").dict(),
        )
        assert response.status_code == 400


class TestCreatePool:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        response = await client.post(
            f"/api/project/{project.name}/pool/create",
            json=CreatePoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_create_pool(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/create",
            headers=get_auth_headers(user.token),
            json=CreatePoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 200
        assert response.json() is None
        res = await session.execute(select(PoolModel).where(PoolModel.deleted == False))
        res.scalar_one()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_400_on_duplicate_name(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/create",
            headers=get_auth_headers(user.token),
            json=CreatePoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 200
        assert response.json() is None
        response = await client.post(
            f"/api/project/{project.name}/pool/create",
            headers=get_auth_headers(user.token),
            json=CreatePoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 400


class TestShowPool:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        response = await client.post(
            f"/api/project/{project.name}/pool/show",
            json=CreatePoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_show_pool(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=TEST_POOL_NAME)
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            backend=BackendType.DATACRUNCH,
            region="en",
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/show",
            headers=get_auth_headers(user.token),
            json=ShowPoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 200
        assert response.json() == {
            "name": "test_router_pool_name",
            "instances": [
                {
                    "backend": "datacrunch",
                    "instance_type": {
                        "name": "instance",
                        "resources": {
                            "cpus": 1,
                            "memory_mib": 512,
                            "gpus": [],
                            "spot": False,
                            "disk": {"size_mib": 102400},
                            "description": "",
                        },
                    },
                    "id": str(instance.id),
                    "project_name": project.name,
                    "name": "test_instance",
                    "fleet_id": None,
                    "fleet_name": None,
                    "instance_num": 0,
                    "job_name": None,
                    "hostname": "running_instance.ip",
                    "status": "idle",
                    "unreachable": False,
                    "termination_reason": None,
                    "created": "2023-01-02T03:04:00+00:00",
                    "pool_name": None,
                    "region": "en",
                    "price": 1,
                    "total_blocks": None,
                    "busy_blocks": None,
                }
            ],
        }

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_show_missing_pool(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=TEST_POOL_NAME)
        await create_instance(
            session=session,
            project=project,
            pool=pool,
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/show",
            headers=get_auth_headers(user.token),
            json=ShowPoolRequest(name="missing_pool").dict(),
        )
        assert response.status_code == 400
        assert response.json() == {
            "detail": [{"msg": "Pool not found", "code": "resource_not_exists"}]
        }


class TestAddRemote:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        remote = AddRemoteInstanceRequest(
            instance_name="test_instance_name",
            instance_network=None,
            region="",
            host="localhost",
            port=22,
            pool_name="pool_name",
            ssh_user="user",
            ssh_keys=[SSHKey(public="abc")],
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/add_remote",
            json=remote.dict(),
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_add_remote(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        remote = AddRemoteInstanceRequest(
            instance_name="test_instance_name",
            instance_network=None,
            region="",
            host="localhost",
            port=22,
            pool_name="pool_name",
            ssh_user="user",
            ssh_keys=[SSHKey(public="abc")],
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/add_remote",
            headers=get_auth_headers(user.token),
            json=remote.dict(),
        )
        assert response.status_code == 200

        data = response.json()
        assert data["status"] == "pending"
        assert data["name"] == "test_instance_name"


class TestRemoveInstance:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        remote = AddRemoteInstanceRequest(
            instance_name="test_instance_name",
            instance_network=None,
            region="",
            host="localhost",
            port=22,
            pool_name="pool_name",
            ssh_user="user",
            ssh_keys=[SSHKey(public="abc")],
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/add_remote",
            json=remote.dict(),
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_remove_instance(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=TEST_POOL_NAME)
        instance = await create_instance(
            session=session,
            project=project,
            pool=pool,
            backend=BackendType.DATACRUNCH,
            region="en",
        )
        response = await client.post(
            f"/api/project/{project.name}/pool/remove",
            headers=get_auth_headers(user.token),
            json=RemoveInstanceRequest(
                pool_name=TEST_POOL_NAME,
                instance_name=instance.name,
            ).dict(),
        )
        assert response.status_code == 200
        assert response.json() is None

        response = await client.post(
            f"/api/project/{project.name}/pool/show",
            headers=get_auth_headers(user.token),
            json=ShowPoolRequest(name=TEST_POOL_NAME).dict(),
        )
        assert response.status_code == 200
        assert response.json() == {
            "name": "test_router_pool_name",
            "instances": [
                {
                    "backend": "datacrunch",
                    "instance_type": {
                        "name": "instance",
                        "resources": {
                            "cpus": 1,
                            "memory_mib": 512,
                            "gpus": [],
                            "spot": False,
                            "disk": {"size_mib": 102400},
                            "description": "",
                        },
                    },
                    "id": str(instance.id),
                    "project_name": project.name,
                    "name": "test_instance",
                    "fleet_id": None,
                    "fleet_name": None,
                    "instance_num": 0,
                    "job_name": None,
                    "hostname": "running_instance.ip",
                    "status": "terminating",
                    "unreachable": False,
                    "termination_reason": None,
                    "created": "2023-01-02T03:04:00+00:00",
                    "pool_name": None,
                    "region": "en",
                    "price": 1,
                    "total_blocks": None,
                    "busy_blocks": None,
                }
            ],
        }


class TestListInstances:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_returns_403_if_not_authenticated(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        response = await client.post(
            "/api/pools/list_instances",
            json={},
        )
        assert response.status_code == 403

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_lists_instances(self, test_db, session: AsyncSession, client: AsyncClient):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=TEST_POOL_NAME)
        instance1 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            created_at=dt.datetime(2023, 10, 4, 12, 0, tzinfo=dt.timezone.utc),
        )
        instance2 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            created_at=dt.datetime(2023, 10, 5, 12, 0, tzinfo=dt.timezone.utc),
        )
        response = await client.post(
            "/api/pools/list_instances",
            headers=get_auth_headers(user.token),
            json={},
        )
        assert response.status_code == 200
        response_json = response.json()
        assert len(response_json) == 2
        assert response_json[0]["id"] == str(instance2.id)
        assert response_json[1]["id"] == str(instance1.id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize("test_db", ["sqlite", "postgres"], indirect=True)
    async def test_lists_paginated_instances(
        self, test_db, session: AsyncSession, client: AsyncClient
    ):
        user = await create_user(session=session, global_role=GlobalRole.USER)
        project = await create_project(session=session, owner=user)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.ADMIN
        )
        pool = await create_pool(session, project, pool_name=TEST_POOL_NAME)
        instance1 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            created_at=dt.datetime(2023, 10, 5, 12, 0, tzinfo=dt.timezone.utc),
        )
        instance2 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            created_at=dt.datetime(2023, 10, 3, 12, 0, tzinfo=dt.timezone.utc),
        )
        instance3 = await create_instance(
            session=session,
            project=project,
            pool=pool,
            created_at=dt.datetime(2023, 10, 6, 12, 0, tzinfo=dt.timezone.utc),
        )
        response = await client.post(
            "/api/pools/list_instances",
            headers=get_auth_headers(user.token),
            json={"limit": 2},
        )
        assert response.status_code == 200
        response_json = response.json()
        assert len(response_json) == 2
        assert response_json[0]["id"] == str(instance3.id)
        assert response_json[1]["id"] == str(instance1.id)
        response = await client.post(
            "/api/pools/list_instances",
            headers=get_auth_headers(user.token),
            json={
                "prev_id": response_json[1]["id"],
                "prev_created_at": response_json[1]["created"],
            },
        )
        assert response.status_code == 200
        response_json = response.json()
        assert len(response_json) == 1
        assert response_json[0]["id"] == str(instance2.id)
