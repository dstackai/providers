import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from dstack._internal.core.models.runs import RunStatus
from dstack._internal.core.models.users import GlobalRole, ProjectRole
from dstack._internal.server.background.tasks.process_fleets import process_empty_fleets
from dstack._internal.server.services.projects import add_project_member
from dstack._internal.server.testing.common import (
    create_fleet,
    create_project,
    create_repo,
    create_run,
    create_user,
)


class TestProcessEmptyFleets:
    @pytest.mark.asyncio
    async def test_deletes_empty_fleets(self, test_db, session: AsyncSession):
        project = await create_project(session)
        fleet = await create_fleet(
            session=session,
            project=project,
        )
        await process_empty_fleets()
        await session.refresh(fleet)
        assert fleet.deleted

    @pytest.mark.asyncio
    async def test_does_not_delete_fleet_with_active_run(self, test_db, session: AsyncSession):
        project = await create_project(session)
        fleet = await create_fleet(
            session=session,
            project=project,
        )
        user = await create_user(session=session, global_role=GlobalRole.USER)
        await add_project_member(
            session=session, project=project, user=user, project_role=ProjectRole.USER
        )
        repo = await create_repo(
            session=session,
            project_id=project.id,
        )
        run = await create_run(
            session=session,
            project=project,
            repo=repo,
            user=user,
            status=RunStatus.RUNNING,
        )
        fleet.runs.append(run)
        await session.commit()
        await process_empty_fleets()
        await session.refresh(fleet)
        assert not fleet.deleted
