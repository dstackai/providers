import asyncio

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload, lazyload

from dstack._internal.core.models.runs import JobStatus
from dstack._internal.server.db import get_session_ctx
from dstack._internal.server.models import InstanceModel, JobModel, ProjectModel, VolumeModel
from dstack._internal.server.services.jobs import (
    process_terminating_job,
    process_volumes_detaching,
)
from dstack._internal.server.services.locking import get_locker
from dstack._internal.server.services.logging import fmt
from dstack._internal.utils.common import get_current_datetime, get_or_error
from dstack._internal.utils.logging import get_logger

logger = get_logger(__name__)


async def process_terminating_jobs(batch_size: int = 1):
    tasks = []
    for _ in range(batch_size):
        tasks.append(_process_next_terminating_job())
    await asyncio.gather(*tasks)


async def _process_next_terminating_job():
    job_lock, job_lockset = get_locker().get_lockset(JobModel.__tablename__)
    instance_lock, instance_lockset = get_locker().get_lockset(InstanceModel.__tablename__)
    async with get_session_ctx() as session:
        async with job_lock, instance_lock:
            res = await session.execute(
                select(JobModel)
                .where(
                    JobModel.id.not_in(job_lockset),
                    JobModel.status == JobStatus.TERMINATING,
                    or_(JobModel.remove_at.is_(None), JobModel.remove_at < get_current_datetime()),
                )
                .order_by(JobModel.last_processed_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            job_model = res.scalar()
            if job_model is None:
                return
            if job_model.used_instance_id is not None:
                res = await session.execute(
                    select(InstanceModel)
                    .where(
                        InstanceModel.id == job_model.used_instance_id,
                        InstanceModel.id.not_in(instance_lockset),
                    )
                    .options(lazyload(InstanceModel.jobs))
                    .with_for_update(skip_locked=True)
                )
                instance_model = res.scalar()
                if instance_model is None:
                    # InstanceModel is locked
                    return
                instance_lockset.add(instance_model.id)
            job_lockset.add(job_model.id)
        try:
            job_model_id = job_model.id
            instance_model_id = job_model.used_instance_id
            await _process_job(
                session=session,
                job_model=job_model,
            )
        finally:
            job_lockset.difference_update([job_model_id])
            instance_lockset.difference_update([instance_model_id])


async def _process_job(session: AsyncSession, job_model: JobModel):
    logger.debug("%s: terminating job", fmt(job_model))
    res = await session.execute(
        select(InstanceModel)
        .where(InstanceModel.id == job_model.used_instance_id)
        .options(
            joinedload(InstanceModel.project).joinedload(ProjectModel.backends),
            joinedload(InstanceModel.volumes).joinedload(VolumeModel.user),
        )
    )
    instance_model = res.unique().scalar()
    if job_model.volumes_detached_at is None:
        await process_terminating_job(session, job_model, instance_model)
    else:
        instance_model = get_or_error(instance_model)
        await process_volumes_detaching(session, job_model, instance_model)
    job_model.last_processed_at = get_current_datetime()
    await session.commit()
