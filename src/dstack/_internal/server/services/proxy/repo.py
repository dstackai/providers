from typing import List, Optional

import pydantic
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

import dstack._internal.server.services.jobs as jobs_services
from dstack._internal.core.consts import DSTACK_RUNNER_SSH_PORT
from dstack._internal.core.models.common import is_core_model_instance
from dstack._internal.core.models.configurations import ServiceConfiguration
from dstack._internal.core.models.instances import SSHConnectionParams
from dstack._internal.core.models.runs import (
    JobProvisioningData,
    JobStatus,
    RunSpec,
    RunStatus,
    ServiceSpec,
)
from dstack._internal.core.models.services import AnyModel
from dstack._internal.proxy.lib.models import (
    AnyModelFormat,
    ChatModel,
    OpenAIChatModelFormat,
    Project,
    Replica,
    Service,
    TGIChatModelFormat,
)
from dstack._internal.proxy.lib.repo import BaseProxyRepo
from dstack._internal.server.models import JobModel, ProjectModel, RunModel
from dstack._internal.server.settings import DEFAULT_SERVICE_CLIENT_MAX_BODY_SIZE


class ServerProxyRepo(BaseProxyRepo):
    """
    A repo implementation used by dstack-proxy running within dstack-server.
    Retrieves data from dstack-server's database.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def get_service(self, project_name: str, run_name: str) -> Optional[Service]:
        res = await self.session.execute(
            select(JobModel)
            .join(JobModel.project)
            .join(JobModel.run)
            .where(
                ProjectModel.name == project_name,
                RunModel.gateway_id.is_(None),
                JobModel.run_name == run_name,
                JobModel.status == JobStatus.RUNNING,
                JobModel.job_num == 0,
            )
            .options(joinedload(JobModel.run))
        )
        jobs = res.scalars().all()
        if not len(jobs):
            return None
        run = jobs[0].run
        run_spec = RunSpec.__response__.parse_raw(run.run_spec)
        if not is_core_model_instance(run_spec.configuration, ServiceConfiguration):
            return None
        replicas = []
        for job in jobs:
            jpd: JobProvisioningData = JobProvisioningData.__response__.parse_raw(
                job.job_provisioning_data
            )
            if not jpd.dockerized:
                ssh_destination = f"{jpd.username}@{jpd.hostname}"
                ssh_port = jpd.ssh_port
                ssh_proxy = jpd.ssh_proxy
            else:
                ssh_destination = "root@localhost"  # TODO(#1535): support non-root images properly
                ssh_port = DSTACK_RUNNER_SSH_PORT
                job_submission = jobs_services.job_model_to_job_submission(job)
                jrd = job_submission.job_runtime_data
                if jrd is not None and jrd.ports is not None:
                    ssh_port = jrd.ports.get(ssh_port, ssh_port)
                ssh_proxy = SSHConnectionParams(
                    hostname=jpd.hostname,
                    username=jpd.username,
                    port=jpd.ssh_port,
                )
            replica = Replica(
                id=job.id.hex,
                app_port=run_spec.configuration.port.container_port,
                ssh_destination=ssh_destination,
                ssh_port=ssh_port,
                ssh_proxy=ssh_proxy,
            )
            replicas.append(replica)
        return Service(
            project_name=project_name,
            run_name=run.run_name,
            domain=None,
            https=None,
            auth=run_spec.configuration.auth,
            client_max_body_size=DEFAULT_SERVICE_CLIENT_MAX_BODY_SIZE,
            strip_prefix=run_spec.configuration.strip_prefix,
            replicas=tuple(replicas),
        )

    async def list_models(self, project_name: str) -> List[ChatModel]:
        res = await self.session.execute(
            select(RunModel)
            .join(RunModel.project)
            .where(
                ProjectModel.name == project_name,
                RunModel.gateway_id.is_(None),
                RunModel.service_spec.is_not(None),
                RunModel.status == RunStatus.RUNNING,
            )
        )
        models = []
        for run in res.scalars().all():
            service_spec: ServiceSpec = ServiceSpec.__response__.parse_raw(run.service_spec)
            model_spec = service_spec.model
            model_options_obj = service_spec.options.get("openai", {}).get("model")
            if model_spec is None or model_options_obj is None:
                continue
            model_options = pydantic.parse_obj_as(AnyModel, model_options_obj)
            model = ChatModel(
                project_name=project_name,
                name=model_spec.name,
                created_at=run.submitted_at,
                run_name=run.run_name,
                format_spec=_model_options_to_format_spec(model_options),
            )
            models.append(model)
        return models

    async def get_model(self, project_name: str, name: str) -> Optional[ChatModel]:
        models = await self.list_models(project_name)
        models = [m for m in models if m.name == name]
        if not models:
            return None
        # If there are many models with the same name, choose the most recent
        return max(models, key=lambda m: m.created_at)

    async def get_project(self, name: str) -> Optional[Project]:
        res = await self.session.execute(select(ProjectModel).where(ProjectModel.name == name))
        project = res.scalar_one_or_none()
        if project is None:
            return None
        return Project(
            name=project.name,
            ssh_private_key=project.ssh_private_key,
        )


def _model_options_to_format_spec(model: AnyModel) -> AnyModelFormat:
    if model.type == "chat":
        if model.format == "openai":
            return OpenAIChatModelFormat(prefix=model.prefix)
        elif model.format == "tgi":
            return TGIChatModelFormat(
                chat_template=model.chat_template,
                eos_token=model.eos_token,
            )
        else:
            raise RuntimeError(f"Unexpected model format {model.format}")
    else:
        raise RuntimeError(f"Unexpected model type {model.type}")
