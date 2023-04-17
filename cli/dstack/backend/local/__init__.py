from pathlib import Path
from typing import Generator, List, Optional

from dstack.backend.base import Backend, BackendType
from dstack.backend.base import artifacts as base_artifacts
from dstack.backend.base import cache as base_cache
from dstack.backend.base import jobs as base_jobs
from dstack.backend.base import repos as base_repos
from dstack.backend.base import runs as base_runs
from dstack.backend.base import secrets as base_secrets
from dstack.backend.base import tags as base_tags
from dstack.backend.local import artifacts, logs
from dstack.backend.local.compute import LocalCompute
from dstack.backend.local.config import LocalConfig
from dstack.backend.local.secrets import LocalSecretsManager
from dstack.backend.local.storage import LocalStorage
from dstack.core.artifact import Artifact
from dstack.core.job import Job, JobHead, JobStatus
from dstack.core.log_event import LogEvent
from dstack.core.repo import Repo, RepoCredentials, RepoRef
from dstack.core.run import RunHead
from dstack.core.secret import Secret
from dstack.core.tag import TagHead
from dstack.utils.common import PathLike


class LocalBackend(Backend):
    def __init__(self, repo: Repo):
        super().__init__(repo=repo)
        self.backend_config = LocalConfig()
        self.backend_config.load()
        self._loaded = True
        self._storage = LocalStorage(self.backend_config.path)
        self._compute = LocalCompute()
        self._secrets_manager = LocalSecretsManager(
            self.backend_config.path, repo_id=self.repo.repo_id if self.repo else None
        )

    @property
    def name(self):
        return "local"

    @property
    def type(self) -> BackendType:
        return BackendType.LOCAL

    def configure(self):
        pass

    def create_run(self) -> str:
        return base_runs.create_run(self._storage, self.type)

    def create_job(self, job: Job):
        base_jobs.create_job(self._storage, job)

    def get_job(self, job_id: str, repo_ref: Optional[RepoRef] = None) -> Optional[Job]:
        repo_ref = repo_ref or self.repo.repo_ref
        return base_jobs.get_job(self._storage, repo_ref.repo_id, job_id)

    def list_jobs(self, run_name: str) -> List[Job]:
        return base_jobs.list_jobs(self._storage, self.repo.repo_id, run_name)

    def run_job(self, job: Job, failed_to_start_job_new_status: JobStatus):
        base_jobs.run_job(self._storage, self._compute, job, failed_to_start_job_new_status)

    def stop_job(self, job_id: str, abort: bool):
        base_jobs.stop_job(self._storage, self._compute, self.repo.repo_id, job_id, abort)

    def list_job_heads(
        self, run_name: Optional[str] = None, repo_ref: Optional[RepoRef] = None
    ) -> List[JobHead]:
        repo_ref = repo_ref or self.repo.repo_ref
        return base_jobs.list_job_heads(self._storage, repo_ref.repo_id, run_name)

    def delete_job_head(self, job_id: str):
        base_jobs.delete_job_head(self._storage, self.repo.repo_id, job_id)

    def list_run_heads(
        self,
        run_name: Optional[str] = None,
        include_request_heads: bool = True,
        interrupted_job_new_status: JobStatus = JobStatus.FAILED,
        repo_ref: Optional[RepoRef] = None,
    ) -> List[RunHead]:
        job_heads = self.list_job_heads(run_name, repo_ref=repo_ref)
        return base_runs.get_run_heads(
            self._storage,
            self._compute,
            job_heads,
            include_request_heads,
            interrupted_job_new_status,
        )

    def poll_logs(
        self,
        job_heads: List[JobHead],
        start_time: int,
        attached: bool,
    ) -> Generator[LogEvent, None, None]:
        return logs.poll_logs(
            self._storage, self._compute, self.repo.repo_id, job_heads, start_time, attached
        )

    def list_run_artifact_files(self, run_name: str) -> List[Artifact]:
        return base_artifacts.list_run_artifact_files(self._storage, self.repo.repo_id, run_name)

    def download_run_artifact_files(
        self,
        run_name: str,
        output_dir: Optional[PathLike],
        files_path: Optional[PathLike] = None,
    ):
        list_artifacts = self.list_run_artifact_files(run_name)
        base_artifacts.download_run_artifact_files(
            storage=self._storage,
            repo_id=self.repo.repo_id,
            artifacts=list_artifacts,
            output_dir=output_dir,
            files_path=files_path,
        )

    def upload_job_artifact_files(
        self,
        job_id: str,
        artifact_name: str,
        artifact_path: PathLike,
        local_path: PathLike,
    ):
        base_artifacts.upload_job_artifact_files(
            storage=self._storage,
            repo_id=self.repo.repo_id,
            job_id=job_id,
            artifact_name=artifact_name,
            artifact_path=artifact_path,
            local_path=local_path,
        )

    def list_tag_heads(self) -> List[TagHead]:
        return base_tags.list_tag_heads(self._storage, self.repo.repo_id)

    def get_tag_head(self, tag_name: str) -> Optional[TagHead]:
        return base_tags.get_tag_head(self._storage, self.repo.repo_id, tag_name)

    def add_tag_from_run(
        self,
        tag_name: str,
        run_name: str,
        run_jobs: Optional[List[Job]],
    ):
        base_tags.create_tag_from_run(
            self._storage,
            self.repo.repo_id,
            tag_name,
            run_name,
            run_jobs,
        )

    def add_tag_from_local_dirs(self, tag_name: str, local_dirs: List[str]):
        base_tags.create_tag_from_local_dirs(
            self._storage,
            self.repo,
            tag_name,
            local_dirs,
            self.type,
        )

    def delete_tag_head(self, tag_head: TagHead):
        base_tags.delete_tag(self._storage, self.repo.repo_id, tag_head)

    def update_repo_last_run_at(self, last_run_at: int):
        base_repos.update_repo_last_run_at(self._storage, self.repo.repo_ref, last_run_at)

    def get_repo_credentials(self) -> Optional[RepoCredentials]:
        return base_repos.get_repo_credentials(self._secrets_manager)

    def save_repo_credentials(self, repo_credentials: RepoCredentials):
        base_repos.save_repo_credentials(
            self._secrets_manager,
            repo_credentials,
        )

    def list_secret_names(self) -> List[str]:
        return base_secrets.list_secret_names(self._storage, self.repo.repo_id)

    def get_secret(self, secret_name: str) -> Optional[Secret]:
        return base_secrets.get_secret(self._secrets_manager, secret_name)

    def add_secret(self, secret: Secret):
        base_secrets.add_secret(
            self._storage,
            self._secrets_manager,
            secret,
        )

    def update_secret(self, secret: Secret):
        base_secrets.update_secret(
            self._storage,
            self._secrets_manager,
            secret,
        )

    def delete_secret(self, secret_name: str):
        base_secrets.delete_secret(
            self._storage,
            self._secrets_manager,
            secret_name,
        )

    def get_artifacts_path(self) -> Path:
        return artifacts.get_artifacts_path(self.backend_config.path, self.repo.repo_id)

    @classmethod
    def get_configurator(cls):
        return None

    def delete_workflow_cache(self, workflow_name: str):
        base_cache.delete_workflow_cache(
            self._storage, self.repo.repo_id, self.repo.repo_user_id, workflow_name
        )
