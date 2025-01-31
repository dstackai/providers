from datetime import datetime
from typing import List, Optional, Union
from uuid import UUID

from pydantic import parse_obj_as

from dstack._internal.core.models.common import is_core_model_instance
from dstack._internal.core.models.configurations import (
    STRIP_PREFIX_DEFAULT,
    ServiceConfiguration,
)
from dstack._internal.core.models.pools import Instance
from dstack._internal.core.models.profiles import Profile
from dstack._internal.core.models.runs import (
    ApplyRunPlanInput,
    PoolInstanceOffers,
    Requirements,
    Run,
    RunPlan,
    RunSpec,
)
from dstack._internal.server.schemas.runs import (
    ApplyRunPlanRequest,
    CreateInstanceRequest,
    DeleteRunsRequest,
    GetOffersRequest,
    GetRunPlanRequest,
    GetRunRequest,
    ListRunsRequest,
    StopRunsRequest,
    SubmitRunRequest,
)
from dstack.api.server._group import APIClientGroup


class RunsAPIClient(APIClientGroup):
    def list(
        self,
        project_name: Optional[str],
        repo_id: Optional[str],
        username: Optional[str] = None,
        only_active: bool = False,
        prev_submitted_at: Optional[datetime] = None,
        prev_run_id: Optional[UUID] = None,
        limit: int = 100,
        ascending: bool = False,
    ) -> List[Run]:
        body = ListRunsRequest(
            project_name=project_name,
            repo_id=repo_id,
            username=username,
            only_active=only_active,
            prev_submitted_at=prev_submitted_at,
            prev_run_id=prev_run_id,
            limit=limit,
            ascending=ascending,
        )
        resp = self._request("/api/runs/list", body=body.json())
        return parse_obj_as(List[Run.__response__], resp.json())

    def get(self, project_name: str, run_name: str) -> Run:
        body = GetRunRequest(run_name=run_name)
        # dstack versions prior to 0.18.34 don't support id field, and we don't use it here either
        json_body = body.json(exclude={"id"})
        resp = self._request(f"/api/project/{project_name}/runs/get", body=json_body)
        return parse_obj_as(Run.__response__, resp.json())

    def get_plan(self, project_name: str, run_spec: RunSpec) -> RunPlan:
        body = GetRunPlanRequest(run_spec=run_spec)
        resp = self._request(
            f"/api/project/{project_name}/runs/get_plan",
            body=body.json(exclude=_get_run_spec_excludes(run_spec)),
        )
        return parse_obj_as(RunPlan.__response__, resp.json())

    def apply_plan(
        self,
        project_name: str,
        plan: Union[RunPlan, ApplyRunPlanInput],
        force: bool = False,
    ) -> Run:
        plan_input: ApplyRunPlanInput = ApplyRunPlanInput.__response__.parse_obj(plan)
        body = ApplyRunPlanRequest(plan=plan_input, force=force)
        resp = self._request(f"/api/project/{project_name}/runs/apply", body=body.json())
        return parse_obj_as(Run.__response__, resp.json())

    def submit(self, project_name: str, run_spec: RunSpec) -> Run:
        body = SubmitRunRequest(run_spec=run_spec)
        resp = self._request(
            f"/api/project/{project_name}/runs/submit",
            body=body.json(exclude=_get_run_spec_excludes(run_spec)),
        )
        return parse_obj_as(Run.__response__, resp.json())

    def stop(self, project_name: str, runs_names: List[str], abort: bool):
        body = StopRunsRequest(runs_names=runs_names, abort=abort)
        self._request(f"/api/project/{project_name}/runs/stop", body=body.json())

    def delete(self, project_name: str, runs_names: List[str]):
        body = DeleteRunsRequest(runs_names=runs_names)
        self._request(f"/api/project/{project_name}/runs/delete", body=body.json())

    # FIXME: get_offers and create_instance do not belong runs api

    def get_offers(
        self, project_name: str, profile: Profile, requirements: Requirements
    ) -> PoolInstanceOffers:
        body = GetOffersRequest(profile=profile, requirements=requirements)
        resp = self._request(f"/api/project/{project_name}/runs/get_offers", body=body.json())
        return parse_obj_as(PoolInstanceOffers.__response__, resp.json())

    def create_instance(
        self,
        project_name: str,
        profile: Profile,
        requirements: Requirements,
    ) -> Instance:
        body = CreateInstanceRequest(profile=profile, requirements=requirements)
        resp = self._request(f"/api/project/{project_name}/runs/create_instance", body=body.json())
        return parse_obj_as(Instance.__response__, resp.json())


def _get_run_spec_excludes(run_spec: RunSpec) -> Optional[dict]:
    spec_excludes: dict[str, set[str]] = {}
    configuration_excludes: set[str] = set()
    profile_excludes: set[str] = set()
    configuration = run_spec.configuration
    profile = run_spec.profile

    # client >= 0.18.18 / server <= 0.18.17 compatibility tweak
    if not configuration.privileged:
        configuration_excludes.add("privileged")
    # client >= 0.18.23 / server <= 0.18.22 compatibility tweak
    if configuration.type == "service" and configuration.gateway is None:
        configuration_excludes.add("gateway")
    # client >= 0.18.30 / server <= 0.18.29 compatibility tweak
    if run_spec.configuration.user is None:
        configuration_excludes.add("user")
    # client >= 0.18.30 / server <= 0.18.29 compatibility tweak
    if configuration.reservation is None:
        configuration_excludes.add("reservation")
    if profile is not None and profile.reservation is None:
        profile_excludes.add("reservation")
    if configuration.idle_duration is None:
        configuration_excludes.add("idle_duration")
    if profile is not None and profile.idle_duration is None:
        profile_excludes.add("idle_duration")
    # client >= 0.18.38 / server <= 0.18.37 compatibility tweak
    if configuration.stop_duration is None:
        configuration_excludes.add("stop_duration")
    if profile is not None and profile.stop_duration is None:
        profile_excludes.add("stop_duration")
    # client >= 0.18.40 / server <= 0.18.39 compatibility tweak
    if (
        is_core_model_instance(configuration, ServiceConfiguration)
        and configuration.strip_prefix == STRIP_PREFIX_DEFAULT
    ):
        configuration_excludes.add("strip_prefix")

    if configuration_excludes:
        spec_excludes["configuration"] = configuration_excludes
    if profile_excludes:
        spec_excludes["profile"] = profile_excludes
    if spec_excludes:
        return {"run_spec": spec_excludes}
    return None
