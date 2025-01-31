from typing import List, Optional, Union

from pydantic import parse_obj_as

from dstack._internal.core.models.fleets import Fleet, FleetPlan, FleetSpec
from dstack._internal.server.schemas.fleets import (
    CreateFleetRequest,
    DeleteFleetInstancesRequest,
    DeleteFleetsRequest,
    GetFleetPlanRequest,
    GetFleetRequest,
)
from dstack.api.server._group import APIClientGroup


class FleetsAPIClient(APIClientGroup):
    def list(self, project_name: str) -> List[Fleet]:
        resp = self._request(f"/api/project/{project_name}/fleets/list")
        return parse_obj_as(List[Fleet.__response__], resp.json())

    def get(self, project_name: str, name: str) -> Fleet:
        body = GetFleetRequest(name=name)
        resp = self._request(
            f"/api/project/{project_name}/fleets/get",
            body=body.json(exclude={"id"}),  # `id` is not supported in pre-0.18.36 servers
        )
        return parse_obj_as(Fleet.__response__, resp.json())

    def get_plan(
        self,
        project_name: str,
        spec: FleetSpec,
    ) -> FleetPlan:
        body = GetFleetPlanRequest(spec=spec)
        body_json = body.json(exclude=_get_fleet_spec_excludes(spec))
        resp = self._request(f"/api/project/{project_name}/fleets/get_plan", body=body_json)
        return parse_obj_as(FleetPlan.__response__, resp.json())

    def create(
        self,
        project_name: str,
        spec: FleetSpec,
    ) -> Fleet:
        body = CreateFleetRequest(spec=spec)
        body_json = body.json(exclude=_get_fleet_spec_excludes(spec))
        resp = self._request(f"/api/project/{project_name}/fleets/create", body=body_json)
        return parse_obj_as(Fleet.__response__, resp.json())

    def delete(self, project_name: str, names: List[str]) -> None:
        body = DeleteFleetsRequest(names=names)
        self._request(f"/api/project/{project_name}/fleets/delete", body=body.json())

    def delete_instances(self, project_name: str, name: str, instance_nums: List[int]) -> None:
        body = DeleteFleetInstancesRequest(name=name, instance_nums=instance_nums)
        self._request(f"/api/project/{project_name}/fleets/delete_instances", body=body.json())


_ExcludeDict = dict[str, Union[bool, set[str], "_ExcludeDict"]]


def _get_fleet_spec_excludes(fleet_spec: FleetSpec) -> Optional[_ExcludeDict]:
    spec_excludes: _ExcludeDict = {}
    configuration_excludes: _ExcludeDict = {}
    profile_excludes: set[str] = set()
    ssh_hosts_excludes: set[str] = set()

    # TODO: Can be removed in 0.19
    if fleet_spec.configuration_path is None:
        spec_excludes["configuration_path"] = True
    if fleet_spec.configuration.ssh_config is not None:
        if all(
            isinstance(h, str) or h.internal_ip is None
            for h in fleet_spec.configuration.ssh_config.hosts
        ):
            ssh_hosts_excludes.add("internal_ip")
        if all(
            isinstance(h, str) or h.blocks == 1 for h in fleet_spec.configuration.ssh_config.hosts
        ):
            ssh_hosts_excludes.add("blocks")
    # client >= 0.18.30 / server <= 0.18.29 compatibility tweak
    if fleet_spec.configuration.reservation is None:
        configuration_excludes["reservation"] = True
    if fleet_spec.profile is not None and fleet_spec.profile.reservation is None:
        profile_excludes.add("reservation")
    if fleet_spec.configuration.idle_duration is None:
        configuration_excludes["idle_duration"] = True
    if fleet_spec.profile is not None and fleet_spec.profile.idle_duration is None:
        profile_excludes.add("idle_duration")
    # client >= 0.18.38 / server <= 0.18.37 compatibility tweak
    if fleet_spec.profile is not None and fleet_spec.profile.stop_duration is None:
        profile_excludes.add("stop_duration")
    if fleet_spec.configuration.blocks == 1:
        configuration_excludes["blocks"] = True

    if ssh_hosts_excludes:
        configuration_excludes["ssh_config"] = {"hosts": {"__all__": ssh_hosts_excludes}}
    if configuration_excludes:
        spec_excludes["configuration"] = configuration_excludes
    if profile_excludes:
        spec_excludes["profile"] = profile_excludes
    if spec_excludes:
        return {"spec": spec_excludes}
    return None
