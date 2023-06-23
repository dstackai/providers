from typing import Any, List, Optional

import requests

API_URL = "https://cloud.lambdalabs.com/api/v1"


class LambdaAPIClient:
    def __init__(self, api_key: str):
        self.api_key = api_key

    def list_instance_types(self):
        resp = self._make_request("GET", "/instance-types")
        if resp.ok:
            return resp.json()["data"]
        resp.raise_for_status()

    def list_instances(self):
        resp = self._make_request("GET", "/instances")
        if resp.ok:
            return resp.json()["data"]
        resp.raise_for_status()

    def launch_instances(
        self,
        region_name: str,
        instance_type_name: str,
        ssh_key_names: List[str],
        file_system_names: List[str],
        quantity: int,
        name: Optional[str],
    ) -> List[str]:
        data = {
            "region_name": region_name,
            "instance_type_name": instance_type_name,
            "ssh_key_names": ssh_key_names,
            "file_system_names": file_system_names,
            "quantity": quantity,
            "name": name,
        }
        resp = self._make_request("POST", "/instance-operations/launch", data)
        if resp.ok:
            return resp.json()["data"]["instance_ids"]
        resp.raise_for_status()

    def _make_request(self, method: str, path: str, data: Any = None):
        return requests.request(
            method=method,
            url=API_URL + path,
            json=data,
            headers={"Authorization": f"Bearer {self.api_key}"},
        )

    def _url(self, path: str) -> str:
        return API_URL + path
