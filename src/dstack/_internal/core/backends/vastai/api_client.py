from typing import List, Optional, Union

import requests

import dstack._internal.server.services.docker as docker
from dstack._internal.core.errors import NoCapacityError
from dstack._internal.core.models.configurations import RegistryAuth

DISK_SIZE = 80  # TODO(egor-s): use requirements instead


class VastAIAPIClient:
    # TODO(egor-s): handle error 429
    # TODO(egor-s): cache responses to avoid error 429
    def __init__(self, api_key: str):
        self.api_url = "https://console.vast.ai/api/v0".rstrip("/")
        self.api_key = api_key
        self.s = requests.Session()

    def get_bundle(self, bundle_id: Union[str, int]) -> Optional[dict]:
        resp = self.s.post(self._url(f"/bundles/"), json={"id": {"eq": bundle_id}})
        resp.raise_for_status()
        data = resp.json()
        offers = data["offers"]
        return offers[0] if offers else None

    def create_instance(
        self,
        instance_name: str,
        bundle_id: Union[str, int],
        image_name: str,
        onstart: str,
        registry_auth: Optional[RegistryAuth] = None,
    ) -> dict:
        """
        Args:
            instance_name: instance label
            bundle_id: desired host
            image_name: docker image name
            onstart: commands to run on start
            registry_auth: registry auth credentials for private images

        Raises:
            NoCapacityError: if instance cannot be created

        Returns:
            create instance response
        """
        image_login = None
        if registry_auth:
            registry = docker.parse_image_name(image_name).registry or "docker.io"
            image_login = f"-u {registry_auth.username} -p {registry_auth.password} {registry}"
        payload = {
            "client_id": "me",
            "image": image_name,
            "disk": DISK_SIZE,
            "label": instance_name,
            "env": {
                "-p 10022:10022": "1",
            },
            "onstart": onstart,
            "runtype": "ssh_direc",
            "image_login": image_login,
            "python_utf8": False,
            "lang_utf8": False,
            "use_jupyter_lab": False,
            "jupyter_dir": None,
            "create_from": None,
            "force": False,
        }
        resp = self.s.put(self._url(f"/asks/{bundle_id}/"), json=payload)
        if resp.status_code != 200 or not (data := resp.json())["success"]:
            raise NoCapacityError(resp.text)
        return data

    def destroy_instance(self, instance_id: Union[str, int]) -> bool:
        """
        Args:
            instance_id: instance to destroy

        Returns:
            True if instance was destroyed successfully
        """
        resp = self.s.delete(self._url(f"/instances/{instance_id}/"))
        if resp.status_code != 200 or not resp.json()["success"]:
            return False
        return True

    def get_instances(self) -> List[dict]:
        resp = self.s.get(self._url(f"/instances/"))
        resp.raise_for_status()
        data = resp.json()
        return data["instances"]

    def get_instance(self, instance_id: Union[str, int]) -> Optional[dict]:
        instances = self.get_instances()
        for instance in instances:
            if instance["id"] == int(instance_id):
                return instance
        return None

    def request_logs(self, instance_id: Union[str, int]) -> dict:
        resp = self.s.put(
            self._url(f"/instances/request_logs/{instance_id}/"), json={"tail": "1000"}
        )
        resp.raise_for_status()
        data = resp.json()
        if not data["success"]:
            raise requests.HTTPError(data)
        return data

    def auth_test(self) -> bool:
        try:
            self.get_instances()
            return True
        except requests.HTTPError:
            return False

    def _url(self, path):
        return f"{self.api_url}/{path.lstrip('/')}?api_key={self.api_key}"
