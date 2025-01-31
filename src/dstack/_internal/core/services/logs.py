import re
import urllib.parse
from typing import Dict, List, Optional

from dstack._internal.core.models.runs import AppSpec
from dstack._internal.utils.common import concat_url_path


class URLReplacer:
    def __init__(
        self,
        app_specs: List[AppSpec],
        ports: Dict[int, int],
        hostname: str,
        secure: bool,
        path_prefix: str = "",
        ip_address: Optional[str] = None,
    ):
        self.app_specs = {app_spec.port: app_spec for app_spec in app_specs}
        self.ports = ports
        self.hostname = hostname
        self.secure = secure
        self.path_prefix = path_prefix.encode()

        hosts = ["localhost", "0.0.0.0", "127.0.0.1"]
        if ip_address and ip_address not in hosts:
            hosts.append(ip_address)
        hosts_re = "|".join(hosts)
        self._url_re = re.compile(
            rf"http://(?:{hosts_re}):(\d+)\b([-a-zA-Z0-9()@:%_\+.~#?&//=]*)".encode()
        )

    def _replace_url(self, match: re.Match) -> bytes:
        remote_port = int(match.group(1))
        if remote_port not in self.ports:
            return match.group(0)
        local_port = self.ports[remote_port]
        omit_port = (not self.secure and local_port == 80) or (self.secure and local_port == 443)

        app_spec = self.app_specs.get(remote_port)
        url = urllib.parse.urlparse(match.group(0))
        qs = {k: v[0] for k, v in urllib.parse.parse_qs(url.query).items()}
        if app_spec and app_spec.url_query_params is not None:
            qs.update({k.encode(): v.encode() for k, v in app_spec.url_query_params.items()})
        path = url.path
        if not path.startswith(self.path_prefix.removesuffix(b"/")):
            path = concat_url_path(self.path_prefix, path)

        url = url._replace(
            scheme=("https" if self.secure else "http").encode(),
            netloc=(self.hostname if omit_port else f"{self.hostname}:{local_port}").encode(),
            path=path,
            query=urllib.parse.urlencode(qs).encode(),
        )
        return url.geturl()

    def __call__(self, entry: bytes) -> bytes:
        return self._url_re.sub(self._replace_url, entry)
