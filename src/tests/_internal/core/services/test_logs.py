import pytest

from dstack._internal.core.models.runs import AppSpec
from dstack._internal.core.services.logs import URLReplacer

localhost = "127.0.0.1"


class TestTaskURLReplacer:
    def test_empty_mapping(self):
        replacer = URLReplacer(
            ports={},
            app_specs=[AppSpec(port=3001, app_name="qwerty")],
            hostname=localhost,
            secure=False,
        )
        assert replacer(b"http://0.0.0.0:3001/qwerty") == b"http://0.0.0.0:3001/qwerty"

    def test_hostname(self):
        replacer = URLReplacer(
            ports={3001: 3001},
            app_specs=[AppSpec(port=3001, app_name="qwerty")],
            hostname="host.name",
            secure=False,
        )
        assert replacer(b"http://0.0.0.0:3001/qwerty") == b"http://host.name:3001/qwerty"

    def test_unique_mapping(self):
        replacer = URLReplacer(
            ports={4000: 5000, 3001: 5501},
            app_specs=[AppSpec(port=3001, app_name="qwerty")],
            hostname=localhost,
            secure=False,
        )
        assert replacer(b"http://0.0.0.0:3001/qwerty") == b"http://127.0.0.1:5501/qwerty"

    def test_with_query_params(self):
        replacer = URLReplacer(
            ports={4000: 5000, 3001: 5501},
            app_specs=[AppSpec(port=3001, app_name="qwerty", url_query_params={"q": "foobar"})],
            hostname=localhost,
            secure=False,
        )
        assert replacer(b"http://0.0.0.0:3001/qwerty") == b"http://127.0.0.1:5501/qwerty?q=foobar"

    def test_same_url(self):
        replacer = URLReplacer(
            ports={4000: 5000, 3001: 5501},
            app_specs=[AppSpec(port=3001, app_name="qwerty")],
            hostname=localhost,
            secure=False,
        )
        assert (
            replacer(b"http://0.0.0.0:3001/qwerty and http://0.0.0.0:3001/foobar")
            == b"http://127.0.0.1:5501/qwerty and http://127.0.0.1:5501/foobar"
        )

    def test_different_urls(self):
        replacer = URLReplacer(
            ports={4000: 5000, 3001: 5501, 3002: 5502},
            app_specs=[
                AppSpec(port=3001, app_name="qwerty"),
                AppSpec(port=3002, app_name="foobar"),
            ],
            hostname=localhost,
            secure=False,
        )
        assert (
            replacer(b"http://0.0.0.0:3001/qwerty and http://0.0.0.0:3002/foobar")
            == b"http://127.0.0.1:5501/qwerty and http://127.0.0.1:5502/foobar"
        )

    def test_circular_mapping(self):
        replacer = URLReplacer(
            ports={4000: 5000, 3001: 3002, 3002: 3003, 3003: 3001},
            app_specs=[
                AppSpec(port=3001, app_name="qwerty"),
                AppSpec(port=3002, app_name="foobar"),
                AppSpec(port=3003, app_name="asdasd"),
            ],
            hostname=localhost,
            secure=False,
        )
        assert (
            replacer(
                b"http://0.0.0.0:3001/qwerty and http://0.0.0.0:3002/foobar and http://0.0.0.0:3003/asdasd"
            )
            == b"http://127.0.0.1:3002/qwerty and http://127.0.0.1:3003/foobar and http://127.0.0.1:3001/asdasd"
        )

    def test_fastapi(self):
        replacer = URLReplacer(
            ports={3615: 53615},
            app_specs=[AppSpec(port=3615, app_name="fastapi")],
            hostname=localhost,
            secure=False,
        )
        assert (
            replacer(
                b"\x1b[32mINFO\x1b[0m:     Uvicorn running on \x1b[1mhttp://0.0.0.0:3615\x1b[0m (Press CTRL+C to quit)"
            )
            == b"\x1b[32mINFO\x1b[0m:     Uvicorn running on \x1b[1mhttp://127.0.0.1:53615\x1b[0m (Press CTRL+C to quit)"
        )

    def test_ip_address(self):
        replacer = URLReplacer(
            ports={3001: 3002},
            app_specs=[AppSpec(port=3001, app_name="qwerty")],
            hostname=localhost,
            secure=False,
            ip_address="1.2.3.4",
        )
        assert replacer(b"http://1.2.3.4:3001/qwerty") == b"http://127.0.0.1:3002/qwerty"


class TestServiceURLReplacer:
    def test_no_apps(self):
        replacer = URLReplacer(ports={8000: 8080}, app_specs=[], hostname="1.2.3.4", secure=False)
        assert replacer(b"http://0.0.0.0:8000") == b"http://1.2.3.4:8080"

    def test_omit_http_default_port(self):
        replacer = URLReplacer(ports={8000: 80}, app_specs=[], hostname="1.2.3.4", secure=False)
        assert replacer(b"http://0.0.0.0:8000/qwerty") == b"http://1.2.3.4/qwerty"

    def test_omit_https_default_port(self):
        replacer = URLReplacer(
            ports={8000: 443}, app_specs=[], hostname="secure.host.com", secure=True
        )
        assert replacer(b"http://0.0.0.0:8000/qwerty") == b"https://secure.host.com/qwerty"

    @pytest.mark.parametrize(
        ("in_path", "out_path"),
        [
            ("", "/proxy/services/main/service/"),
            ("/", "/proxy/services/main/service/"),
            ("/a/b/c", "/proxy/services/main/service/a/b/c"),
            ("/proxy/services/main/service", "/proxy/services/main/service"),
            ("/proxy/services/main/service/", "/proxy/services/main/service/"),
            ("/proxy/services/main/service/a/b/c", "/proxy/services/main/service/a/b/c"),
        ],
    )
    def test_adds_prefix_unless_already_present(self, in_path: str, out_path: str) -> None:
        replacer = URLReplacer(
            ports={8888: 3000},
            app_specs=[],
            hostname="0.0.0.0",
            secure=False,
            path_prefix="/proxy/services/main/service/",
        )
        assert (
            replacer(f"http://0.0.0.0:8888{in_path}".encode())
            == f"http://0.0.0.0:3000{out_path}".encode()
        )
