import re
import sys
from pathlib import Path

from setuptools import find_packages, setup

project_dir = Path(__file__).parent


def get_version():
    text = (project_dir / "src" / "dstack" / "version.py").read_text()
    match = re.compile(r"__version__\s*=\s*\"?([^\n\"]+)\"?.*").match(text)
    if match:
        if match.group(1) != "None":
            return match.group(1)
        else:
            return None
    else:
        sys.exit("Can't parse version.py")


def get_long_description():
    return re.sub(
        r"<picture>\s*|<source[^>]*>\s*|\s*</picture>|<video[^>]*>\s*|</video>\s*|### Demo\s*",
        "",
        open(project_dir / "README.md").read(),
    )


BASE_DEPS = [
    "pyyaml",
    "requests",
    "typing-extensions>=4.0.0",
    "cryptography",
    "packaging",
    "python-dateutil",
    "gitpython",
    "jsonschema",
    "paramiko>=3.2.0",
    "cursor",
    "rich",
    "rich-argparse",
    "tqdm",
    "simple-term-menu",
    "pydantic>=1.10.10,<2.0.0",
    "pydantic-duality>=1.2.4",
    "websocket-client",
    "python-multipart>=0.0.16",
    "filelock",
    "psutil",
    "gpuhunt>=0.1.0,<0.2.0",
    "argcomplete>=3.5.0",
]

GATEWAY_AND_SERVER_COMMON_DEPS = [
    "fastapi",
    "starlette>=0.26.0",
    "uvicorn",
    "aiorwlock",
    "aiocache",
    "httpx",
    "jinja2",
]

SERVER_DEPS = GATEWAY_AND_SERVER_COMMON_DEPS + [
    "watchfiles",
    "sqlalchemy[asyncio]>=2.0.0",
    "sqlalchemy_utils>=0.40.0",
    "alembic>=1.10.2",
    "apscheduler<4",
    "aiosqlite",
    "docker>=6.0.0",
    "python-dxf==12.1.0",
    "sentry-sdk[fastapi]",
    "alembic-postgresql-enum",
    "asyncpg",
    "cachetools",
    "python-json-logger>=3.1.0",
    "grpcio>=1.50",  # indirect
]

AWS_DEPS = [
    "boto3",
    "botocore",
]

AZURE_DEPS = [
    "azure-identity>=1.12.0",
    "azure-mgmt-subscription>=3.1.1",
    "azure-mgmt-compute>=29.1.0",
    "azure-mgmt-network>=23.0.0,<28.0.0",
    "azure-mgmt-resource>=22.0.0",
    "azure-mgmt-authorization>=3.0.0",
]

GCP_DEPS = [
    "google-auth>=2.3.0",  # indirect
    "google-cloud-storage>=2.0.0",
    "google-cloud-compute>=1.5.0",
    "google-cloud-logging>=2.0.0",
    "google-api-python-client>=2.80.0",
    "google-cloud-billing>=1.11.0",
    "google-cloud-tpu>=1.18.3",
]

DATACRUNCH_DEPS = ["datacrunch"]

KUBERNETES_DEPS = ["kubernetes"]

LAMBDA_DEPS = AWS_DEPS

OCI_DEPS = ["oci"]

ALL_DEPS = (
    SERVER_DEPS + AWS_DEPS + AZURE_DEPS + GCP_DEPS + DATACRUNCH_DEPS + KUBERNETES_DEPS + OCI_DEPS
)

setup(
    name="dstack",
    version=get_version(),
    author="Andrey Cheptsov",
    author_email="andrey@dstack.ai",
    package_dir={"": "src"},
    packages=find_packages("src"),
    package_data={
        "dstack._internal.proxy.gateway": ["resources/**/*"],
        "dstack._internal.server": ["statics/**/*"],
    },
    include_package_data=True,
    scripts=[],
    entry_points={
        "console_scripts": ["dstack=dstack._internal.cli.main:main"],
    },
    url="https://dstack.ai",
    project_urls={
        "Source": "https://github.com/dstackai/dstack",
        "Docs": "https://dstack.ai/docs",
        "Issues": "https://github.com/dstackai/dstack/issues",
        "Changelog": "https://github.com/dstackai/dstack/releases",
        "Discord": "https://discord.gg/u8SmfwPpMd",
    },
    description="dstack is an open-source orchestration engine for running AI workloads on any cloud or on-premises.",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    python_requires=">=3.9",
    install_requires=BASE_DEPS,
    extras_require={
        "all": ALL_DEPS,
        "gateway": GATEWAY_AND_SERVER_COMMON_DEPS,
        "server": SERVER_DEPS,
        "aws": SERVER_DEPS + AWS_DEPS,
        "azure": SERVER_DEPS + AZURE_DEPS,
        "datacrunch": SERVER_DEPS + DATACRUNCH_DEPS,
        "gcp": SERVER_DEPS + GCP_DEPS,
        "kubernetes": SERVER_DEPS + KUBERNETES_DEPS,
        "lambda": SERVER_DEPS + LAMBDA_DEPS,
        "oci": SERVER_DEPS + OCI_DEPS,
    },
    classifiers=[
        "Development Status :: 4 - Beta",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Programming Language :: Python :: 3",
    ],
)
