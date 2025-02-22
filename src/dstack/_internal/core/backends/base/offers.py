from dataclasses import asdict
from typing import Callable, List, Optional

import gpuhunt

from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.instances import (
    Disk,
    Gpu,
    InstanceOffer,
    InstanceType,
    Resources,
)
from dstack._internal.core.models.resources import DEFAULT_DISK, Memory, Range
from dstack._internal.core.models.runs import Requirements

# Offers not supported by all dstack versions are hidden behind one or more flags.
# This list enables the flags that are currently supported.
SUPPORTED_GPUHUNT_FLAGS = [
    "oci-spot",
]


def get_catalog_offers(
    backend: BackendType,
    locations: Optional[List[str]] = None,
    requirements: Optional[Requirements] = None,
    configurable_disk_size: Range[Memory] = Range[Memory](min=Memory.parse("1GB"), max=None),
    extra_filter: Optional[Callable[[InstanceOffer], bool]] = None,
    catalog: Optional[gpuhunt.Catalog] = None,
) -> List[InstanceOffer]:
    provider = backend.value
    if backend == BackendType.LAMBDA:
        provider = "lambdalabs"
    q = requirements_to_query_filter(requirements)
    q.provider = [provider]
    offers = []

    catalog = catalog if catalog is not None else gpuhunt.default_catalog()
    for item in catalog.query(**asdict(q)):
        if locations is not None and item.location not in locations:
            continue
        offer = catalog_item_to_offer(backend, item, requirements, configurable_disk_size)
        if offer is None:
            continue
        if extra_filter is not None and not extra_filter(offer):
            continue
        offers.append(offer)
    return offers


def catalog_item_to_offer(
    backend: BackendType,
    item: gpuhunt.CatalogItem,
    requirements: Optional[Requirements],
    configurable_disk_size: Range[Memory],
) -> Optional[InstanceOffer]:
    gpus = []
    if item.gpu_count > 0:
        gpu = Gpu(
            vendor=item.gpu_vendor, name=item.gpu_name, memory_mib=round(item.gpu_memory * 1024)
        )
        gpus = [gpu] * item.gpu_count
    disk_size_mib = choose_disk_size_mib(
        catalog_item_disk_size_gib=item.disk_size,
        requirements_disk_size=requirements.resources.disk.size
        if requirements and requirements.resources.disk
        else None,
        configurable_disk_size=configurable_disk_size,
    )
    if disk_size_mib is None:
        return None
    resources = Resources(
        cpus=item.cpu,
        memory_mib=round(item.memory * 1024),
        gpus=gpus,
        spot=item.spot,
        disk=Disk(size_mib=disk_size_mib),
    )
    resources.description = resources.pretty_format()
    return InstanceOffer(
        backend=backend,
        instance=InstanceType(
            name=item.instance_name,
            resources=resources,
        ),
        region=item.location,
        price=item.price,
    )


def offer_to_catalog_item(offer: InstanceOffer) -> gpuhunt.CatalogItem:
    gpu_count = len(offer.instance.resources.gpus)
    gpu_vendor = None
    gpu_name = None
    gpu_memory = None
    if gpu_count > 0:
        gpu = offer.instance.resources.gpus[0]
        gpu_vendor = gpu.vendor
        gpu_name = gpu.name
        gpu_memory = gpu.memory_mib / 1024
    return gpuhunt.CatalogItem(
        provider=offer.backend.value,
        instance_name=offer.instance.name,
        location=offer.region,
        price=offer.price,
        cpu=offer.instance.resources.cpus,
        memory=offer.instance.resources.memory_mib / 1024,
        gpu_count=gpu_count,
        gpu_vendor=gpu_vendor,
        gpu_name=gpu_name,
        gpu_memory=gpu_memory,
        spot=offer.instance.resources.spot,
        disk_size=offer.instance.resources.disk.size_mib / 1024,
    )


def requirements_to_query_filter(req: Optional[Requirements]) -> gpuhunt.QueryFilter:
    q = gpuhunt.QueryFilter(allowed_flags=SUPPORTED_GPUHUNT_FLAGS)
    if req is None:
        return q

    q.max_price = req.max_price
    q.spot = req.spot

    res = req.resources
    if res.cpu:
        q.min_cpu = res.cpu.min
        q.max_cpu = res.cpu.max
    if res.memory:
        q.min_memory = res.memory.min
        q.max_memory = res.memory.max
    if res.disk:
        q.min_disk_size = res.disk.size.min
        q.max_disk_size = res.disk.size.max

    if res.gpu:
        q.gpu_vendor = res.gpu.vendor
        q.gpu_name = res.gpu.name
        if res.gpu.memory:
            q.min_gpu_memory = res.gpu.memory.min
            q.max_gpu_memory = res.gpu.memory.max
        if res.gpu.count:
            q.min_gpu_count = res.gpu.count.min
            q.max_gpu_count = res.gpu.count.max
        if res.gpu.total_memory:
            q.min_total_gpu_memory = res.gpu.total_memory.min
            q.max_total_gpu_memory = res.gpu.total_memory.max
        if res.gpu.compute_capability:
            q.min_compute_capability = res.gpu.compute_capability

    return q


def match_requirements(
    offers: List[InstanceOffer], requirements: Optional[Requirements]
) -> List[InstanceOffer]:
    query_filter = requirements_to_query_filter(requirements)
    filtered_offers = []
    for offer in offers:
        catalog_item = offer_to_catalog_item(offer)
        if gpuhunt.matches(catalog_item, q=query_filter):
            filtered_offers.append(offer)
    return filtered_offers


def choose_disk_size_mib(
    catalog_item_disk_size_gib: Optional[float],
    requirements_disk_size: Optional[Range[Memory]],
    configurable_disk_size: Range[Memory],
) -> Optional[int]:
    if catalog_item_disk_size_gib:
        disk_size_gib = catalog_item_disk_size_gib
    else:
        disk_size_range = requirements_disk_size or DEFAULT_DISK.size
        disk_size_range = disk_size_range.intersect(configurable_disk_size)
        if disk_size_range is None:
            return None
        disk_size_gib = disk_size_range.min

    return round(disk_size_gib * 1024)
