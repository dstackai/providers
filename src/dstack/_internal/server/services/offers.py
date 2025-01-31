from typing import List, Literal, Optional, Tuple, Union

import gpuhunt

from dstack._internal.core.backends import (
    BACKENDS_WITH_CREATE_INSTANCE_SUPPORT,
    BACKENDS_WITH_MULTINODE_SUPPORT,
    BACKENDS_WITH_RESERVATION_SUPPORT,
)
from dstack._internal.core.backends.base import Backend
from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.instances import (
    InstanceOfferWithAvailability,
    InstanceSharedOffer,
    InstanceType,
    Resources,
)
from dstack._internal.core.models.profiles import Profile
from dstack._internal.core.models.runs import JobProvisioningData, Requirements
from dstack._internal.core.models.volumes import Volume
from dstack._internal.server.models import ProjectModel
from dstack._internal.server.services import backends as backends_services


async def get_offers_by_requirements(
    project: ProjectModel,
    profile: Profile,
    requirements: Requirements,
    exclude_not_available=False,
    multinode: bool = False,
    master_job_provisioning_data: Optional[JobProvisioningData] = None,
    volumes: Optional[List[List[Volume]]] = None,
    privileged: bool = False,
    instance_mounts: bool = False,
    blocks: Optional[Union[Literal["auto"], int]] = None,
) -> List[Tuple[Backend, InstanceOfferWithAvailability]]:
    backends: List[Backend] = await backends_services.get_project_backends(project=project)

    # For backward-compatibility to show offers if users set `backends: [dstack]`
    if (
        profile.backends is not None
        and len(profile.backends) == 1
        and BackendType.DSTACK in profile.backends
    ):
        profile.backends = None

    backend_types = profile.backends
    regions = profile.regions

    if volumes:
        mount_point_volumes = volumes[0]
        backend_types = [v.configuration.backend for v in mount_point_volumes]
        regions = [v.configuration.region for v in mount_point_volumes]

    if multinode:
        if not backend_types:
            backend_types = BACKENDS_WITH_MULTINODE_SUPPORT
        backend_types = [b for b in backend_types if b in BACKENDS_WITH_MULTINODE_SUPPORT]

    if privileged or instance_mounts:
        if not backend_types:
            backend_types = BACKENDS_WITH_CREATE_INSTANCE_SUPPORT
        backend_types = [b for b in backend_types if b in BACKENDS_WITH_CREATE_INSTANCE_SUPPORT]

    if profile.reservation is not None:
        if not backend_types:
            backend_types = BACKENDS_WITH_RESERVATION_SUPPORT
        backend_types = [b for b in backend_types if b in BACKENDS_WITH_RESERVATION_SUPPORT]

    # For multi-node, restrict backend and region.
    # The default behavior is to provision all nodes in the same backend and region.
    if master_job_provisioning_data is not None:
        if not backend_types:
            backend_types = [master_job_provisioning_data.get_base_backend()]
        if not regions:
            regions = [master_job_provisioning_data.region]
        backend_types = [
            b for b in backend_types if b == master_job_provisioning_data.get_base_backend()
        ]
        regions = [r for r in regions if r == master_job_provisioning_data.region]

    if backend_types is not None:
        backends = [b for b in backends if b.TYPE in backend_types or b.TYPE == BackendType.DSTACK]

    offers = await backends_services.get_instance_offers(
        backends=backends,
        requirements=requirements,
        exclude_not_available=exclude_not_available,
    )

    # Filter offers again for backends since a backend
    # can return offers of different backend types (e.g. BackendType.DSTACK).
    # The first filter should remain as an optimization.
    if backend_types is not None:
        offers = [(b, o) for b, o in offers if o.backend in backend_types]

    if regions is not None:
        offers = [(b, o) for b, o in offers if o.region in regions]

    if profile.instance_types is not None:
        offers = [(b, o) for b, o in offers if o.instance.name in profile.instance_types]

    if blocks is None or blocks == 1:
        return offers

    shareable_offers = []
    for backend, offer in offers:
        resources = offer.instance.resources
        gpus = resources.gpus
        if not gpus:
            continue
        if gpus[0].vendor == gpuhunt.AcceleratorVendor.GOOGLE:
            # TPU instances cannot be shared
            continue
        if blocks == "auto":
            divisor = len(gpus)
        else:
            divisor = blocks
        if len(gpus) % divisor or resources.cpus % divisor:
            # gpus or cpus are not divisible by blocks
            continue
        shareable_offers.append((backend, offer))
    return shareable_offers


def generate_shared_offer(offer: InstanceOfferWithAvailability, blocks: int, total_blocks: int):
    full_resources = offer.instance.resources
    resources = Resources(
        cpus=full_resources.cpus // total_blocks * blocks,
        memory_mib=full_resources.memory_mib // total_blocks * blocks,
        gpus=full_resources.gpus[: len(full_resources.gpus) // total_blocks * blocks],
        spot=full_resources.spot,
        disk=full_resources.disk,
        description=full_resources.description,
    )
    return InstanceSharedOffer(
        backend=offer.backend,
        instance=InstanceType(
            name=offer.instance.name,
            resources=resources,
        ),
        region=offer.region,
        price=offer.price,
        availability=offer.availability,
        blocks=blocks,
        total_blocks=total_blocks,
    )
