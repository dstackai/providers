from typing import List

from rich.table import Table

from dstack._internal.cli.utils.common import console
from dstack._internal.core.models.backends.base import BackendType
from dstack._internal.core.models.fleets import Fleet, FleetStatus
from dstack._internal.core.models.instances import InstanceStatus
from dstack._internal.utils.common import DateFormatter, pretty_date


def print_fleets_table(fleets: List[Fleet], verbose: bool = False) -> None:
    console.print(get_fleets_table(fleets, verbose=verbose))
    console.print()


def get_fleets_table(
    fleets: List[Fleet], verbose: bool = False, format_date: DateFormatter = pretty_date
) -> Table:
    table = Table(box=None)
    table.add_column("FLEET", no_wrap=True)
    if verbose:
        table.add_column("RESERVATION")
    table.add_column("INSTANCE")
    table.add_column("BACKEND")
    table.add_column("RESOURCES")
    table.add_column("PRICE")
    table.add_column("STATUS")
    table.add_column("CREATED")

    if verbose:
        table.add_column("ERROR")

    for fleet in fleets:
        for i, instance in enumerate(fleet.instances):
            resources = ""
            if instance.instance_type is not None and (
                instance.backend != BackendType.REMOTE
                or instance.status not in [InstanceStatus.PENDING, InstanceStatus.PROVISIONING]
            ):
                resources = instance.instance_type.resources.pretty_format(include_spot=True)

            status = instance.status.value
            total_blocks = instance.total_blocks
            busy_blocks = instance.busy_blocks
            if total_blocks is not None and busy_blocks is not None:
                if busy_blocks < total_blocks and instance.status == InstanceStatus.BUSY:
                    idle_blocks = total_blocks - busy_blocks
                    status = f"{idle_blocks}/{total_blocks} {InstanceStatus.IDLE.value}"
            if (
                instance.status in [InstanceStatus.IDLE, InstanceStatus.BUSY]
                and instance.unreachable
            ):
                status += "\n(unreachable)"

            backend = instance.backend or ""
            if backend == "remote":
                backend = "ssh"
            if instance.region:
                backend += f" ({instance.region})"

            row = [
                fleet.name if i == 0 else "",
            ]
            if verbose:
                row.append(fleet.spec.configuration.reservation or "" if i == 0 else "")
            row += [
                str(instance.instance_num),
                backend,
                resources,
                f"${instance.price:.4}" if instance.price is not None else "",
                status,
                format_date(instance.created),
            ]
            if verbose:
                error = ""
                if instance.status == InstanceStatus.TERMINATED and instance.termination_reason:
                    error = f"{instance.termination_reason}"
                row.append(error)

            table.add_row(*row)

        if len(fleet.instances) == 0 and fleet.status != FleetStatus.TERMINATING:
            row = [
                fleet.name,
                "-",
                "-",
                "-",
                "-",
                "-",
                format_date(fleet.created_at),
            ]
            table.add_row(*row)

    return table
