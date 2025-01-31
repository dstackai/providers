from typing import Any, Dict, List, Union

from rich.markup import escape
from rich.table import Table

from dstack._internal.cli.utils.common import NO_OFFERS_WARNING, add_row_from_dict, console
from dstack._internal.core.models.instances import InstanceAvailability, InstanceSharedOffer
from dstack._internal.core.models.profiles import (
    DEFAULT_RUN_TERMINATION_IDLE_TIME,
    TerminationPolicy,
)
from dstack._internal.core.models.runs import (
    Job,
    RunPlan,
)
from dstack._internal.core.services.profiles import get_termination
from dstack._internal.utils.common import DateFormatter, format_pretty_duration, pretty_date
from dstack.api import Run


def print_run_plan(run_plan: RunPlan, offers_limit: int = 3):
    job_plan = run_plan.job_plans[0]

    props = Table(box=None, show_header=False)
    props.add_column(no_wrap=True)  # key
    props.add_column()  # value

    req = job_plan.job_spec.requirements
    pretty_req = req.pretty_format(resources_only=True)
    max_price = f"${req.max_price:g}" if req.max_price else "-"
    max_duration = (
        format_pretty_duration(job_plan.job_spec.max_duration)
        if job_plan.job_spec.max_duration
        else "-"
    )
    if job_plan.job_spec.retry is None:
        retry = "-"
    else:
        retry = escape(job_plan.job_spec.retry.pretty_format())

    profile = run_plan.run_spec.merged_profile
    creation_policy = profile.creation_policy
    # FIXME: This assumes the default idle_duration is the same for client and server.
    # If the server changes idle_duration, old clients will see incorrect value.
    termination_policy, termination_idle_time = get_termination(
        profile, DEFAULT_RUN_TERMINATION_IDLE_TIME
    )
    if termination_policy == TerminationPolicy.DONT_DESTROY:
        idle_duration = "-"
    else:
        idle_duration = format_pretty_duration(termination_idle_time)

    if req.spot is None:
        spot_policy = "auto"
    elif req.spot:
        spot_policy = "spot"
    else:
        spot_policy = "on-demand"

    def th(s: str) -> str:
        return f"[bold]{s}[/bold]"

    props.add_row(th("Project"), run_plan.project_name)
    props.add_row(th("User"), run_plan.user)
    props.add_row(th("Configuration"), run_plan.run_spec.configuration_path)
    props.add_row(th("Type"), run_plan.run_spec.configuration.type)
    props.add_row(th("Resources"), pretty_req)
    props.add_row(th("Max price"), max_price)
    props.add_row(th("Max duration"), max_duration)
    props.add_row(th("Spot policy"), spot_policy)
    props.add_row(th("Retry policy"), retry)
    props.add_row(th("Creation policy"), creation_policy)
    props.add_row(th("Idle duration"), idle_duration)
    props.add_row(th("Reservation"), run_plan.run_spec.configuration.reservation or "-")

    offers = Table(box=None)
    offers.add_column("#")
    offers.add_column("BACKEND")
    offers.add_column("REGION")
    offers.add_column("INSTANCE")
    offers.add_column("RESOURCES")
    offers.add_column("SPOT")
    offers.add_column("PRICE")
    offers.add_column()

    job_plan.offers = job_plan.offers[:offers_limit]

    for i, offer in enumerate(job_plan.offers, start=1):
        r = offer.instance.resources

        availability = ""
        if offer.availability in {
            InstanceAvailability.NOT_AVAILABLE,
            InstanceAvailability.NO_QUOTA,
            InstanceAvailability.IDLE,
            InstanceAvailability.BUSY,
        }:
            availability = offer.availability.value.replace("_", " ").lower()
        instance = offer.instance.name
        if isinstance(offer, InstanceSharedOffer):
            instance += f" ({offer.blocks}/{offer.total_blocks})"
        offers.add_row(
            f"{i}",
            offer.backend.replace("remote", "ssh"),
            offer.region,
            instance,
            r.pretty_format(),
            "yes" if r.spot else "no",
            f"${offer.price:g}",
            availability,
            style=None if i == 1 else "secondary",
        )
    if job_plan.total_offers > len(job_plan.offers):
        offers.add_row("", "...", style="secondary")

    console.print(props)
    console.print()
    if len(job_plan.offers) > 0:
        console.print(offers)
        if job_plan.total_offers > len(job_plan.offers):
            console.print(
                f"[secondary] Shown {len(job_plan.offers)} of {job_plan.total_offers} offers, "
                f"${job_plan.max_price:g} max[/]"
            )
        console.print()
    else:
        console.print(NO_OFFERS_WARNING)


def get_runs_table(
    runs: List[Run], verbose: bool = False, format_date: DateFormatter = pretty_date
) -> Table:
    table = Table(box=None)
    table.add_column("NAME", style="bold", no_wrap=True)
    table.add_column("BACKEND", style="grey58")
    if verbose:
        table.add_column("INSTANCE", no_wrap=True)
    table.add_column("RESOURCES")
    if verbose:
        table.add_column("RESERVATION", no_wrap=True)
    table.add_column("PRICE", no_wrap=True)
    table.add_column("STATUS", no_wrap=True)
    table.add_column("SUBMITTED", style="grey58", no_wrap=True)
    if verbose:
        table.add_column("ERROR", no_wrap=True)

    for run in runs:
        run_error = _get_run_error(run)
        run = run._run  # TODO(egor-s): make public attribute

        run_row: Dict[Union[str, int], Any] = {
            "NAME": run.run_spec.run_name,
            "SUBMITTED": format_date(run.submitted_at),
            "ERROR": run_error,
        }
        if len(run.jobs) != 1:
            run_row["STATUS"] = run.status
            add_row_from_dict(table, run_row)

        for job in run.jobs:
            job_row: Dict[Union[str, int], Any] = {
                "NAME": f"  replica={job.job_spec.replica_num} job={job.job_spec.job_num}",
                "STATUS": job.job_submissions[-1].status,
                "SUBMITTED": format_date(job.job_submissions[-1].submitted_at),
                "ERROR": _get_job_error(job),
            }
            latest_job_submission = job.job_submissions[-1]
            jpd = latest_job_submission.job_provisioning_data
            if jpd is not None:
                resources = jpd.instance_type.resources
                instance = jpd.instance_type.name
                jrd = latest_job_submission.job_runtime_data
                if jrd is not None and jrd.offer is not None:
                    resources = jrd.offer.instance.resources
                    instance += f" ({jrd.offer.blocks}/{jrd.offer.total_blocks})"
                job_row.update(
                    {
                        "BACKEND": f"{jpd.backend.value.replace('remote', 'ssh')} ({jpd.region})",
                        "INSTANCE": instance,
                        "RESOURCES": resources.pretty_format(include_spot=True),
                        "RESERVATION": jpd.reservation,
                        "PRICE": f"${jpd.price:.4}",
                    }
                )
            if len(run.jobs) == 1:
                # merge rows
                job_row.update(run_row)
            add_row_from_dict(table, job_row, style="secondary" if len(run.jobs) != 1 else None)

    return table


def _get_run_error(run: Run) -> str:
    return run._run.error or ""


def _get_job_error(job: Job) -> str:
    if job.job_submissions[-1].termination_reason is None:
        return ""
    return job.job_submissions[-1].termination_reason.name
