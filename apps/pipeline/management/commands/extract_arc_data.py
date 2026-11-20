import typing
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from apps.pipeline.management.date_range import add_date_range_arguments, resolve_dates
from apps.pipeline.models import ArcIngestionRun, IngestionStatus
from apps.pipeline.tasks import launch_arc_pipeline, run_arc_pipeline


class Command(BaseCommand):
    help = (
        "Trigger the ARC pipeline. Without arguments, ingests yesterday's observations (run date today). "
        "Use --date for a single past run date or --start/--end (inclusive) to backfill a range; "
        "each run date ingests the observations of the day before it. One ingestion run is queued per day."
    )

    @typing.override
    def add_arguments(self, parser: CommandParser) -> None:
        add_date_range_arguments(parser)

    @typing.override
    def handle(self, *args: Any, **options: Any) -> None:
        dates = resolve_dates(options["date"], options["start"], options["end"])

        if dates is None:
            launch_arc_pipeline.delay()  # type: ignore[attr-defined]
            self.stdout.write(self.style.SUCCESS("Queued ARC extraction for today."))
            return

        # run_date is unique on ArcIngestionRun, so a day can only ever have one run row.
        existing = {run.run_date: run for run in ArcIngestionRun.objects.filter(run_date__in=dates)}

        queued = retried = skipped = 0
        for day in dates:
            run = existing.get(day)
            if run is None:
                launch_arc_pipeline.delay(day.isoformat())  # type: ignore[attr-defined]
                self.stdout.write(f"  Queued {day}")
                queued += 1
            elif run.status == IngestionStatus.FAILED:
                # Failed runs roll back their observations, so the existing row can simply be re-run.
                run.set_status(IngestionStatus.PENDING)
                run_arc_pipeline.delay(run.pk)  # type: ignore[attr-defined]
                self.stdout.write(f"  Re-queued {day} (previous run failed)")
                retried += 1
            else:
                self.stdout.write(f"  Skipped {day} (existing run is {run.status})")
                skipped += 1

        self.stdout.write(
            self.style.SUCCESS(f"Queued {queued} new ARC run(s), re-queued {retried} failed, skipped {skipped}."),
        )
