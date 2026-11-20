import typing
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from apps.pipeline.management.date_range import add_date_range_arguments, resolve_dates
from apps.pipeline.models import IngestionStatus, JbaIngestionRun
from apps.pipeline.tasks import launch_jba_pipeline


class Command(BaseCommand):
    help = (
        "Trigger the JBA pipeline. Without arguments, ingests today's forecast. "
        "Use --date for a single past day or --start/--end (inclusive) to backfill a range; "
        "one ingestion run is queued per day."
    )

    @typing.override
    def add_arguments(self, parser: CommandParser) -> None:
        add_date_range_arguments(parser)

    @typing.override
    def handle(self, *args: Any, **options: Any) -> None:
        dates = resolve_dates(options["date"], options["start"], options["end"])

        if dates is None:
            launch_jba_pipeline.delay()  # type: ignore[attr-defined]
            self.stdout.write(self.style.SUCCESS("Queued JBA extraction for today."))
            return

        # Re-ingesting a day would fail on the file/impact unique constraints, so skip those up front.
        already_ingested = set(
            JbaIngestionRun.objects.filter(
                run_date__in=dates,
                status=IngestionStatus.SUCCESS,
            ).values_list("run_date", flat=True),
        )

        queued = 0
        for day in dates:
            if day in already_ingested:
                self.stdout.write(f"  Skipped {day} (already has a successful run)")
                continue
            launch_jba_pipeline.delay(day.isoformat())  # type: ignore[attr-defined]
            self.stdout.write(f"  Queued {day}")
            queued += 1

        self.stdout.write(
            self.style.SUCCESS(f"Queued {queued} JBA run(s); skipped {len(already_ingested)} already ingested."),
        )
