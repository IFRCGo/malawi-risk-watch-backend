"""Shared `--date` / `--start` / `--end` handling for the pipeline backfill commands."""

from datetime import date, timedelta

from django.core.management.base import CommandError, CommandParser

MAX_RANGE_DAYS = 366


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise CommandError(f"Invalid date {value!r}, expected YYYY-MM-DD.") from exc


def add_date_range_arguments(parser: CommandParser) -> None:
    parser.add_argument("--date", type=parse_date, help="Single run date to ingest (YYYY-MM-DD).")
    parser.add_argument("--start", type=parse_date, help="First run date of the range to ingest (YYYY-MM-DD).")
    parser.add_argument("--end", type=parse_date, help="Last run date of the range to ingest, inclusive (YYYY-MM-DD).")


def resolve_dates(single: date | None, start: date | None, end: date | None) -> list[date] | None:
    """Return the run dates to ingest, or None when no date option was given (i.e. "today")."""
    if single and (start or end):
        raise CommandError("Use either --date or --start/--end, not both.")
    if single:
        start = end = single
    elif start or end:
        if not (start and end):
            raise CommandError("--start and --end must be given together.")
    else:
        return None

    if start > end:
        raise CommandError("--start must be on or before --end.")
    if end > date.today():
        raise CommandError("Cannot ingest future dates.")
    days = (end - start).days + 1
    if days > MAX_RANGE_DAYS:
        raise CommandError(f"Range is {days} days; split it into chunks of at most {MAX_RANGE_DAYS} days.")

    return [start + timedelta(days=i) for i in range(days)]
