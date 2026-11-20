"""Load a realistic, end-to-end test scenario into the pipeline tables.

Generates source files in the exact formats ARC and JBA deliver and feeds them
through the real pipeline code (CSV parsing, admin-area resolution, aggregation,
COG conversion, run metadata), so the resulting rows look like a real ingestion.

Storyline, relative to the last ARC observation date L (= --end - 1):

  * L-8  a small, isolated storm over the far north   -> marginal trigger, REJECTED
  * L-5  a heavy storm over the Lower Shire (south)    -> trigger, CONFIRMED + SENT
  * L    a heavy storm over the central lakeshore      -> trigger, PENDING_REVIEW

JBA forecasts see each storm coming ~6 days ahead with growing ensemble
agreement, and keep forecasting flood impacts for a few days after it.
The pending trigger is left for testers to review in the app, which runs the
real notification task and sends real emails to the recipients.

Only data inside the chosen window is touched. Admin areas must already be
loaded (`./manage.py sync_geo`).
"""

import csv
import io
import math
import typing
import zipfile
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import transaction
from django.utils import timezone
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from apps.admin_areas.models import AdminArea
from apps.notifications.models import NotificationLog, NotificationRecipient
from apps.pipeline.extraction.arc.pipeline import ArcPipeline
from apps.pipeline.extraction.jba.pipeline import JBAPipeline
from apps.pipeline.management.date_range import parse_date
from apps.pipeline.models import (
    ArcIngestionRun,
    ArcRainfallObservation,
    ArcTriggerEvent,
    FloodForecastFile,
    FloodForecastImpact,
    IngestionStatus,
    JbaIngestionRun,
    TriggerEventStatus,
)
from apps.users.models import User, UserRole

# The real ARC grid: 974 cells at 0.1° covering Malawi (id, lon, lat taken from an ARC delivery).
ARC_GRID_CSV = Path(__file__).resolve().parents[4] / "dummy" / "NASA_grid_togglecorp_malawi_test1.csv"

ARC_FILENAME = "{d:%Y%m%d}_NASA_grid_togglecorp_2024s1553.csv"
JBA_ZIP_FILENAME = "for_mwi_ts_rd{d:%Y%m%d}T0000Z_population_impacts.csv.zip"
JBA_TIFF_FILENAME = "for_mwi_ts_fe{t:%Y%m%d}T0000Z_rd{i:%Y%m%d}T0000Z_ensAgreement_band05.tif"

# Times the scheduled jobs run (UTC), used to back-date run timestamps.
JBA_RUN_TIME = time(12, 45)
ARC_RUN_TIME = time(23, 0)

JBA_LEAD_DAYS = 11  # targets issue+0 .. issue+10, as JBA delivers
JBA_ENSEMBLES = 51
JBA_BANDS = ("band_1", "band_5", "band_11")
RASTER_RES_DEG = 0.005  # coarser than JBA's ~30 m so files stay small

# Daily rainfall (mm) thresholds for ARC return periods, highest first. Real deliveries have
# reached ~100 mm/day with event_rp 0, so the 2-year threshold sits just above that.
RP_THRESHOLDS = ((180.0, 20), (150.0, 10), (125.0, 5), (100.0, 2))
TRIGGER_RP = 5  # a cell triggers at a 5-year event or rarer
# People affected per cell for each return period (low, high).
RP_IMPACT = {2: (0, 50), 5: (50, 200), 10: (150, 400), 20: (300, 800)}

DEFAULT_RECIPIENTS = (
    # (email, name, organization, region pcode or None for national)
    ("north.dodma@example.com", "Northern Region Desk", "DoDMA", "MW1"),
    ("central.dodma@example.com", "Central Region Desk", "DoDMA", "MW2"),
    ("south.mrcs@example.com", "Southern Region Branch", "MRCS", "MW3"),
    ("national.ops@example.com", "National Operations", "MRCS", None),
)


@dataclass(frozen=True)
class Storm:
    name: str
    peak_offset: int  # days before the last observation date
    lat: float
    lon: float
    radius_deg: float  # spatial spread (1 sd) of the rain field
    peak_mm: float  # extra daily rainfall at the centre on the peak day
    duration_sd: float  # temporal spread (1 sd, days)


STORMS = (
    Storm("Northern lakeshore cells", 8, lat=-10.15, lon=34.05, radius_deg=0.15, peak_mm=150, duration_sd=0.6),
    Storm("Lower Shire storm", 5, lat=-16.45, lon=34.95, radius_deg=0.55, peak_mm=190, duration_sd=1.0),
    Storm("Central lakeshore storm", 0, lat=-13.70, lon=34.35, radius_deg=0.50, peak_mm=175, duration_sd=1.1),
)


@dataclass
class District:
    id: int
    pcode: str
    region_pcode: str | None
    lat: float
    lon: float
    bbox: tuple[float, float, float, float]  # west, south, east, north
    exposure: float  # people exposed to a full-strength flood


class Command(BaseCommand):
    help = (
        "Load a realistic ARC + JBA test scenario (rainfall, flood forecasts, trigger events, notifications) "
        "for the run dates ending at --end, generated through the real pipeline code."
    )

    @typing.override
    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--end",
            type=parse_date,
            default=None,
            help=(
                "Last run date of the scenario (YYYY-MM-DD, default today). JBA issues forecasts on each run date; "
                "ARC observes the day before. Running on today clashes with today's scheduled runs, so either "
                "load after they finish with --replace, or pause the cron jobs."
            ),
        )
        parser.add_argument("--days", type=int, default=14, help="Number of run dates to generate (default 14).")
        parser.add_argument(
            "--replace",
            action="store_true",
            help="Delete existing ARC/JBA/trigger data inside the window first (otherwise refuse if any exists).",
        )
        parser.add_argument(
            "--notify-email",
            action="append",
            default=[],
            help="Add a notification recipient scoped to every district (repeatable). Use your own address to "
            "receive the alert when you confirm the pending trigger.",
        )
        parser.add_argument("--seed", type=int, default=2026, help="Random seed (same seed → same data).")

    @typing.override
    def handle(self, *args: Any, **options: Any) -> None:
        if "PROD" in settings.APP_ENVIRONMENT:
            raise CommandError("Refusing to load test data in a production environment.")

        end: date = options["end"] or date.today()
        days: int = options["days"]
        if end > date.today():
            raise CommandError("--end cannot be in the future.")
        if days < 1:
            raise CommandError("--days must be at least 1.")

        self.rng = np.random.default_rng(options["seed"])
        self.now = timezone.now()
        run_dates = [end - timedelta(days=offset) for offset in range(days - 1, -1, -1)]
        obs_dates = [d - timedelta(days=1) for d in run_dates]
        self.last_obs = obs_dates[-1]

        self.districts = self._load_districts()
        self.grid = pl.read_csv(ARC_GRID_CSV, columns=["id", "lon", "lat"])

        with transaction.atomic():
            self._clear_window(run_dates, obs_dates, replace=options["replace"])
            for run_date, obs_date in zip(run_dates, obs_dates, strict=True):
                self._load_arc_run(run_date, obs_date)
                self._load_jba_run(run_date)
            recipients = self._ensure_recipients(options["notify_email"])
            self._create_trigger_events(obs_dates, recipients)

        self.stdout.write(
            self.style.SUCCESS(
                f"Loaded test scenario for run dates {run_dates[0]} → {run_dates[-1]} "
                f"(ARC observations {obs_dates[0]} → {obs_dates[-1]}).",
            ),
        )

    # ------------------------------------------------------------------
    # Setup
    # ------------------------------------------------------------------

    def _load_districts(self) -> list[District]:
        districts = []
        for area in AdminArea.objects.filter(level=2).select_related("parent").order_by("pcode"):
            if not area.bbox:
                continue
            ring = area.bbox["coordinates"][0]
            lons, lats = [c[0] for c in ring], [c[1] for c in ring]
            districts.append(
                District(
                    id=area.pk,
                    pcode=area.pcode,
                    region_pcode=area.parent.pcode if area.parent else None,
                    lat=(min(lats) + max(lats)) / 2,
                    lon=(min(lons) + max(lons)) / 2,
                    bbox=(min(lons), min(lats), max(lons), max(lats)),
                    exposure=float(self.rng.uniform(800, 6_000)),
                ),
            )
        if not districts:
            raise CommandError("No districts with boundaries found. Load admin areas first: ./manage.py sync_geo")
        return districts

    def _clear_window(self, run_dates: list[date], obs_dates: list[date], *, replace: bool) -> None:
        start, end = run_dates[0], run_dates[-1]
        obs_start, obs_end = obs_dates[0], obs_dates[-1]

        querysets = {
            "notification logs": NotificationLog.objects.filter(
                trigger_event__trigger_date__range=(obs_start, obs_end),
            ),
            "ARC trigger events": ArcTriggerEvent.objects.filter(trigger_date__range=(obs_start, obs_end)),
            "ARC observations": ArcRainfallObservation.objects.filter(observation_date__range=(obs_start, obs_end)),
            "ARC runs": ArcIngestionRun.objects.filter(run_date__range=(start, end)),
            "JBA impacts": FloodForecastImpact.objects.filter(forecast_issue_date__range=(start, end)),
            "JBA forecast files": FloodForecastFile.objects.filter(forecast_issue_date__range=(start, end)),
            "JBA runs": JbaIngestionRun.objects.filter(run_date__range=(start, end)),
        }
        existing = {label: qs.count() for label, qs in querysets.items()}
        existing = {label: n for label, n in existing.items() if n}
        if not existing:
            return

        summary = ", ".join(f"{n} {label}" for label, n in existing.items())
        if not replace:
            raise CommandError(f"The window {start} → {end} already has data ({summary}). Re-run with --replace.")

        # Order matters: notification logs PROTECT their trigger event.
        for qs in querysets.values():
            qs.delete()
        self.stdout.write(f"  Cleared existing data in window: {summary}")

    # ------------------------------------------------------------------
    # Scenario model
    # ------------------------------------------------------------------

    def _at(self, day: date, at: time, offset: timedelta = timedelta()) -> datetime:
        """Timestamp for a back-dated event, capped at now so nothing lands in the future."""
        return min(datetime.combine(day, at, tzinfo=UTC) + offset, self.now)

    def _storm_day(self, storm: Storm) -> date:
        return self.last_obs - timedelta(days=storm.peak_offset)

    @staticmethod
    def _spatial(storm: Storm, lat: np.ndarray | float, lon: np.ndarray | float, spread: float = 1.0) -> Any:
        dist2 = (np.asarray(lat) - storm.lat) ** 2 + (np.asarray(lon) - storm.lon) ** 2
        return np.exp(-dist2 / (2 * (storm.radius_deg * spread) ** 2))

    def _temporal(self, storm: Storm, day: date, lag: int = 0, stretch: float = 1.0) -> float:
        delta = (day - self._storm_day(storm)).days - lag
        return math.exp(-((delta / (storm.duration_sd * stretch)) ** 2) / 2)

    def _flood_intensity(self, district: District, target: date) -> float:
        """True flood severity (0-1) in a district on a day: rivers peak a day after the rain and recede slowly."""
        total = 0.0
        for storm in STORMS:
            total += (
                (storm.peak_mm / 190)
                * self._temporal(storm, target, lag=1, stretch=1.8)
                * float(self._spatial(storm, district.lat, district.lon, spread=1.4))
            )
        return min(total, 1.0) if total >= 0.03 else 0.0

    def _forecast_skill(self, issue: date) -> dict[Storm, float]:
        """How confidently a forecast issued on `issue` sees each storm (0 before ~6 days ahead)."""
        skill = {}
        for storm in STORMS:
            days_ahead = (self._storm_day(storm) - issue).days
            skill[storm] = 0.0 if days_ahead > 6 else float(np.clip(1 - days_ahead / 7, 0.25, 1.0))
        return skill

    # ------------------------------------------------------------------
    # ARC
    # ------------------------------------------------------------------

    def _arc_csv(self, obs_date: date) -> bytes:
        lat = self.grid["lat"].to_numpy()
        lon = self.grid["lon"].to_numpy()
        n = len(lat)

        # Background rainy-season rain: patchy, many light days, ~10% dry cells.
        wetness = self.rng.uniform(0.3, 1.2)
        rain = self.rng.gamma(0.7, 9 * wetness, n) * (self.rng.random(n) > 0.1)
        for storm in STORMS:
            rain += (
                storm.peak_mm
                * self._temporal(storm, obs_date)
                * self._spatial(storm, lat, lon)
                * self.rng.uniform(0.85, 1.15, n)
            )

        rows = []
        for i in range(n):
            rainfall = float(rain[i])
            rp = next((p for threshold, p in RP_THRESHOLDS if rainfall >= threshold), 0)
            raw = round(rainfall / self.rng.uniform(0.80, 0.95), 1) if rainfall > 0 else 0.0
            impact = float(round(self.rng.uniform(*RP_IMPACT[rp]), -1)) if rp else 0.0
            rows.append(
                [
                    int(self.grid["id"][i]),
                    lon[i],
                    lat[i],
                    obs_date.isoformat(),
                    raw,
                    rainfall,
                    impact,
                    rp,
                    "True" if rp >= TRIGGER_RP else "False",
                ],
            )

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(["id", "lon", "lat", "date", "rainfall_raw", "rainfall", "impact", "event_rp", "cell_trigger"])
        writer.writerows(rows)
        return buf.getvalue().encode()

    def _load_arc_run(self, run_date: date, obs_date: date) -> None:
        run = ArcIngestionRun.objects.create(run_date=run_date, status=IngestionStatus.RUNNING)
        raw = self._arc_csv(obs_date)
        key = f"malawi/{obs_date:%Y/%m/%d}/" + ARC_FILENAME.format(d=obs_date)

        written = ArcPipeline(arc_ingestion_run=run).run_from_csv(key, raw, obs_date)

        ArcIngestionRun.objects.filter(pk=run.pk).update(
            started_at=self._at(run_date, ARC_RUN_TIME),
            completed_at=self._at(run_date, ARC_RUN_TIME, timedelta(seconds=6)),
        )
        ArcRainfallObservation.objects.filter(ingestion_run=run).update(
            ingested_at=self._at(run_date, ARC_RUN_TIME, timedelta(seconds=5)),
        )
        triggered = ArcRainfallObservation.objects.filter(
            ingestion_run=run,
            cell_trigger=True,
            admin_area_id__in=[d.id for d in self.districts],
        ).count()
        self.stdout.write(f"  ARC  {obs_date}: {written} rows, {triggered} triggered district(s)")

    # ------------------------------------------------------------------
    # JBA
    # ------------------------------------------------------------------

    def _jba_members(self, issue: date) -> dict[tuple[str, date], np.ndarray]:
        """band_5 people-affected per ensemble member, for every (district, target date)."""
        skill = self._forecast_skill(issue)
        members = {}
        for district in self.districts:
            for lead in range(JBA_LEAD_DAYS):
                target = issue + timedelta(days=lead)
                values = np.zeros(JBA_ENSEMBLES)
                intensity = self._flood_intensity(district, target)
                if intensity:
                    # Agreement grows as the storm gets closer; spread grows with lead time.
                    confidence = max(
                        (
                            s
                            * float(self._spatial(storm, district.lat, district.lon, 1.4))
                            * self._temporal(storm, target, lag=1, stretch=1.8)
                            for storm, s in skill.items()
                        ),
                        default=0.0,
                    )
                    agreement = min(1.0, confidence * 2.5) if confidence else 0.0
                    hits = self.rng.random(JBA_ENSEMBLES) < agreement
                    spread = self.rng.lognormal(0, 0.08 + 0.04 * lead, JBA_ENSEMBLES)
                    values = np.where(hits, district.exposure * intensity * spread, 0.0)
                    values[values < 20] = 0.0
                members[(district.pcode, target)] = values
        return members

    def _jba_zip(self, issue: date, members: dict[tuple[str, date], np.ndarray]) -> bytes:
        targets = [issue + timedelta(days=lead) for lead in range(JBA_LEAD_DAYS)]
        band_scale = {"band_1": 1.25, "band_5": 1.0, "band_11": 0.55}  # shallower bands affect more people

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(["admin_code", "ensemble", *[band for band in JBA_BANDS for _ in targets]])
        writer.writerow(["", "", *[t.isoformat() for _ in JBA_BANDS for t in targets]])
        for district in self.districts:
            for member in range(JBA_ENSEMBLES):
                values = [
                    float(members[(district.pcode, t)][member] * band_scale[band]) for band in JBA_BANDS for t in targets
                ]
                writer.writerow([district.pcode, f"{member:02d}", *values])

        csv_name = JBA_ZIP_FILENAME.format(d=issue).removesuffix(".zip")
        out = io.BytesIO()
        with zipfile.ZipFile(out, "w", zipfile.ZIP_DEFLATED) as z:
            z.writestr(csv_name, buf.getvalue())
        return out.getvalue()

    def _jba_raster(self, target: date, members: dict[tuple[str, date], np.ndarray]) -> bytes:
        """Band-5 ensemble-agreement raster (uint8, 0-51 members flooding), cropped to the flooded area like JBA's."""
        flooded = [(d, int((members[(d.pcode, target)] > 0).sum())) for d in self.districts]
        flooded = [(d, n) for d, n in flooded if n]

        if flooded:
            west = min(d.bbox[0] for d, _ in flooded)
            south = min(d.bbox[1] for d, _ in flooded)
            east = max(d.bbox[2] for d, _ in flooded)
            north = max(d.bbox[3] for d, _ in flooded)
        else:  # JBA still delivers a tiny empty raster on dry days
            west, north = 34.30, -13.20
            east, south = west + 10 * RASTER_RES_DEG, north - 10 * RASTER_RES_DEG

        width = max(1, round((east - west) / RASTER_RES_DEG))
        height = max(1, round((north - south) / RASTER_RES_DEG))
        lon = west + (np.arange(width) + 0.5) * RASTER_RES_DEG
        lat = north - (np.arange(height) + 0.5) * RASTER_RES_DEG
        lon_grid, lat_grid = np.meshgrid(lon, lat)

        agreement = np.zeros((height, width))
        # Seed per target so the flood footprint is stable across issue dates.
        footprint_rng = np.random.default_rng(target.toordinal())
        for district, n_members in flooded:
            for _ in range(4):  # a few floodplain patches per district
                c_lon = footprint_rng.uniform(district.bbox[0], district.bbox[2])
                c_lat = footprint_rng.uniform(district.bbox[1], district.bbox[3])
                sx, sy = footprint_rng.uniform(0.01, 0.05, 2)
                patch = np.exp(-(((lon_grid - c_lon) / sx) ** 2 + ((lat_grid - c_lat) / sy) ** 2) / 2)
                agreement = np.maximum(agreement, patch * n_members)
        data = np.where(agreement >= 1, np.round(agreement), 0).astype("uint8")

        with MemoryFile() as mem:
            with mem.open(
                driver="GTiff",
                width=width,
                height=height,
                count=1,
                dtype="uint8",
                crs="EPSG:4326",
                transform=from_origin(west, north, RASTER_RES_DEG, RASTER_RES_DEG),
                nodata=0,
                compress="deflate",
            ) as ds:
                ds.write(data, 1)
            return mem.read()

    def _load_jba_run(self, issue: date) -> None:
        run = JbaIngestionRun.objects.create(run_date=issue, status=IngestionStatus.RUNNING)
        members = self._jba_members(issue)
        targets = [issue + timedelta(days=lead) for lead in range(JBA_LEAD_DAYS)]
        rasters = [(JBA_TIFF_FILENAME.format(t=t, i=issue), self._jba_raster(t, members)) for t in targets]
        impacts_zip = (JBA_ZIP_FILENAME.format(d=issue), self._jba_zip(issue, members))
        JBAPipeline(jba_ingestion_run=run).run_from_files(rasters, impacts_zip)

        JbaIngestionRun.objects.filter(pk=run.pk).update(
            started_at=self._at(issue, JBA_RUN_TIME),
            completed_at=self._at(issue, JBA_RUN_TIME, timedelta(minutes=4)),
        )
        FloodForecastFile.objects.filter(ingestion_run=run).update(
            created_at=self._at(issue, JBA_RUN_TIME, timedelta(minutes=3)),
        )
        FloodForecastImpact.objects.filter(forecast_issue_date=issue).update(
            created_at=self._at(issue, JBA_RUN_TIME, timedelta(minutes=4)),
        )
        impacts = FloodForecastImpact.objects.filter(forecast_issue_date=issue)
        self.stdout.write(
            f"  JBA  {issue}: {len(rasters)} rasters, {impacts.count()} impacts "
            f"({impacts.filter(band_5_max__gt=0).count()} non-zero)",
        )

    # ------------------------------------------------------------------
    # Triggers and notifications
    # ------------------------------------------------------------------

    def _ensure_recipients(self, notify_emails: list[str]) -> list[NotificationRecipient]:
        all_ids = [d.id for d in self.districts]
        specs = [
            (email, name, org, [d.id for d in self.districts if region is None or d.region_pcode == region])
            for email, name, org, region in DEFAULT_RECIPIENTS
        ]
        specs += [(email, None, "Test", all_ids) for email in notify_emails]

        recipients = []
        for email, name, org, area_ids in specs:
            recipient, created = NotificationRecipient.objects.get_or_create(
                email=email,
                defaults={"name": name, "organization": org, "admin_area_ids": area_ids, "is_active": True},
            )
            recipients.append(recipient)
            if created:
                self.stdout.write(f"  Recipient {email} ({len(area_ids)} districts)")
        return recipients

    def _reviewer(self) -> User:
        reviewer = User.objects.filter(role=UserRole.REVIEWER, is_active=True).order_by("pk").first()
        reviewer = reviewer or User.objects.filter(role=UserRole.ADMIN, is_active=True).order_by("pk").first()
        if reviewer:
            return reviewer
        reviewer = User.objects.create(email="reviewer@example.com", name="Test Reviewer", role=UserRole.REVIEWER)
        reviewer.set_unusable_password()
        reviewer.save(update_fields=["password"])
        return reviewer

    def _create_trigger_events(self, obs_dates: list[date], recipients: list[NotificationRecipient]) -> None:
        district_ids = {d.id for d in self.districts}
        triggered: dict[date, list[int]] = {}
        for obs_date in obs_dates:
            ids = sorted(
                ArcRainfallObservation.objects.filter(
                    observation_date=obs_date,
                    cell_trigger=True,
                    admin_area_id__in=district_ids,
                ).values_list("admin_area_id", flat=True),
            )
            if ids:
                triggered[obs_date] = ids

        if not triggered:
            self.stdout.write(self.style.WARNING("  No ARC triggers in this window; widen it with --days."))
            return

        latest = max(triggered)
        # The smallest earlier event is the one a reviewer would plausibly dismiss.
        earlier = [d for d in triggered if d != latest]
        rejected = min(earlier, key=lambda d: len(triggered[d])) if len(earlier) > 1 else None
        reviewer = self._reviewer()

        for trigger_date, area_ids in sorted(triggered.items()):
            run_date = trigger_date + timedelta(days=1)
            created_at = self._at(run_date, ARC_RUN_TIME, timedelta(minutes=1))
            reviewed_at = self._at(run_date, ARC_RUN_TIME, timedelta(hours=10))
            event = ArcTriggerEvent.objects.create(
                trigger_date=trigger_date,
                triggered_admin_areas_count=len(area_ids),
                affected_admin_areas=area_ids,
                status=TriggerEventStatus.PENDING_REVIEW,
            )
            if trigger_date == latest:
                status = TriggerEventStatus.PENDING_REVIEW
            elif trigger_date == rejected:
                status = TriggerEventStatus.REJECTED
                event.review_notes = "Isolated cells on the lakeshore; no corroborating reports or JBA flood signal."
            else:
                status = TriggerEventStatus.SENT
                event.review_notes = "Confirmed against JBA forecast and district reports."
                event.email_sent_at = self._at(run_date, ARC_RUN_TIME, timedelta(hours=10, minutes=1))
                for recipient in recipients:
                    if recipient.is_active and set(recipient.admin_area_ids) & set(area_ids):
                        log = NotificationLog.objects.create(
                            trigger_event=event,
                            recipient=recipient,
                            recipient_email=recipient.email,
                            status=NotificationLog.Status.SENT,
                        )
                        NotificationLog.objects.filter(pk=log.pk).update(sent_at=event.email_sent_at)

            if status != TriggerEventStatus.PENDING_REVIEW:
                event.reviewed_by = reviewer
                event.reviewed_at = reviewed_at
            event.status = status
            event.save()
            ArcTriggerEvent.objects.filter(pk=event.pk).update(created_at=created_at)
            self.stdout.write(f"  Trigger {trigger_date}: {len(area_ids)} district(s) → {status}")
