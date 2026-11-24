"""Resolve geographic points to admin areas.

Reverse-geocoding: given a list of (lat, lon) points, resolve each to
the AdminArea PK whose PostGIS geometry contains the point.

Strategy
--------
Build a single VALUES-based SQL query so we hit the DB in one round-trip
per pipeline run.  The deepest matching admin level (≤ target_level) wins.
Points that fall outside all known boundaries resolve to None and are
silently dropped by the transform phase.
"""

import logging

from django.db import connection

logger = logging.getLogger(__name__)


def resolve_admin_areas(
    coords: list[tuple[float, float]],
    *,
    target_level: int = 3,
) -> dict[tuple[float, float], list[int]]:
    """Resolve a list of (lat, lon) tuples to their deepest matching AdminArea PK.

    A point sits inside the nested boundaries of its country, region and
    district at once, so only the deepest (most specific) matching level
    ``<= target_level`` is kept via ``DISTINCT ON``. Returning every match
    instead would duplicate each observation onto its parent admin areas.

    Returns
    -------
    dict mapping (lat, lon) → a list with the single deepest AdminArea PK,
    or an empty list when the point falls outside every known boundary.

    """
    if not coords:
        return {}

    values_sql = ", ".join(f"({lat!r}, {lon!r}, {i})" for i, (lat, lon) in enumerate(coords))

    # DISTINCT ON (i.idx) with the level-descending order keeps one row per input
    # point: the deepest admin area that contains it. aa.id breaks ties so the
    # result is deterministic when two same-level boundaries overlap.
    sql = f"""
        SELECT DISTINCT ON (i.idx) i.lat, i.lon, aa.id AS admin_id
        FROM (VALUES {values_sql}) AS i(lat, lon, idx)
        JOIN admin_areas_adminarea aa
          ON ST_Contains(
                 ST_SetSRID(ST_GeomFromGeoJSON(aa.bbox::text), 4326),
                 ST_SetSRID(ST_MakePoint(i.lon, i.lat), 4326)
             )
        WHERE aa.level <= {target_level}
        ORDER BY i.idx, aa.level DESC, aa.id
    """

    result: dict[tuple[float, float], list[int]] = {c: [] for c in coords}

    with connection.cursor() as cur:
        cur.execute(sql)
        for lat, lon, admin_id in cur.fetchall():
            result[(float(lat), float(lon))].append(admin_id)

    resolved = sum(1 for v in result.values() if v)
    logger.info(
        "Resolved %d/%d coordinate pairs to their deepest admin area (level <= %d).",
        resolved,
        len(coords),
        target_level,
    )
    return result
