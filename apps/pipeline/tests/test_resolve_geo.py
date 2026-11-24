"""Tests for coordinate → admin area resolution."""

import pytest

from apps.admin_areas.models import AdminArea
from apps.pipeline.resolve_geo import resolve_admin_areas


def _square(lon: float, lat: float, half: float) -> dict:
    """A GeoJSON polygon square centred on (lon, lat)."""
    return {
        "type": "Polygon",
        "coordinates": [
            [
                [lon - half, lat - half],
                [lon + half, lat - half],
                [lon + half, lat + half],
                [lon - half, lat + half],
                [lon - half, lat - half],
            ],
        ],
    }


@pytest.mark.django_db
def test_resolves_to_deepest_level_only():
    """A point inside nested country/region/district boxes resolves to the district only."""
    lon, lat = 34.0, -13.0
    country = AdminArea.objects.create(
        pcode="MW",
        name="Malawi",
        level=0,
        bbox=_square(lon, lat, 5.0),
    )
    region = AdminArea.objects.create(
        pcode="MW2",
        name="Central Region",
        level=1,
        parent=country,
        bbox=_square(lon, lat, 2.0),
    )
    district = AdminArea.objects.create(
        pcode="MW204",
        name="Lilongwe",
        level=2,
        parent=region,
        bbox=_square(lon, lat, 0.5),
    )

    result = resolve_admin_areas([(lat, lon)])

    # The point sits inside all three nested boxes, but only the deepest wins —
    # the parents must never receive their own observation.
    assert result == {(lat, lon): [district.id]}


@pytest.mark.django_db
def test_point_outside_every_boundary_resolves_to_empty():
    AdminArea.objects.create(
        pcode="MW",
        name="Malawi",
        level=0,
        bbox=_square(34.0, -13.0, 1.0),
    )
    result = resolve_admin_areas([(10.0, 10.0)])
    assert result == {(10.0, 10.0): []}
