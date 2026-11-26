"""Every zone-based run must use a zone covering the project station."""

from types import SimpleNamespace

import pytest

from acras.config import PROJECT_STATION
from scripts.download_data import verify_scopes


class Cursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.params = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def execute(self, query, params):
        self.params.append(params)

    def fetchone(self):
        return next(self.rows)


def test_zone_verification_uses_acras2_coordinates():
    cur = Cursor([
        {"latitude": 1, "longitude": 2},
        {"lat_min": 0, "lat_max": 2, "lng_min": 1, "lng_max": 3},
    ])
    verify_scopes(SimpleNamespace(cursor=lambda: cur), ["synthetic-zone"])
    assert cur.params[0] == (PROJECT_STATION,)


def test_every_selected_zone_must_contain_acras2():
    cur = Cursor([
        {"latitude": 1, "longitude": 2},
        {"lat_min": 0, "lat_max": 2, "lng_min": 1, "lng_max": 3},
        {"lat_min": 5, "lat_max": 6, "lng_min": 5, "lng_max": 6},
    ])
    with pytest.raises(ValueError, match="does not contain ACRAS2-8"):
        verify_scopes(SimpleNamespace(cursor=lambda: cur), ["inside-zone", "outside-zone"])


def test_missing_station_coordinates_prevent_unverified_zone_downloads():
    cur = Cursor([None])
    with pytest.raises(ValueError, match="ACRAS2-8 coordinates are missing"):
        verify_scopes(SimpleNamespace(cursor=lambda: cur), ["synthetic-zone"])
