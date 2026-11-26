"""Reference metadata from /zones and /station-registry."""

from __future__ import annotations

from typing import Any, Sequence
from uuid import UUID

from ..config import PROJECT_STATION
from .base import StaticExtractor
from .common import records, text


class ZonesExtractor(StaticExtractor):
    name = "zones"
    path = "/zones"
    table = "zone"
    columns = (
        "id", "name", "active", "account_id", "account_name",
        "lat_min", "lat_max", "lng_min", "lng_max",
    )
    conflict = """on conflict (id) do update set
        name = excluded.name, active = excluded.active,
        account_id = excluded.account_id, account_name = excluded.account_name,
        lat_min = excluded.lat_min, lat_max = excluded.lat_max,
        lng_min = excluded.lng_min, lng_max = excluded.lng_max,
        ingested_at = now()"""

    def to_rows(self, payload: Any) -> list[Sequence[Any]]:
        rows = []
        for record in records(payload):
            account = record.get("account") or {}
            if not isinstance(account, dict):
                raise ValueError("Zone account must be an object")
            rows.append((
                str(UUID(record["id"])), text(record.get("name"), "zone name"),
                record.get("active"), account.get("id"), account.get("name"),
                record.get("latMin"), record.get("latMax"),
                record.get("lngMin"), record.get("lngMax"),
            ))
        return rows


class StationsExtractor(StaticExtractor):
    """Retain only ACRAS2-8 from the service's full station registry."""

    name = "stations"
    path = "/station-registry"
    table = "station"
    columns = ("id", "name", "latitude", "longitude", "altitude", "nb_sensors", "sample_rate")
    conflict = """on conflict (id) do update set
        name = excluded.name, latitude = excluded.latitude,
        longitude = excluded.longitude, altitude = excluded.altitude,
        nb_sensors = excluded.nb_sensors, sample_rate = excluded.sample_rate,
        ingested_at = now()"""

    def to_rows(self, payload: Any) -> list[Sequence[Any]]:
        rows = []
        for record in records(payload):
            if record.get("name") != PROJECT_STATION:
                continue
            station_id = record.get("id")
            if isinstance(station_id, bool) or not isinstance(station_id, int):
                raise ValueError("Station id must be an integer")
            rows.append((
                station_id, text(record.get("name"), "station name"),
                record.get("latitude"), record.get("longitude"), record.get("altitude"),
                record.get("nbSensors"), record.get("sampleRate"),
            ))
        if len(rows) != 1:
            raise ValueError(f"Expected exactly one {PROJECT_STATION} station in the registry")
        return rows
