"""Historical flight observations. Aircraft presence is not an acoustic label."""

from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
import json
from typing import Any, Sequence
from uuid import UUID

from .. import db
from .base import WindowedExtractor
from .common import records, text, timestamp, window_params


# Ordered mapping to the existing schema. Expiring photo URLs are omitted.
FIELDS = (
    ("icao24", "icao24"), ("date_time", "dateTime"),
    ("source", "source"), ("source_id", "sourceId"), ("callsign", "callsign"),
    ("position_date_time", "positionDateTime"),
    ("last_contact_date_time", "lastContactDateTime"),
    ("latitude", "latitude"), ("longitude", "longitude"),
    ("baro_altitude_m", "baroAltitudeM"), ("geo_altitude_m", "geoAltitudeM"),
    ("ground_speed_ms", "groundSpeedMs"), ("true_track_deg", "trueTrackDeg"),
    ("vertical_rate_ms", "verticalRateMs"), ("on_ground", "onGround"),
    ("squawk", "squawk"), ("spi", "spi"), ("position_source", "positionSource"),
    ("category", "category"), ("registration", "registration"),
    ("icao_type_code", "icaoTypeCode"),
)


class FlightExtractor(WindowedExtractor):
    name = "flight"
    path = "/flight-data/historic"
    table = "flight_data"
    columns = ("row_hash", "zone_id", *(column for column, _ in FIELDS))
    conflict = "on conflict (row_hash) do nothing"

    @property
    def window(self) -> timedelta:
        return self.config.flight_window

    def params(self, scope: str, start: datetime, end: datetime) -> dict[str, Any]:
        return window_params(
            str(UUID(scope)), start, end, selector="zoneId", maximum=timedelta(hours=1)
        )

    def to_rows(
        self, scope: str, payload: Any, start: datetime, end: datetime
    ) -> list[Sequence[Any]]:
        zone = str(UUID(scope))
        rows = []
        for record in records(payload):
            moment = timestamp(record.get("dateTime"))
            if not start <= moment < end:
                continue
            text(record.get("icao24"), "icao24")
            values = []
            for column, key in FIELDS:
                value = record.get(key)
                if column == "callsign" and value is None:
                    value = record.get("callSign")
                if column.endswith("date_time") and value is not None:
                    value = timestamp(value)
                values.append(value)
            # Hash persisted fields plus scope. Changing photo URLs do not
            # create new observations. Neither aircraft flags nor labels enter.
            canonical = [zone, *[
                value.isoformat() if isinstance(value, datetime) else value for value in values
            ]]
            digest = hashlib.sha256(json.dumps(
                canonical, separators=(",", ":"), allow_nan=False
            ).encode()).digest()
            rows.append((digest, zone, *values))
        return rows

    def _write(self, rows: Sequence[Sequence[Any]]) -> int:
        written = super()._write(rows)
        metadata = []
        for row in rows:
            data = dict(zip(self.columns, row))
            metadata.append((
                data["icao24"], data["registration"], data["icao_type_code"],
                data["category"], data["date_time"], data["date_time"],
            ))
        db.insert_many(
            self.conn, "aircraft",
            ("icao24", "registration", "icao_type_code", "best_category", "first_seen", "last_seen"),
            metadata,
            conflict="""on conflict (icao24) do update set
                registration = coalesce(excluded.registration, aircraft.registration),
                icao_type_code = coalesce(excluded.icao_type_code, aircraft.icao_type_code),
                best_category = coalesce(excluded.best_category, aircraft.best_category),
                first_seen = least(aircraft.first_seen, excluded.first_seen),
                last_seen = greatest(aircraft.last_seen, excluded.last_seen),
                updated_at = now()""",
        )
        return written
