"""Weather observations, retaining spatial grid points and source units."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Sequence
from uuid import UUID

from psycopg.types.json import Jsonb

from .base import WindowedExtractor
from .common import records, timestamp, window_params


class MeteoExtractor(WindowedExtractor):
    name = "meteo"
    path = "/meteo-data/historic"
    table = "meteo_data"
    columns = (
        "id", "kind", "zone_id", "station_id", "date_time", "latitude", "longitude",
        "success", "http_status_code", "content",
    )
    conflict = "on conflict (id) do nothing"

    @property
    def window(self) -> timedelta:
        return self.config.meteo_window

    def params(self, scope: str, start: datetime, end: datetime) -> dict[str, Any]:
        return window_params(
            str(UUID(scope)), start, end, selector="zoneId", maximum=timedelta(days=1)
        )

    def to_rows(
        self, scope: str, payload: Any, start: datetime, end: datetime
    ) -> list[Sequence[Any]]:
        zone = UUID(scope)
        rows = []
        for record in records(payload):
            moment = timestamp(record.get("dateTime"))
            if not start <= moment < end:
                continue
            if record.get("zoneId") is not None and UUID(record["zoneId"]) != zone:
                raise ValueError("Weather response contains an unexpected zoneId")
            content = record.get("content")
            if not isinstance(content, dict):
                raise ValueError("Weather content must be a JSON object")
            rows.append((
                str(UUID(record["id"])), record.get("kind"), str(zone), record.get("stationId"),
                moment, record.get("latitude"), record.get("longitude"),
                record.get("success"), record.get("httpStatusCode"), Jsonb(content),
            ))
        return rows
