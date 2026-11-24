"""Processed acoustic frames and immutable hourly Parquet manifests."""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
import hashlib
import math
from pathlib import Path
from typing import Any, Sequence

from ..config import require_project_station
from ..storage.parquet import ParquetStore
from .base import Stats, WindowedExtractor
from .common import timestamp, utc_hour, window_params


SERIES = (
    "lfSpectrogram", "lfAzimuth", "lfElevation",
    "vlfSpectrogram", "vlfAzimuth", "vlfElevation",
)


class SonicExtractor(WindowedExtractor):
    name = "sonic"
    path = "/sonic-data/historic"
    table = "sonic_frame_file"

    @property
    def window(self) -> timedelta:
        return self.config.sonic_window

    def run(
        self, scopes: Sequence[str], start: datetime, end: datetime, *, resume: bool = True
    ) -> Stats:
        for scope in scopes:
            require_project_station(scope)
        return super().run(scopes, start, end, resume=resume)

    def params(self, scope: str, start: datetime, end: datetime) -> dict[str, Any]:
        require_project_station(scope)
        return window_params(
            scope, start, end, selector="stationName", maximum=timedelta(hours=1)
        )

    def to_rows(
        self, scope: str, payload: Any, start: datetime, end: datetime
    ) -> list[Sequence[Any]]:
        require_project_station(scope)
        if not isinstance(payload, dict) or set(payload) != set(SERIES):
            raise ValueError("Expected the six documented acoustic series")
        rows = []
        for series in SERIES:
            frames = payload[series]
            if not isinstance(frames, list):
                raise ValueError(f"{series} must be a frame array")
            for frame in frames:
                if not isinstance(frame, dict):
                    raise ValueError("Acoustic frame must be an object")
                raw_time = frame.get("startTime")
                if isinstance(raw_time, bool) or not isinstance(raw_time, int):
                    raise ValueError("Acoustic startTime must be epoch milliseconds")
                moment = timestamp(raw_time)
                if not start <= moment < end:
                    continue
                values = frame.get("values")
                if not isinstance(values, list) or not values or any(
                    isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                    for v in values
                ):
                    raise ValueError("Acoustic values must be a nonempty array of finite numbers")
                rows.append((scope, series, moment, values))
        return rows

    def _write(self, rows: Sequence[Sequence[Any]]) -> int:
        groups = defaultdict(list)
        for station, series, moment, values in rows:
            require_project_station(station)
            groups[(station, series, utc_hour(moment))].append({"start_time": moment, "values": values})
        store = ParquetStore(self.config.data_dir)
        added = 0
        # A transaction-scoped advisory lock serializes each hour's read/merge/
        # manifest update, including across different downloader processes.
        for (station, series, hour), frames in sorted(groups.items()):
            lock_name = f"acras:sonic:{station}:{series}:{hour.isoformat()}"
            lock_id = int.from_bytes(hashlib.sha256(lock_name.encode()).digest()[:8], "big", signed=True)
            with self.conn.cursor() as cur:
                cur.execute("select pg_advisory_xact_lock(%s)", (lock_id,))
                cur.execute(
                    """select path from sonic_frame_file
                         where station_name = %s and series = %s and hour_start = %s""",
                    (station, series, hour),
                )
                old = cur.fetchone()
                stored = store.merge_hour(
                    station=station, series=series, hour_start=hour, frames=frames,
                    previous_path=Path(old["path"]) if old else None,
                )
                cur.execute(
                    """insert into sonic_frame_file
                         (station_name, series, hour_start, path, n_frames, values_per_frame, bytes)
                       values (%s, %s, %s, %s, %s, %s, %s)
                       on conflict (station_name, series, hour_start) do update set
                         path = excluded.path, n_frames = excluded.n_frames,
                         values_per_frame = excluded.values_per_frame,
                         bytes = excluded.bytes, written_at = now()""",
                    (station, series, hour, stored.path.as_posix(), stored.n_frames,
                     stored.values_per_frame, stored.bytes),
                )
                added += stored.frames_added
        return added
