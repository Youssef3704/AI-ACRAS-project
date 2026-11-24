"""Immutable hourly acoustic files, referenced by PostgreSQL manifests.

Every merged version has a content-based filename. A database rollback leaves
the previous manifest and its file intact; an unreferenced new file is harmless.
Do not delete old versions while ingestion or readers are active.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import math
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping, Sequence
from urllib.parse import quote


@dataclass(frozen=True)
class StoredHour:
    path: Path  # Relative to ACRAS_DATA_DIR.
    n_frames: int
    values_per_frame: int
    bytes: int
    frames_added: int


class ParquetStore:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir).resolve()

    def _resolve(self, relative: Path) -> Path:
        if relative.is_absolute():
            raise ValueError("Acoustic file paths must be relative")
        result = (self.data_dir / relative).resolve()
        if not result.is_relative_to(self.data_dir):
            raise ValueError("Acoustic file path escapes the data directory")
        return result

    def merge_hour(
        self,
        *,
        station: str,
        series: str,
        hour_start: datetime,
        frames: Sequence[Mapping[str, Any]],
        previous_path: Path | None = None,
    ) -> StoredHour:
        """Merge normalized frames while the caller holds the database hour lock.

        Frames contain a timezone-aware start_time and a numeric values array.
        Existing timestamps must have identical values; revisions require an
        explicit policy rather than silently replacing earlier measurements.
        """
        import pyarrow as pa
        import pyarrow.parquet as pq

        if hour_start.tzinfo is None or hour_start.utcoffset() is None:
            raise ValueError("UTC hour_start must include a timezone")
        hour_start = hour_start.astimezone(timezone.utc)
        if (hour_start.minute, hour_start.second, hour_start.microsecond) != (0, 0, 0):
            raise ValueError("hour_start must be an exact hour")
        if not frames:
            raise ValueError("Cannot write an empty acoustic hour")
        for component in (station, series):
            if not isinstance(component, str) or not component.strip() or component in (".", ".."):
                raise ValueError("Invalid station or series name")

        merged: dict[datetime, list[int | float]] = {}

        def add(frame: Mapping[str, Any]) -> None:
            moment = frame.get("start_time")
            values = frame.get("values")
            if not isinstance(moment, datetime) or moment.tzinfo is None or moment.utcoffset() is None:
                raise ValueError("Frame start_time must include a timezone")
            moment = moment.astimezone(timezone.utc)
            if moment.microsecond % 1000:
                raise ValueError("Acoustic timestamps must use millisecond precision")
            if moment.replace(minute=0, second=0, microsecond=0) != hour_start:
                raise ValueError("Frame does not belong to the requested hour")
            if not isinstance(values, list) or not values:
                raise ValueError("Frame values must be a nonempty array")
            if any(
                isinstance(v, bool) or not isinstance(v, (int, float)) or not math.isfinite(v)
                for v in values
            ):
                raise ValueError("Frame values must contain finite numbers")
            if moment in merged and merged[moment] != values:
                raise ValueError("Conflicting acoustic values for the same timestamp")
            merged[moment] = values

        if previous_path is not None:
            prior = pq.read_table(self._resolve(previous_path))
            if prior.column_names != ["start_time", "values"]:
                raise ValueError("Unexpected acoustic Parquet schema")
            for frame in prior.to_pylist():
                add(frame)
        before = len(merged)
        for frame in frames:
            add(frame)
        widths = {len(values) for values in merged.values()}
        if len(widths) != 1:
            raise ValueError("Array length changed within the same station/series/hour")
        width = widths.pop()
        added = len(merged) - before
        if added == 0 and previous_path is not None:
            return StoredHour(previous_path, len(merged), width, self._resolve(previous_path).stat().st_size, 0)

        ordered = sorted(merged)
        # Preserve integer arrays as int64, and float arrays as float64. No
        # undocumented int16 scaling or physical-unit interpretation is applied.
        all_integer = all(isinstance(v, int) for values in merged.values() for v in values)
        if not all_integer and any(
            isinstance(v, int) and abs(v) > 2**53 for values in merged.values() for v in values
        ):
            raise ValueError("Mixed numeric values would lose integer precision")
        value_type = pa.int64() if all_integer else pa.float64()
        table = pa.table({
            "start_time": pa.array(ordered, type=pa.timestamp("ms", tz="UTC")),
            "values": pa.array([merged[t] for t in ordered], type=pa.list_(value_type)),
        })
        sink = pa.BufferOutputStream()
        pq.write_table(table, sink, compression="zstd")
        content = sink.getvalue().to_pybytes()
        digest = hashlib.sha256(content).hexdigest()
        relative = (
            Path("sonic") / quote(station, safe="") / quote(series, safe="")
            / hour_start.strftime("%Y/%m/%d/%H") / f"{digest}.parquet"
        )
        target = self._resolve(relative)
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            temporary = None
            try:
                with tempfile.NamedTemporaryFile(dir=target.parent, suffix=".tmp", delete=False) as stream:
                    temporary = Path(stream.name)
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, target)
            finally:
                if temporary is not None:
                    temporary.unlink(missing_ok=True)
        return StoredHour(relative, len(merged), width, target.stat().st_size, added)
