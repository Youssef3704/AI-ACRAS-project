"""Extractor base classes.

The base owns everything mechanical and route-independent:
  * splitting a requested time range into API-sized windows,
  * resuming from the last committed window,
  * one transaction per window, so a crash costs at most one window,
  * an audit row per window attempt,
  * idempotent writes.

A route subclass supplies only three things: where to GET, what parameters to
send, and how to turn the payload into rows. Keep it that way -- the four
routes have genuinely different payload shapes (flat records, nested JSON,
dict-of-series), and pushing that variation into the base class makes it
unreadable.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator, Sequence

from psycopg import Connection

from .. import db
from ..api import AcrasClient, stamp
from ..config import Config

log = logging.getLogger(__name__)


@dataclass
class Stats:
    """What one run of an extractor did."""

    windows: int = 0
    rows_seen: int = 0
    rows_written: int = 0
    failures: list[str] = field(default_factory=list)

    def __iadd__(self, other: "Stats") -> "Stats":
        self.windows += other.windows
        self.rows_seen += other.rows_seen
        self.rows_written += other.rows_written
        self.failures.extend(other.failures)
        return self

    def __str__(self) -> str:
        text = (
            f"{self.windows} window(s), {self.rows_seen} seen, "
            f"{self.rows_written} written"
        )
        if self.failures:
            text += f", {len(self.failures)} FAILED"
        return text


def iter_windows(
    start: datetime, end: datetime, size: timedelta
) -> Iterator[tuple[datetime, datetime]]:
    """Split [start, end) into half-open chunks of at most `size`.

    Adapters request the full end bound and filter responses to [start, end).
    This handles an inclusive API `to` without dropping subsecond observations.
    """
    if size <= timedelta(0):
        raise ValueError("window size must be positive")
    cursor = start
    while cursor < end:
        stop = min(cursor + size, end)
        yield cursor, stop
        cursor = stop


def uncovered_ranges(
    start: datetime, end: datetime, covered: Sequence[tuple[datetime, datetime]]
) -> Iterator[tuple[datetime, datetime]]:
    """Subtract successful coverage without skipping earlier gaps or backfills."""
    cursor = start
    for lower, upper in sorted(covered):
        if upper <= cursor or lower >= end:
            continue
        if lower > cursor:
            yield cursor, lower
        cursor = max(cursor, upper)
        if cursor >= end:
            return
    if cursor < end:
        yield cursor, end


class Extractor(ABC):
    """Common wiring. Subclass StaticExtractor or WindowedExtractor instead."""

    #: Stable identifier, used as the audit-log key. Do not rename casually:
    #: it is what resume points are recorded against.
    name: str = ""
    #: API route to GET, relative to the configured base URL.
    path: str = ""
    #: Target table, and the columns `to_rows` yields in order.
    table: str = ""
    columns: Sequence[str] = ()
    #: Appended to the INSERT so re-ingesting a window is a no-op.
    conflict: str = ""

    def __init__(self, client: AcrasClient, conn: Connection, config: Config) -> None:
        self.client = client
        self.conn = conn
        self.config = config

    @abstractmethod
    def run(self, *args: Any, **kwargs: Any) -> Stats: ...

    def _write(self, rows: Sequence[Sequence[Any]]) -> int:
        return db.insert_many(
            self.conn, self.table, self.columns, rows, conflict=self.conflict
        )


class StaticExtractor(Extractor):
    """For small reference routes with no time dimension: zones, stations.

    Re-fetched in full on every run and upserted; they are a handful of rows.
    """

    @abstractmethod
    def to_rows(self, payload: Any) -> list[Sequence[Any]]:
        """Turn the raw JSON payload into insertable tuples."""

    def run(self) -> Stats:
        stats = Stats(windows=1)
        run_id = db.start_run(self.conn, self.name, None, None, None)
        try:
            payload = self.client.get(self.path)
            rows = self.to_rows(payload)
            stats.rows_seen = len(rows)
            stats.rows_written = self._write(rows)
            # Commit data and the success audit together. Otherwise a crash
            # between their commits can leave the progress log behind the data.
            db.finish_run(
                self.conn, run_id, status="ok",
                rows_seen=stats.rows_seen, rows_written=stats.rows_written,
            )
        except Exception as exc:
            self.conn.rollback()
            stats.rows_written = 0
            db.finish_run(
                self.conn, run_id, status="failed", rows_seen=stats.rows_seen, error=str(exc)
            )
            stats.failures.append(f"{self.name}: {exc}")
            log.exception("%s failed", self.name)
            return stats

        log.info("%s: %s", self.name, stats)
        return stats


class WindowedExtractor(Extractor):
    """For time-ranged routes: flight data, meteo data, sonic data.

    One scope is one independently-tracked stream -- a zone id for flight and
    meteo, a station name for sonic -- so scopes resume independently.
    """

    @property
    @abstractmethod
    def window(self) -> timedelta:
        """How much time to request per call. Sized to the payload volume."""

    @abstractmethod
    def params(self, scope: str, start: datetime, end: datetime) -> dict[str, Any]:
        """Query parameters for one window of one scope."""

    @abstractmethod
    def to_rows(
        self, scope: str, payload: Any, start: datetime, end: datetime
    ) -> list[Sequence[Any]]:
        """Turn the raw JSON payload into insertable tuples."""

    def before_window(self, scope: str, start: datetime, end: datetime) -> None:
        """Hook for subclasses needing per-window setup. Default: nothing."""

    def run(
        self,
        scopes: Sequence[str],
        start: datetime,
        end: datetime,
        *,
        resume: bool = True,
    ) -> Stats:
        stamp(start)
        stamp(end)
        if start >= end:
            raise ValueError("start must be earlier than end")
        if not scopes:
            raise ValueError("At least one scope is required")
        total = Stats()
        for scope in dict.fromkeys(scopes):
            total += self._run_scope(scope, start, end, resume=resume)
        return total

    def _run_scope(
        self, scope: str, start: datetime, end: datetime, *, resume: bool
    ) -> Stats:
        stats = Stats()

        covered = db.committed_windows(self.conn, self.name, scope, start, end) if resume else []
        for missing_start, missing_end in uncovered_ranges(start, end, covered):
            for win_start, win_end in iter_windows(missing_start, missing_end, self.window):
                stats += self._run_window(scope, win_start, win_end)

        log.info("%s[%s]: %s", self.name, scope, stats)
        return stats

    def _run_window(self, scope: str, start: datetime, end: datetime) -> Stats:
        """One window, one transaction, one audit row."""
        stats = Stats(windows=1)
        run_id = db.start_run(self.conn, self.name, scope, start, end)
        try:
            self.before_window(scope, start, end)
            payload = self.client.get(self.path, **self.params(scope, start, end))
            rows = self.to_rows(scope, payload, start, end)
            stats.rows_seen = len(rows)
            stats.rows_written = self._write(rows)
            db.finish_run(
                self.conn, run_id, status="ok",
                rows_seen=stats.rows_seen, rows_written=stats.rows_written,
            )
        except Exception as exc:
            # Roll back the data first so the audit row commits cleanly and the
            # failure is not erased along with the partial write.
            self.conn.rollback()
            stats.rows_written = 0
            db.finish_run(
                self.conn, run_id, status="failed", rows_seen=stats.rows_seen, error=repr(exc)
            )
            stats.failures.append(f"{self.name}[{scope}] {start.isoformat()}: {exc}")
            log.error(
                "%s[%s] window %s..%s failed: %s",
                self.name, scope, start.isoformat(), end.isoformat(), exc,
            )
            return stats

        log.info(
            "%s[%s] %s..%s: %d seen, %d written",
            self.name, scope, start.isoformat(), end.isoformat(),
            stats.rows_seen, stats.rows_written,
        )
        return stats


def parse_timestamp(value: str | int | None) -> datetime | None:
    """Parse the two timestamp shapes the API uses.

    Flight and meteo send ISO-8601 with a trailing Z; sonic frames send epoch
    milliseconds. Everything is stored timezone-aware in UTC.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value / 1000.0, tz=timezone.utc)
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
