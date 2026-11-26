"""Recovery across gaps, overlapping successes, and transaction failures."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from acras import db
from acras.extractors.base import WindowedExtractor, uncovered_ranges


START = datetime(2025, 1, 1, tzinfo=timezone.utc)


def at(hours):
    return START + timedelta(hours=hours)


def test_resume_fills_earlier_holes_without_redownloading_committed_ranges():
    covered = [(at(1), at(2)), (at(3), at(5)), (at(4), at(6))]
    assert list(uncovered_ranges(at(0), at(7), covered)) == [
        (at(0), at(1)), (at(2), at(3)), (at(6), at(7))
    ]
    assert list(uncovered_ranges(at(4), at(5), covered)) == []


class Probe(WindowedExtractor):
    name = "probe"
    path = "/synthetic"

    @property
    def window(self):
        return timedelta(hours=1)

    def params(self, scope, start, end):
        return {"start": start, "end": end}

    def to_rows(self, scope, payload, start, end):
        return payload

    def _write(self, rows):
        self.conn.events.append("write")
        if self.conn.fail:
            raise ValueError("synthetic write failure")
        return len(rows)


def test_data_and_success_log_commit_together_and_failure_rolls_back_first(monkeypatch, config):
    events = []
    conn = SimpleNamespace(events=events, fail=False)
    conn.rollback = lambda: events.append("rollback")
    conn.commit = lambda: events.append("commit")
    calls = []
    client = SimpleNamespace(get=lambda path, **params: calls.append(params) or [(1,)])

    def start_run(*args):
        events.append("start audit")
        conn.commit()
        return 1

    def finish_run(*args, status, **kwargs):
        events.append(status)
        conn.commit()

    monkeypatch.setattr(db, "start_run", start_run)
    monkeypatch.setattr(db, "finish_run", finish_run)
    monkeypatch.setattr(db, "committed_windows", lambda *args: [(at(1), at(2))])
    extractor = Probe(client, conn, config)
    stats = extractor.run(["test"], at(0), at(3))
    assert stats.windows == 2
    assert stats.rows_written == 2
    assert [call["start"] for call in calls] == [at(0), at(2)]
    assert events[:5] == ["start audit", "commit", "write", "ok", "commit"]
    conn.fail = True
    events.clear()
    stats = extractor.run(["test"], at(0), at(1), resume=False)
    assert stats.failures
    assert events == ["start audit", "commit", "write", "rollback", "failed", "commit"]

    def failing_success_audit(*args, status, **kwargs):
        events.append(status)
        if status == "ok":
            raise ValueError("synthetic audit failure")
        conn.commit()

    conn.fail = False
    events.clear()
    monkeypatch.setattr(db, "finish_run", failing_success_audit)
    stats = extractor.run(["test"], at(0), at(1), resume=False)
    assert stats.failures
    assert stats.rows_written == 0
    assert events == ["start audit", "commit", "write", "ok", "rollback", "failed", "commit"]
