"""Postgres access. Plain SQL, no ORM.

Migrations are the .sql files in sql/, applied once each in filename order.
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Sequence

import psycopg
from psycopg import Connection
from psycopg.rows import dict_row

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def connect(database_url: str) -> Connection:
    """Open a connection with dict rows. Caller owns the transaction."""
    return psycopg.connect(database_url, row_factory=dict_row, autocommit=False)


def run_migrations(conn: Connection, sql_dir: Path = SQL_DIR) -> list[str]:
    """Apply any .sql file in sql_dir that has not been applied yet."""
    with conn.cursor() as cur:
        cur.execute(
            """
            create table if not exists schema_migration (
                filename    text primary key,
                applied_at  timestamptz not null default now()
            )
            """
        )
        cur.execute("select filename from schema_migration")
        done = {row["filename"] for row in cur.fetchall()}
    conn.commit()

    applied: list[str] = []
    for path in sorted(sql_dir.glob("*.sql")):
        if path.name in done:
            continue
        log.info("applying migration %s", path.name)
        with conn.cursor() as cur:
            cur.execute(path.read_text())
            cur.execute(
                "insert into schema_migration (filename) values (%s)", (path.name,)
            )
        conn.commit()
        applied.append(path.name)
    return applied


def insert_many(
    conn: Connection,
    table: str,
    columns: Sequence[str],
    rows: Iterable[Sequence[Any]],
    *,
    conflict: str = "",
) -> int:
    """Bulk insert. Returns the number of rows the server actually wrote.

    `conflict` is appended verbatim, e.g. "on conflict (id) do nothing" or a
    full do-update clause. It is developer-supplied SQL, never user input.
    """
    rows = list(rows)
    if not rows:
        return 0

    placeholders = "(" + ", ".join(["%s"] * len(columns)) + ")"
    statement = (
        f"insert into {table} ({', '.join(columns)}) values {placeholders} {conflict}"
    )
    with conn.cursor() as cur:
        cur.executemany(statement, rows)
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0


# --------------------------------------------------------------- bookkeeping


def resume_point(conn: Connection, extractor: str, scope_key: str) -> datetime | None:
    """Where to pick up: the end of the latest window this scope committed.

    Derived from ingest_run rather than stored, so there is no second piece of
    state that can disagree with the audit log after a crash.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            select window_end from ingest_progress
             where extractor = %s and scope_key = %s
            """,
            (extractor, scope_key),
        )
        row = cur.fetchone()
    return row["window_end"] if row else None


def gaps(conn: Connection, extractor: str | None = None) -> list[dict[str, Any]]:
    """Windows that failed and were never retried successfully."""
    with conn.cursor() as cur:
        if extractor:
            cur.execute(
                "select * from ingest_gap where extractor = %s order by window_from",
                (extractor,),
            )
        else:
            cur.execute("select * from ingest_gap order by extractor, window_from")
        return cur.fetchall()


def stale_runs(conn: Connection, older_than: str = "1 hour") -> list[dict[str, Any]]:
    """Runs stuck in 'running' because the process was hard-killed."""
    with conn.cursor() as cur:
        cur.execute(
            """
            select * from ingest_run
             where status = 'running'
               and started_at < now() - %s::interval
             order by started_at
            """,
            (older_than,),
        )
        return cur.fetchall()


def start_run(
    conn: Connection,
    extractor: str,
    scope_key: str | None,
    window_from: datetime | None,
    window_to: datetime | None,
) -> int:
    """Open an audit row and commit it immediately.

    Committing here is deliberate: the audit log must survive the rollback of
    the data transaction it describes, otherwise a failed window erases its own
    evidence. Call this only at a window boundary, with no uncommitted data
    work pending on the connection.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingest_run (extractor, scope_key, window_from, window_to, status)
            values (%s, %s, %s, %s, 'running')
            returning id
            """,
            (extractor, scope_key, window_from, window_to),
        )
        run_id = cur.fetchone()["id"]
    conn.commit()
    return run_id


def finish_run(
    conn: Connection,
    run_id: int,
    *,
    status: str,
    rows_seen: int = 0,
    rows_written: int = 0,
    error: str | None = None,
) -> None:
    """Close out an audit row and commit it.

    On the failure path the caller must roll back the data transaction *before*
    calling this, so the 'failed' row is written on a clean transaction and
    survives.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            update ingest_run
               set status = %s, rows_seen = %s, rows_written = %s,
                   error = %s, finished_at = now()
             where id = %s
            """,
            (status, rows_seen, rows_written, error, run_id),
        )
    conn.commit()
