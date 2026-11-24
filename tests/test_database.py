"""Optional real PostgreSQL tests; synthetic data only, never ACRAS API calls.

Set ACRAS_TEST_DATABASE_URL to a disposable PostgreSQL 14+ database. Each test
creates and removes its own unique schema. Do not point this at production.
"""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
import os
from types import SimpleNamespace
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.rows import dict_row
import pytest

from acras import db
from acras.config import PROJECT_STATION
from acras.extractors.flight import FlightExtractor
from acras.extractors.meteo import MeteoExtractor
from acras.extractors.reference import StationsExtractor, ZonesExtractor
from acras.extractors.sonic import SERIES, SonicExtractor


ZONE = "00000000-0000-0000-0000-000000000001"
START = datetime(2025, 1, 1, tzinfo=timezone.utc)


@pytest.fixture
def database():
    url = os.environ.get("ACRAS_TEST_DATABASE_URL")
    if not url:
        pytest.skip("Set ACRAS_TEST_DATABASE_URL to run real PostgreSQL integration tests")
    schema = f"acras_test_{uuid4().hex}"
    conn = psycopg.connect(url, row_factory=dict_row, connect_timeout=5)
    try:
        conn.execute(sql.SQL("create schema {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("set search_path to {}, public").format(sql.Identifier(schema)))
        conn.commit()
        assert db.run_migrations(conn) == ["001_schema.sql", "002_ingest_gap_coverage.sql"]
        assert db.run_migrations(conn) == []
        yield conn
    finally:
        conn.rollback()
        conn.execute("set search_path to public")
        conn.execute(sql.SQL("drop schema if exists {} cascade").format(sql.Identifier(schema)))
        conn.commit()
        conn.close()


def references(conn, config):
    client = SimpleNamespace(get=lambda path: [{
        "id": ZONE, "name": "Synthetic zone", "latMin": 0, "latMax": 2,
        "lngMin": 0, "lngMax": 2,
    }] if path == "/zones" else [{
        "id": 1, "name": PROJECT_STATION, "latitude": 1, "longitude": 1,
        "nbSensors": 8, "sampleRate": 1600,
    }])
    assert not ZonesExtractor(client, conn, config).run().failures
    assert not StationsExtractor(client, conn, config).run().failures


def test_relational_deduplication_and_resume(database, config):
    references(database, config)
    flight_payload = [
        {"icao24": "abcdef", "dateTime": START.isoformat(), "callsign": "TEST"},
        {"icao24": "abcdef", "dateTime": START.isoformat(), "callsign": "TEST"},
    ]
    client = SimpleNamespace(get=lambda path, **params: flight_payload)
    extractor = FlightExtractor(client, database, config)
    assert extractor.run([ZONE], START, START + timedelta(seconds=10)).rows_written == 1
    assert extractor.run([ZONE], START, START + timedelta(seconds=10)).windows == 0
    assert extractor.run([ZONE], START, START + timedelta(seconds=10), resume=False).rows_written == 0
    assert database.execute("select count(*) as n from flight_data").fetchone()["n"] == 1
    assert database.execute("select count(*) as n from aircraft").fetchone()["n"] == 1
    payload = [{
        "id": str(uuid4()), "zoneId": ZONE, "dateTime": START.isoformat(),
        "latitude": 1, "longitude": longitude,
        "content": {"current": {"temperature_2m": 12.5}, "current_units": {"temperature_2m": "C"}},
    } for longitude in (1, 1.5)]
    weather = MeteoExtractor(SimpleNamespace(get=lambda *a, **kw: payload), database, config)
    assert weather.run([ZONE], START, START + timedelta(seconds=10)).rows_written == 2
    values = database.execute("select temperature_2m, content from meteo_data").fetchall()
    assert all(row["temperature_2m"] == 12.5 for row in values)
    assert all(row["content"]["current_units"]["temperature_2m"] == "C" for row in values)


def test_acoustic_manifest_remains_valid_after_database_rollback(database, config):
    pq = pytest.importorskip("pyarrow.parquet")
    references(database, config)
    config = replace(config, sonic_window=timedelta(seconds=10))

    def get(path, **params):
        moment = datetime.fromisoformat(params["from"].replace("Z", "+00:00"))
        return {series: [{"startTime": int(moment.timestamp() * 1000), "values": [1, 2]}]
                for series in SERIES}

    sonic = SonicExtractor(SimpleNamespace(get=get), database, config)
    assert sonic.run([PROJECT_STATION], START, START + timedelta(seconds=20)).rows_written == 12
    prior = database.execute(
        "select path from sonic_frame_file where series = 'lfSpectrogram'"
    ).fetchone()["path"]
    assert pq.read_table(config.data_dir / prior).num_rows == 2
    database.execute("""create function reject_manifest() returns trigger language plpgsql as $$
        begin raise exception 'synthetic manifest failure'; end $$""")
    database.execute("""create trigger reject_manifest before insert or update on sonic_frame_file
        for each row execute function reject_manifest()""")
    database.commit()
    result = sonic.run([PROJECT_STATION], START + timedelta(seconds=20), START + timedelta(seconds=30))
    assert result.failures
    current = database.execute(
        "select path from sonic_frame_file where series = 'lfSpectrogram'"
    ).fetchone()["path"]
    assert current == prior
    assert pq.read_table(config.data_dir / prior).num_rows == 2
    assert database.execute("select count(*) as n from ingest_gap").fetchone()["n"] == 1
