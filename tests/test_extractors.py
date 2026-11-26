"""Synthetic API fixtures: no restricted flight or weather records."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone

import pytest

from acras.api import stamp
from acras.config import Config, PROJECT_STATION
from acras.extractors.flight import FlightExtractor
from acras.extractors.meteo import MeteoExtractor
from acras.extractors.reference import StationsExtractor, ZonesExtractor
from acras.extractors.sonic import SERIES, SonicExtractor


ZONE = "00000000-0000-0000-0000-000000000001"
OTHER_ZONE = "00000000-0000-0000-0000-000000000002"
START = datetime(2025, 1, 1, tzinfo=timezone.utc)
END = START + timedelta(seconds=10)


def flight(moment=START):
    return {
        "icao24": "abcdef", "dateTime": moment.isoformat(), "callsign": "TEST",
        "source": "synthetic", "icaoTypeCode": "TEST", "sourceId": None,
    }


def test_flight_hash_is_scoped_stable_and_ignores_expiring_photo_urls(config):
    extractor = FlightExtractor(None, None, config)
    first = flight()
    first["photoUrl"] = "https://example.invalid/expired"
    second = {**first, "photoUrl": "https://example.invalid/replacement"}
    left = extractor.to_rows(ZONE, [first], START, END)[0]
    right = extractor.to_rows(ZONE, [second], START, END)[0]
    other = extractor.to_rows(OTHER_ZONE, [first], START, END)[0]
    assert left == right
    assert left[0] != other[0]
    mapped = dict(zip(extractor.columns, left))
    assert mapped["callsign"] == "TEST"
    assert mapped["source_id"] is None
    assert "is_helicopter" not in mapped


def test_flight_keeps_subsecond_observations_and_excludes_shared_end(config):
    extractor = FlightExtractor(None, None, config)
    payload = [flight(START), flight(END - timedelta(microseconds=100)), flight(END)]
    rows = extractor.to_rows(ZONE, payload, START, END)
    assert len(rows) == 2
    assert dict(zip(extractor.columns, rows[-1]))["date_time"] == END - timedelta(microseconds=100)


def test_weather_preserves_grid_points_raw_content_and_units(config):
    extractor = MeteoExtractor(None, None, config)
    content = {"current": {"temperature_2m": 12.5}, "current_units": {"temperature_2m": "C"}}
    payload = [
        {"id": ZONE, "zoneId": ZONE, "dateTime": START.isoformat(), "latitude": 1,
         "longitude": 2, "content": content},
        {"id": OTHER_ZONE, "zoneId": ZONE, "dateTime": START.isoformat(), "latitude": 1,
         "longitude": 3, "content": content},
        {"id": ZONE, "zoneId": ZONE, "dateTime": END.isoformat(), "content": content},
    ]
    rows = extractor.to_rows(ZONE, payload, START, END)
    assert len(rows) == 2
    assert rows[0][0] != rows[1][0]
    assert rows[0][-1].obj == content
    assert rows[0][-1].obj["current_units"] == content["current_units"]
    with pytest.raises(ValueError, match="unexpected zoneId"):
        extractor.to_rows(OTHER_ZONE, payload[:1], START, END)


def test_acoustic_series_at_the_same_time_remain_distinct(config):
    extractor = SonicExtractor(None, None, config)
    epoch_ms = int(START.timestamp() * 1000)
    payload = {series: [
        {"startTime": epoch_ms, "values": [1, 2]},
        {"startTime": epoch_ms + 10_000, "values": [3, 4]},
    ] for series in SERIES}
    rows = extractor.to_rows(PROJECT_STATION, payload, START, END)
    assert len(rows) == 6
    assert {row[1] for row in rows} == set(SERIES)
    assert {row[2] for row in rows} == {START}
    invalid = deepcopy(payload)
    invalid[SERIES[0]][0]["values"] = [float("nan")]
    with pytest.raises(ValueError, match="finite numbers"):
        extractor.to_rows(PROJECT_STATION, invalid, START, END)
    invalid = deepcopy(payload)
    invalid[SERIES[0]][0]["startTime"] = "not epoch milliseconds"
    with pytest.raises(ValueError, match="epoch milliseconds"):
        extractor.to_rows(PROJECT_STATION, invalid, START, END)


@pytest.mark.parametrize("cls,scope,selector", [
    (FlightExtractor, ZONE, "zoneId"),
    (MeteoExtractor, ZONE, "zoneId"),
    (SonicExtractor, PROJECT_STATION, "stationName"),
])
def test_scope_selectors_and_utc_conversion(config, cls, scope, selector):
    params = cls(None, None, config).params(
        scope, START.astimezone(timezone(timedelta(hours=1))), END
    )
    assert params == {"from": "2025-01-01T00:00:00Z", "to": "2025-01-01T00:00:10Z", selector: scope}


@pytest.mark.parametrize("cls,scope,duration", [
    (FlightExtractor, ZONE, timedelta(hours=1, seconds=1)),
    (MeteoExtractor, ZONE, timedelta(days=1, seconds=1)),
    (SonicExtractor, PROJECT_STATION, timedelta(hours=1, seconds=1)),
])
def test_api_limits_are_enforced(config, cls, scope, duration):
    with pytest.raises(ValueError, match="at most"):
        cls(None, None, config).params(scope, START, START + duration)


def test_reference_mappings_use_nested_account_and_station_registry(config):
    zone = ZonesExtractor(None, None, config)
    rows = zone.to_rows([{
        "id": ZONE, "name": "Synthetic zone", "account": {"id": 1, "name": "Test"},
        "latMin": 1, "latMax": 2, "lngMin": 3, "lngMax": 4,
    }])
    assert dict(zip(zone.columns, rows[0]))["account_id"] == 1
    station = StationsExtractor(None, None, config)
    rows = station.to_rows([{
        "id": 1, "name": "ACRAS1-8", "latitude": 1.5, "longitude": 3.5,
    }, {
        "id": 2, "name": PROJECT_STATION, "latitude": 1.5, "longitude": 3.5,
        "altitude": 100, "nbSensors": 8, "sampleRate": 1600,
    }])
    assert station.path == "/station-registry"
    assert len(rows) == 1
    assert dict(zip(station.columns, rows[0]))["name"] == PROJECT_STATION
    assert dict(zip(station.columns, rows[0]))["sample_rate"] == 1600


def test_query_timestamps_require_timezone_and_do_not_truncate_fractional_seconds():
    with pytest.raises(ValueError, match="timezone"):
        stamp(START.replace(tzinfo=None))
    with pytest.raises(ValueError, match="whole seconds"):
        stamp(START.replace(microsecond=1000))


def test_config_repr_does_not_expose_credentials(config):
    assert config.api_key not in repr(config)
    assert config.database_url not in repr(config)


def test_config_rejects_an_environment_selecting_another_station(monkeypatch):
    monkeypatch.setenv("ACRAS_STATION_NAME", "ACRAS1-8")
    with pytest.raises(ValueError, match="only supports station ACRAS2-8"):
        Config.from_env()


def test_registry_without_acras2_is_not_marked_successful(config):
    with pytest.raises(ValueError, match="exactly one ACRAS2-8"):
        StationsExtractor(None, None, config).to_rows([{"id": 1, "name": "ACRAS1-8"}])


def test_direct_acoustic_extractor_calls_reject_acras1_before_io(config):
    extractor = SonicExtractor(None, None, config)
    actions = (
        lambda: extractor.run(["ACRAS1-8"], START, END),
        lambda: extractor.params("ACRAS1-8", START, END),
        lambda: extractor.to_rows("ACRAS1-8", {}, START, END),
        lambda: extractor._write([("ACRAS1-8", SERIES[0], START, [1, 2])]),
    )
    for action in actions:
        with pytest.raises(ValueError, match="only supports station ACRAS2-8"):
            action()
