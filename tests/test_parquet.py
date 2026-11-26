"""Real Parquet round trips using synthetic observations in temporary folders."""

from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

pytest.importorskip("pyarrow")
import pyarrow.parquet as pq

from acras.storage.parquet import ParquetStore


HOUR = datetime(2025, 1, 1, tzinfo=timezone.utc)


def frame(second, values=None):
    return {"start_time": HOUR + timedelta(seconds=second), "values": values or [1, 2, 3]}


def write(store, frames, previous=None):
    return store.merge_hour(
        station="TEST-STATION", series="lfSpectrogram", hour_start=HOUR,
        frames=frames, previous_path=previous,
    )


def test_merge_preserves_earlier_batches_and_old_manifest_after_rollback(tmp_path):
    store = ParquetStore(tmp_path)
    first = write(store, [frame(0), frame(1)])
    original_bytes = (tmp_path / first.path).read_bytes()
    second = write(store, [frame(1), frame(600)], first.path)
    assert second.frames_added == 1
    assert second.n_frames == 3
    assert second.values_per_frame == 3
    assert second.path != first.path
    assert (tmp_path / first.path).read_bytes() == original_bytes
    assert pq.read_table(tmp_path / first.path).num_rows == 2
    result = pq.read_table(tmp_path / second.path).to_pylist()
    assert [row["start_time"] for row in result] == [frame(i)["start_time"] for i in (0, 1, 600)]
    assert write(store, [frame(0), frame(600)], second.path) == type(second)(
        second.path, second.n_frames, second.values_per_frame, second.bytes, 0
    )


def test_conflicting_duplicates_do_not_modify_existing_file(tmp_path):
    store = ParquetStore(tmp_path)
    first = write(store, [frame(0)])
    with pytest.raises(ValueError, match="Conflicting"):
        write(store, [frame(0, [9, 9, 9])], first.path)
    assert pq.read_table(tmp_path / first.path).to_pylist()[0]["values"] == [1, 2, 3]


def test_frame_dimensions_and_hour_membership_are_checked(tmp_path):
    store = ParquetStore(tmp_path)
    with pytest.raises(ValueError, match="Array length changed"):
        write(store, [frame(0), frame(1, [1])])
    with pytest.raises(ValueError, match="requested hour"):
        write(store, [frame(3600)])
    with pytest.raises(ValueError, match="relative"):
        write(store, [frame(0)], tmp_path / "absolute.parquet")
    with pytest.raises(ValueError, match="escapes"):
        write(store, [frame(0)], Path("../outside.parquet"))


def test_numeric_types_and_fractional_milliseconds_survive_roundtrip(tmp_path):
    store = ParquetStore(tmp_path)
    sample = {"start_time": HOUR + timedelta(milliseconds=125), "values": [1.5, -2.25]}
    result = write(store, [sample])
    assert pq.read_table(tmp_path / result.path).to_pylist() == [sample]


def test_path_components_cannot_create_parent_directories(tmp_path):
    store = ParquetStore(tmp_path)
    result = store.merge_hour(
        station="some/station", series="some/series", hour_start=HOUR, frames=[frame(0)]
    )
    assert "some%2Fstation" in result.path.parts
    assert (tmp_path / result.path).resolve().is_relative_to(tmp_path)
