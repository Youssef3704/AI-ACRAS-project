"""Exercise the CLI's offline contract without credentials or a database."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "download_data.py"


def run_cli(*args):
    env = os.environ.copy()
    for name in ("ACRAS_API_KEY", "DATABASE_URL", "ACRAS_DATA_DIR", "ACRAS_STATION_NAME"):
        env.pop(name, None)
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_all_sources_use_independent_windows_and_keep_partial_last_window():
    result = run_cli(
        "--source", "all", "--zone-id", "test-zone",
        "--station-name", "ACRAS2-8", "--dry-run",
        "--start", "2025-08-08T10:00:00Z", "--end", "2025-08-08T11:05:00Z",
    )
    assert result.returncode == 0, result.stderr
    lines = result.stdout.splitlines()
    assert sum(line.startswith("flight\t") for line in lines) == 2
    assert sum(line.startswith("meteo\t") for line in lines) == 1
    assert sum(line.startswith("sonic\t") for line in lines) == 7
    assert "[2025-08-08T11:00:00+00:00, 2025-08-08T11:05:00+00:00)" in result.stdout
    assert "10 planned request(s)" in result.stdout


@pytest.mark.parametrize(
    "start,end,message",
    [
        ("2025-08-08T10:00:00", "2025-08-08T11:00:00Z", "timezone offset"),
        ("2025-08-08T10:00:00.5Z", "2025-08-08T11:00:00Z", "whole-second"),
        ("2025-08-08T11:00:00Z", "2025-08-08T10:00:00Z", "earlier than"),
    ],
)
def test_invalid_time_ranges_are_rejected(start, end, message):
    result = run_cli(
        "--source", "flight", "--zone-id", "test-zone", "--dry-run",
        "--start", start, "--end", end,
    )
    assert result.returncode == 2
    assert message in result.stderr


def test_download_requires_an_explicit_execution_mode():
    result = run_cli(
        "--source", "flight", "--zone-id", "test-zone",
        "--start", "2025-08-08T10:00:00Z", "--end", "2025-08-08T11:00:00Z",
    )
    assert result.returncode == 2
    assert "--dry-run --execute" in result.stderr
    assert "ACRAS_API_KEY is not set" not in result.stderr


def test_all_sources_default_to_acras2_only():
    result = run_cli(
        "--source", "all", "--zone-id", "test-zone", "--dry-run",
        "--start", "2025-08-08T10:00:00Z", "--end", "2025-08-08T11:00:00Z",
    )
    assert result.returncode == 0, result.stderr
    sonic = [line for line in result.stdout.splitlines() if line.startswith("sonic\t")]
    assert len(sonic) == 6
    assert all(line.startswith("sonic\tACRAS2-8\t") for line in sonic)


def test_acras1_selection_is_rejected_before_any_connection():
    result = run_cli(
        "--source", "all", "--zone-id", "test-zone", "--dry-run",
        "--station-name", "ACRAS1-8",
        "--start", "2025-08-08T10:00:00Z", "--end", "2025-08-08T11:00:00Z",
    )
    assert result.returncode == 2
    assert "invalid choice" in result.stderr
    assert result.stdout == ""


def test_execute_checks_credentials_before_connecting_to_any_service():
    result = run_cli(
        "--source", "flight", "--zone-id", "00000000-0000-0000-0000-000000000001",
        "--start", "2025-08-08T10:00:00Z", "--end", "2025-08-08T11:00:00Z", "--execute",
    )
    assert result.returncode == 1
    assert "ACRAS_API_KEY is not set" in result.stderr
    assert result.stdout == ""
