"""Runtime configuration, read from the environment only.

Never hardcode the API key. It comes from .env (gitignored) or the Docker
environment. See .env.example.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.environ.get(name, default)
    if required and not value:
        raise RuntimeError(
            f"{name} is not set. Copy .env.example to .env and fill it in, "
            f"or pass it through docker compose."
        )
    return value or ""


@dataclass(frozen=True)
class Config:
    api_key: str
    base_url: str
    database_url: str
    data_dir: Path

    # Per-request window sizes, tuned to the measured payload sizes:
    #   flight ~4.3 MB/hour, meteo tiny, sonic ~25.6 MB/hour/station.
    flight_window: timedelta = timedelta(hours=1)
    meteo_window: timedelta = timedelta(hours=6)
    sonic_window: timedelta = timedelta(minutes=10)

    http_timeout: float = 120.0
    max_retries: int = 5

    @classmethod
    def from_env(cls) -> "Config":
        return cls(
            api_key=_env("ACRAS_API_KEY", required=True),
            base_url=_env("ACRAS_BASE_URL", "https://acras.iav.ch/api").rstrip("/"),
            database_url=_env(
                "DATABASE_URL",
                "postgresql://acras:acras@localhost:5432/acras",
            ),
            data_dir=Path(_env("ACRAS_DATA_DIR", "./data")).resolve(),
        )
