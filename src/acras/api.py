"""Thin HTTP client for the ACRAS API.

Only concern here is getting bytes back reliably: auth, timeouts, retries with
backoff. Parsing and persistence belong to the extractors.
"""

from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Any

import requests

log = logging.getLogger(__name__)

# The API rejects fractional seconds on the window bounds.
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Transient conditions worth retrying; anything else is a real error.
RETRY_STATUS = {429, 500, 502, 503, 504}


def stamp(moment: datetime) -> str:
    """Format a datetime the way the ACRAS query parameters expect."""
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("Query timestamps must include a timezone")
    if moment.microsecond:
        raise ValueError("Query timestamps must use whole seconds")
    return moment.astimezone(timezone.utc).strftime(TIMESTAMP_FORMAT)


class AcrasApiError(RuntimeError):
    def __init__(self, status: int, path: str, body: str) -> None:
        super().__init__(f"{status} {path}: {body[:500]}")
        self.status = status
        self.path = path
        self.body = body


class AcrasClient:
    def __init__(
        self,
        api_key: str,
        base_url: str = "https://acras.iav.ch/api",
        timeout: float = 120.0,
        max_retries: int = 5,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(
            {"X-API-Key": api_key, "Accept": "application/json"}
        )

    def get(self, path: str, **params: Any) -> Any:
        """GET a route and return parsed JSON, retrying transient failures."""
        url = f"{self.base_url}{path}"
        clean = {k: v for k, v in params.items() if v is not None}

        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                response = self.session.get(url, params=clean, timeout=self.timeout)
            except requests.RequestException as exc:
                last_error = exc
                self._backoff(attempt, f"{path}: {exc}")
                continue

            if response.ok:
                return response.json()

            if response.status_code in RETRY_STATUS:
                last_error = AcrasApiError(response.status_code, path, response.text)
                # Honour Retry-After when the server sends one.
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after and retry_after.isdigit() else None
                self._backoff(attempt, f"{path}: HTTP {response.status_code}", delay)
                continue

            # 4xx other than 429: the request is wrong, retrying will not help.
            raise AcrasApiError(response.status_code, path, response.text)

        raise AcrasApiError(0, path, f"exhausted {self.max_retries} retries: {last_error}")

    def _backoff(self, attempt: int, reason: str, delay: float | None = None) -> None:
        wait = delay if delay is not None else (2**attempt) + random.uniform(0, 1)
        log.warning("retrying in %.1fs after %s", wait, reason)
        time.sleep(wait)
