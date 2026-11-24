"""Validation shared by the historical API adapters."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from ..api import stamp
from .base import parse_timestamp


def records(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, list) or any(not isinstance(row, dict) for row in payload):
        raise ValueError("Expected a JSON array of observation objects")
    return payload


def timestamp(value: Any) -> datetime:
    if isinstance(value, bool) or not isinstance(value, (str, int, float)):
        raise ValueError("Observation timestamp is missing or invalid")
    result = parse_timestamp(value)
    if result is None:
        raise ValueError("Observation timestamp is missing")
    return result


def text(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field} must be a nonempty string")
    return value


def window_params(
    scope: str, start: datetime, end: datetime, *, selector: str, maximum: timedelta
) -> dict[str, str]:
    text(scope, selector)
    bounds = {"from": stamp(start), "to": stamp(end)}
    if not timedelta(0) < end - start <= maximum:
        raise ValueError(f"Request duration must be positive and at most {maximum}")
    return {**bounds, selector: scope}


def utc_hour(moment: datetime) -> datetime:
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise ValueError("A timezone is required")
    return moment.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
