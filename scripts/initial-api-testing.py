"""Smoke-test the ACRAS API: list zones, then pull a slice of historic flight data."""

import os
import sys
from datetime import datetime, timedelta, timezone

import requests

BASE_URL = "https://acras.iav.ch/api"

api_key =""
if not api_key:
    sys.exit("ACRAS_API_KEY is not set. Put it in .env and export it before running.")

session = requests.Session()
session.headers.update({"X-API-Key": api_key})


def get(path, **params):
    response = session.get(f"{BASE_URL}{path}", params=params, timeout=10)
    if not response.ok:
        print(f"{response.status_code} {path} -> {response.text}", file=sys.stderr)
    response.raise_for_status()
    return response.json()


zones = get("/zones")
print(f"{len(zones)} zone(s):")
for zone in zones:
    print(f"  {zone['id']}  {zone['name']}  active={zone['active']}")

zone_id = os.environ.get("ACRAS_ZONE_ID") or zones[0]["id"]

to = datetime(2025, 8, 8, 10, 21, 25, tzinfo=timezone.utc)
frm = to - timedelta(seconds=5)
stamp = lambda dt: dt.strftime("%Y-%m-%dT%H:%M:%SZ")

records = get(
    "/flight-data/historic",
    **{"from": stamp(frm), "to": stamp(to), "zoneId": zone_id},
)
print(f"\n{len(records)} record(s) for zone {zone_id} between {stamp(frm)} and {stamp(to)}")
for record in records[:5]:
    print(
        f"  {record['dateTime']}  {record['icao24']}  {record.get('callsign')}  "
        f"({record.get('latitude')}, {record.get('longitude')})  alt={record.get('baroAltitudeM')}m"
    )
