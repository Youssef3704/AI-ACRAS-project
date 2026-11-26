"""Preview or run ACRAS extraction from an editable checkout.

Use --dry-run for an offline preview or --execute for authenticated downloads.
Load your local .env into the shell before execution.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import logging
import sys
from uuid import UUID

from acras.config import PROJECT_STATION


def utc_timestamp(value: str) -> datetime:
    try:
        moment = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("Use an ISO timestamp with a timezone.") from exc
    if moment.tzinfo is None or moment.utcoffset() is None:
        raise argparse.ArgumentTypeError("Include Z or a timezone offset in timestamps.")
    if moment.microsecond:
        raise argparse.ArgumentTypeError("Use whole-second timestamps.")
    return moment.astimezone(timezone.utc)


def verify_scopes(conn, zone_ids):
    """Verify every selected zone contains the project's ACRAS2-8 station.

    Bounding-box containment is a geographic consistency check, not proof of
    an official station-to-zone assignment or an audible aircraft association.
    """
    with conn.cursor() as cur:
        cur.execute(
            "select latitude, longitude from station where name = %s", (PROJECT_STATION,)
        )
        station = cur.fetchone()
        if not station or station["latitude"] is None or station["longitude"] is None:
            raise ValueError(f"{PROJECT_STATION} coordinates are missing from the station registry")
        for zone in zone_ids:
            cur.execute("select lat_min, lat_max, lng_min, lng_max from zone where id = %s", (zone,))
            bounds = cur.fetchone()
            if not bounds:
                raise ValueError("A selected zone is not accessible in the zone registry")
            if any(bounds[key] is None for key in ("lat_min", "lat_max", "lng_min", "lng_max")):
                raise ValueError(f"Zone bounds are missing; cannot verify coverage of {PROJECT_STATION}")
            if not (
                bounds["lat_min"] <= station["latitude"] <= bounds["lat_max"]
                and bounds["lng_min"] <= station["longitude"] <= bounds["lng_max"]
            ):
                raise ValueError(f"A selected zone does not contain {PROJECT_STATION}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=("flight", "meteo", "sonic", "all"), required=True)
    parser.add_argument("--start", type=utc_timestamp, required=True)
    parser.add_argument("--end", type=utc_timestamp, required=True, help="Exclusive end of the period")
    parser.add_argument("--zone-id", action="append", default=[], help="Repeat for multiple zones")
    parser.add_argument(
        "--station-name", choices=(PROJECT_STATION,), default=PROJECT_STATION,
        help=f"Project station, fixed to {PROJECT_STATION}",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--dry-run", action="store_true", help="Preview without credentials or connections")
    mode.add_argument("--execute", action="store_true", help="Download and persist observations")
    parser.add_argument("--no-resume", action="store_true", help="Re-fetch even already committed windows")
    args = parser.parse_args(argv)

    if args.start >= args.end:
        parser.error("--start must be earlier than --end")
    sources = ("flight", "meteo", "sonic") if args.source == "all" else (args.source,)
    if any(source in ("flight", "meteo") for source in sources) and not args.zone_id:
        parser.error("Flight and weather requests require an explicit --zone-id")
    if any(not value.strip() for value in args.zone_id):
        parser.error("Zone selectors cannot be empty")
    stations = [PROJECT_STATION]

    from acras.config import Config
    from acras.extractors.base import iter_windows

    windows = {
        "flight": Config.flight_window,
        "meteo": Config.meteo_window,
        "sonic": Config.sonic_window,
    }
    if args.dry_run:
        count = 0
        for source in sources:
            scopes = stations if source == "sonic" else args.zone_id
            for scope in dict.fromkeys(scopes):
                for start, end in iter_windows(args.start, args.end, windows[source]):
                    print(f"{source}\t{scope}\t[{start.isoformat()}, {end.isoformat()})")
                    count += 1
        print(f"{count} planned request(s). Preview only; completed windows may be skipped during execution.")
        return 0

    import psycopg
    from acras import db
    from acras.api import AcrasClient
    from acras.extractors.flight import FlightExtractor
    from acras.extractors.meteo import MeteoExtractor
    from acras.extractors.reference import StationsExtractor, ZonesExtractor
    from acras.extractors.sonic import SonicExtractor

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    client = None
    try:
        zones = list(dict.fromkeys(str(UUID(zone)) for zone in args.zone_id))
        config = Config.from_env()
        if "sonic" in sources:
            try:
                import pyarrow  # noqa: F401
            except ImportError:
                raise RuntimeError("Install acoustic storage support: python -m pip install -e '.[storage]'")
        client = AcrasClient(
            config.api_key, config.base_url, timeout=config.http_timeout, max_retries=config.max_retries
        )
        with db.connect(config.database_url) as conn:
            db.run_migrations(conn)
            reference_types = [ZonesExtractor, StationsExtractor]
            for reference in reference_types:
                stats = reference(client, conn, config).run()
                if stats.failures:
                    raise RuntimeError("Reference metadata extraction failed; check the error log")
            verify_scopes(conn, zones)
            extractors = {"flight": FlightExtractor, "meteo": MeteoExtractor, "sonic": SonicExtractor}
            failures = 0
            for source in sources:
                scopes = stations if source == "sonic" else zones
                stats = extractors[source](client, conn, config).run(
                    scopes, args.start, args.end, resume=not args.no_resume
                )
                print(f"{source}: {stats}")
                failures += len(stats.failures)
            return 1 if failures else 0
    except psycopg.Error:
        # Connection strings can contain passwords; do not print exceptions
        # supplied by the database driver.
        print(
            "PostgreSQL connection or SQL operation failed. Check DATABASE_URL, "
            "start PostgreSQL 14+, and verify database permissions.",
            file=sys.stderr,
        )
        return 1
    except (ValueError, RuntimeError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        if client is not None:
            client.session.close()


if __name__ == "__main__":
    raise SystemExit(main())
