# AI-ACRAS-project

Helicopter detection and type classification using processed ACRAS observations
(EPFL, IAV, and armasuisse).

**Project station: ACRAS2-8 only.** Acoustic extraction defaults to this station
and rejects other station names. The station registry response is filtered before
storage so only ACRAS2-8 metadata is imported. Flight and weather remain zone-based;
every selected zone is checked against ACRAS2-8's coordinates before extraction.

## Extraction status

Flight, weather, acoustic, and reference extractors now have implemented query
adapters and response mappings. The command supports offline previews and explicit
download execution. Persistence requires PostgreSQL 14+ and a valid API key.

The mappings use observed flight/weather responses, the documented acoustic
response plus the successful station test, and the supplied station-registry
response. Physical frequency/angle axes, scaling, and acoustic labels remain
outside this ingestion implementation.

## Structure

```text
src/acras/
  config.py              Environment settings and window sizes
  api.py                 HTTP client, authentication, UTC bounds, retries
  db.py                  PostgreSQL, migrations, committed coverage
  extractors/
    base.py              Shared lifecycle, transactions, gap-aware resume
    common.py            Shared payload and query validation
    flight.py            Flight records and aircraft metadata
    meteo.py             Weather records, raw JSON content, units
    sonic.py             Six acoustic series and hourly file manifests
    reference.py         /zones and /station-registry metadata
  storage/
    parquet.py           Immutable merged hourly acoustic files
scripts/
  download_data.py       Offline preview or explicit extraction
  initial-api-testing.py Original standalone smoke test
sql/
  001_schema.sql         Original warehouse schema
  002_ingest_gap_coverage.sql Gap detection after differently sized retries
data/sonic/              Local acoustic files, excluded from Git
tests/                   Synthetic offline and optional PostgreSQL checks
compose.yaml             Optional local PostgreSQL service
```

## Setup

Use Python 3.10 or newer:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev,storage,analysis]"
```

Use the editable checkout so migrations can find the root SQL directory.
The storage extra installs PyArrow; dev installs pytest.

Create a local configuration only if `.env` does not already exist:

```bash
cp -n .env.example .env
```

Edit `.env` locally: set a valid replacement API key, the zone ID obtained from
`/zones`, the project station name `ACRAS2-8`, and `DATABASE_URL`. Never
share or commit the API key. Revoke any key exposed in a screenshot, curl command,
or chat, and do not reuse it for further requests.

Load the saved values into the shell; configuration does not load `.env` itself:

```bash
set -a
source .env
set +a
```

Use an existing PostgreSQL 14+ server or, if Docker with Compose is installed and
running, start the supplied local database:

```bash
docker compose up -d db
```

It listens only on localhost port 5432 and matches the template's development URL
(username/password `acras`/`acras`). Data persists in a Docker volume. If customizing
`POSTGRES_PASSWORD`, update `DATABASE_URL` too; environment changes do not reset an
existing database's password.

## Preview and execute

A preview requires neither credentials nor database connectivity:

```bash
python scripts/download_data.py \
  --source all \
  --start 2025-08-08T10:00:00Z \
  --end 2025-08-08T11:00:00Z \
  --zone-id YOUR_ZONE_ID \
  --station-name ACRAS2-8 \
  --dry-run
```

Replace the zone placeholder with your verified ID. Repeat `--zone-id` only for
additional zones covering ACRAS2-8. The station is fixed: `--station-name ACRAS2-8`
is optional and other station values are rejected. This example plans eight
requests before skipping committed coverage.

After setup, download a small first sample:

```bash
python scripts/download_data.py \
  --source all \
  --start 2025-08-08T10:21:20Z \
  --end 2025-08-08T10:21:30Z \
  --zone-id "$ACRAS_ZONE_ID" \
  --station-name "$ACRAS_STATION_NAME" \
  --execute
```

The command applies migrations, refreshes references, validates selectors, and
runs the extractors. Every flight/weather run checks that ACRAS2-8's coordinates
lie within each selected zone's bounds, including runs of just one source. Zone
observations may still cover the surrounding region; they are not measurements
from another acoustic station. Geographic containment does not prove an official
assignment or aircraft audibility.

Use `flight`, `meteo`, or `sonic` for one source. Successful coverage is skipped
by default; uncovered intervals, including earlier failures, are retried.
`--no-resume` re-fetches even successful periods. Failures produce a nonzero exit
status.

The ten-second historical sample had flight/acoustic observations but no weather
records. A larger surrounding weather interval returned observations. Empty
weather responses are not request failures or zero-valued weather.

## Time windows and duplicates

| Source | Location parameter | API maximum | Configured window |
| --- | --- | --- | --- |
| Flight | zoneId | 1 hour | 1 hour |
| Weather | zoneId | 1 day | 6 hours |
| Acoustic | stationName | 1 hour | 10 minutes |

Bounds must include a timezone and use whole seconds. Internal windows are
`[start, end)`. Requests send the full `to` bound, then adapters exclude observations
at or beyond the logical end. This handles inclusive responses without dropping
subsecond flight observations. Timestamps are stored in UTC; acoustic
`startTime` values are epoch milliseconds.

Flight hashes include the zone and persisted source fields, excluding photo URLs.
Weather uses the source ID and preserves separate spatial grid points. Data and
the success audit commit together.

## Acoustic files and recovery

Each station/series/hour has one current database manifest entry. New batches
merge with its prior Parquet version, sorting and deduplicating timestamps.
Conflicting values for an existing timestamp or changing array widths within an
hour fail rather than silently overwrite measurements.

Files have content-based names beneath
`data/sonic/<station>/<series>/<year>/<month>/<day>/<hour>/` and preserve numeric
arrays and UTC millisecond timestamps without assuming physical units or axes.
Database transaction locks serialize merges of the same station/series/hour.

A new immutable file is atomically installed before its manifest commits.
If the transaction fails, the old manifest still references an unchanged file.
Unreferenced versions can remain; automatic deletion is not part of ingestion.

Resume uses successful interval coverage, not only a latest timestamp.
`ingest_gap` identifies failed intervals not yet covered by successful retries,
including retries split into smaller windows.

## Tests and remaining validation

```bash
python -m pytest -q
```

Offline tests use synthetic responses and temporary files. They cover selectors,
limits, UTC/time boundaries, hashing, JSON units, array validation, file merging,
rollback-safe file versions, and gap-aware resume.

Optional PostgreSQL tests use a disposable database and create/remove unique
schemas. They never call ACRAS:

```bash
ACRAS_TEST_DATABASE_URL="$DATABASE_URL" python -m pytest tests/test_database.py -q
```

Use only a development/test database with schema and extension permissions.
These tests skip when that variable is unset. Live persistence cannot be claimed
verified until PostgreSQL is available and those checks have run.

No helicopter labels are inferred. The original schema's `is_helicopter` fields
retain their existing defaults; these do not establish negative acoustic labels.
Validate labeling separately before training. Keep API keys and restricted data
outside Git.
