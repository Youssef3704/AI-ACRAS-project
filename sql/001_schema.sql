-- ACRAS data warehouse schema.
--
-- Design notes:
--   * Dimensions (zone, station, aircraft) are small and upserted on every run.
--   * flight_data and meteo_data are relational facts and live here in full.
--   * Sonic spectrogram/azimuth/elevation arrays do NOT live here: at 1451 int16
--     values per second per station (~0.6 GB/day/station as JSON) they go to
--     Parquet under $ACRAS_DATA_DIR, and this DB keeps only the manifest in
--     sonic_frame_file. Read them by joining the manifest to the files.
--   * No ORM. Plain SQL, applied in filename order by src/acras/db.py.

create extension if not exists pgcrypto;  -- digest() for row hashing

-- ---------------------------------------------------------------- dimensions

create table if not exists zone (
    id            uuid primary key,
    name          text not null,
    active        boolean,
    account_id    bigint,
    account_name  text,
    lat_min       double precision,
    lat_max       double precision,
    lng_min       double precision,
    lng_max       double precision,
    ingested_at   timestamptz not null default now()
);

create table if not exists station (
    id           bigint primary key,
    name         text not null unique,
    latitude     double precision,
    longitude    double precision,
    altitude     double precision,
    nb_sensors   integer,
    sample_rate  integer,
    ingested_at  timestamptz not null default now()
);

-- One row per airframe. Exists because `category` is intermittent in the feed:
-- the AS350 HB-ZZO reports category 'A7' on only ~45% of its messages and NULL
-- on the rest, so per-message filtering silently drops positive labels.
-- is_helicopter is resolved per icao24 from every message ever seen.
-- (The feed's category 'A7' is ADS-B "Rotorcraft", which in this airspace is
-- helicopters; icao_type_code is the authoritative cross-check.)
create table if not exists aircraft (
    icao24          text primary key,
    registration    text,
    icao_type_code  text,
    best_category   text,
    is_helicopter   boolean not null default false,
    first_seen      timestamptz,
    last_seen       timestamptz,
    updated_at      timestamptz not null default now()
);

create index if not exists aircraft_helicopter_idx
    on aircraft (is_helicopter) where is_helicopter;

-- --------------------------------------------------------------------- facts

-- The feed has no stable primary key: ~3 rows/hour repeat
-- (icao24, date_time, source) exactly. row_hash over the payload gives
-- idempotent re-ingest without collapsing genuinely distinct messages.
-- photoUrl is deliberately not stored (signed URL, expires, not useful).
create table if not exists flight_data (
    id                       bigserial primary key,
    row_hash                 bytea not null,
    zone_id                  uuid not null references zone (id),
    icao24                   text not null,
    date_time                timestamptz not null,
    source                   text,
    source_id                text,
    callsign                 text,
    position_date_time       timestamptz,
    last_contact_date_time   timestamptz,
    latitude                 double precision,
    longitude                double precision,
    baro_altitude_m          double precision,
    geo_altitude_m           double precision,
    ground_speed_ms          double precision,
    true_track_deg           double precision,
    vertical_rate_ms         double precision,
    on_ground                boolean,
    squawk                   text,
    spi                      boolean,
    position_source          text,
    category                 text,
    registration             text,
    icao_type_code           text,
    is_helicopter            boolean not null default false,
    ingested_at              timestamptz not null default now()
);

create unique index if not exists flight_data_row_hash_idx on flight_data (row_hash);
create index if not exists flight_data_zone_time_idx  on flight_data (zone_id, date_time);
create index if not exists flight_data_icao_time_idx  on flight_data (icao24, date_time);
-- Partial index: the label query ("was a helicopter airborne at time t?") only
-- ever touches these rows, which are a few percent of the table.
create index if not exists flight_data_helicopter_idx
    on flight_data (date_time) where is_helicopter;

-- Meteo keeps the raw open-meteo payload in jsonb, with the fields we actually
-- model as generated columns so queries stay plain SQL and nothing can drift
-- out of sync with the source blob.
create table if not exists meteo_data (
    id                uuid primary key,
    kind              text,
    zone_id           uuid references zone (id),
    station_id        bigint,
    date_time         timestamptz not null,
    latitude          double precision,
    longitude         double precision,
    success           boolean,
    http_status_code  integer,
    content           jsonb not null,
    ingested_at       timestamptz not null default now(),

    temperature_2m        double precision
        generated always as ((content -> 'current' ->> 'temperature_2m')::double precision) stored,
    apparent_temperature  double precision
        generated always as ((content -> 'current' ->> 'apparent_temperature')::double precision) stored,
    relative_humidity_2m  double precision
        generated always as ((content -> 'current' ->> 'relative_humidity_2m')::double precision) stored,
    pressure_msl          double precision
        generated always as ((content -> 'current' ->> 'pressure_msl')::double precision) stored,
    surface_pressure      double precision
        generated always as ((content -> 'current' ->> 'surface_pressure')::double precision) stored,
    wind_speed_10m        double precision
        generated always as ((content -> 'current' ->> 'wind_speed_10m')::double precision) stored,
    wind_gusts_10m        double precision
        generated always as ((content -> 'current' ->> 'wind_gusts_10m')::double precision) stored,
    wind_direction_10m    double precision
        generated always as ((content -> 'current' ->> 'wind_direction_10m')::double precision) stored,
    precipitation         double precision
        generated always as ((content -> 'current' ->> 'precipitation')::double precision) stored,
    rain                  double precision
        generated always as ((content -> 'current' ->> 'rain')::double precision) stored,
    snowfall              double precision
        generated always as ((content -> 'current' ->> 'snowfall')::double precision) stored,
    cloud_cover           double precision
        generated always as ((content -> 'current' ->> 'cloud_cover')::double precision) stored,
    weather_code          integer
        generated always as ((content -> 'current' ->> 'weather_code')::integer) stored,
    is_day                integer
        generated always as ((content -> 'current' ->> 'is_day')::integer) stored
);

create index if not exists meteo_data_zone_time_idx on meteo_data (zone_id, date_time);
-- The feed returns a 5x5 grid per timestamp, so point lookups need the location.
create index if not exists meteo_data_time_point_idx on meteo_data (date_time, latitude, longitude);

-- ------------------------------------------------- sonic (Parquet + manifest)

-- One row per (station, series, hour) Parquet part file. path is relative to
-- $ACRAS_DATA_DIR so the manifest stays valid inside and outside Docker.
create table if not exists sonic_frame_file (
    station_name      text not null references station (name),
    series            text not null,
    hour_start        timestamptz not null,
    path              text not null,
    n_frames          integer not null,
    values_per_frame  integer not null,
    bytes             bigint,
    written_at        timestamptz not null default now(),
    primary key (station_name, series, hour_start)
);

create index if not exists sonic_frame_file_time_idx on sonic_frame_file (hour_start);

-- ------------------------------------------------------------------ bookkeeping

-- Per-window audit trail: what ran, how much it moved, and what failed.
-- This is the single source of truth for progress; the resume point is derived
-- from it (see the ingest_progress view) rather than stored separately, so the
-- two can never disagree after a crash.
create table if not exists ingest_run (
    id           bigserial primary key,
    extractor    text not null,
    scope_key    text,
    window_from  timestamptz,
    window_to    timestamptz,
    rows_seen    integer not null default 0,
    rows_written integer not null default 0,
    status       text not null,
    error        text,
    started_at   timestamptz not null default now(),
    finished_at  timestamptz
);

create index if not exists ingest_run_extractor_idx on ingest_run (extractor, started_at desc);
-- Serves the resume-point lookup below.
create index if not exists ingest_run_progress_idx
    on ingest_run (extractor, scope_key, window_to) where status = 'ok';

-- Resume point per extractor per scope (zone id or station name), derived from
-- the committed runs. An interrupted multi-week backfill continues from here
-- instead of restarting.
--
-- Caveat: window_end is a high-water mark, so it does not notice a hole left by
-- a window that failed while later ones succeeded. Use ingest_gap for that.
create or replace view ingest_progress as
select extractor,
       scope_key,
       max(window_to)  as window_end,
       count(*)        as ok_runs,
       sum(rows_seen)  as rows_seen,
       sum(rows_written) as rows_written,
       max(finished_at) as last_success_at
  from ingest_run
 where status = 'ok' and window_to is not null
 group by extractor, scope_key;

-- Windows that failed and were never retried successfully: the actual holes in
-- the dataset. This is the query to run before trusting a date range.
create or replace view ingest_gap as
select f.extractor, f.scope_key, f.window_from, f.window_to,
       count(*) as failed_attempts,
       max(f.error) as last_error
  from ingest_run f
 where f.status = 'failed'
   and not exists (
       select 1 from ingest_run ok
        where ok.status = 'ok'
          and ok.extractor = f.extractor
          and ok.scope_key is not distinct from f.scope_key
          and ok.window_from = f.window_from
          and ok.window_to   = f.window_to
   )
 group by f.extractor, f.scope_key, f.window_from, f.window_to;
