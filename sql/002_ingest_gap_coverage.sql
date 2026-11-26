-- PostgreSQL 14+: a failed interval is repaired even when later successful
-- requests cover it in several smaller windows. Keep the original view columns.
create or replace view ingest_gap as
with coverage as (
    select extractor, scope_key,
           range_agg(tstzrange(window_from, window_to, '[)')) as windows
      from ingest_run
     where status = 'ok' and window_from is not null and window_to is not null
     group by extractor, scope_key
)
select f.extractor, f.scope_key, f.window_from, f.window_to,
       count(*) as failed_attempts,
       max(f.error) as last_error
  from ingest_run f
  left join coverage c on c.extractor = f.extractor
                      and c.scope_key is not distinct from f.scope_key
 where f.status = 'failed'
   and f.window_from is not null and f.window_to is not null
   and not (coalesce(c.windows, '{}'::tstzmultirange)
            @> tstzrange(f.window_from, f.window_to, '[)'))
 group by f.extractor, f.scope_key, f.window_from, f.window_to;
