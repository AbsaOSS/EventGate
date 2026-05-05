-- name: get_stats(ts_start, ts_end, lim)
-- Get run/job statistics with keyset pagination.
SELECT r.event_id, r.job_ref, r.tenant_id, r.source_app,
       r.source_app_version, r.environment,
       r.timestamp_start AS run_timestamp_start,
       r.timestamp_end AS run_timestamp_end,
       j.internal_id, j.country, j.catalog_id, j.status,
       j.timestamp_start, j.timestamp_end, j.message, j.additional_info
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
 ORDER BY j.internal_id DESC
 LIMIT :lim;

-- name: get_stats_with_cursor(ts_start, ts_end, cursor_id, lim)
-- Get run/job statistics with cursor-based keyset pagination.
SELECT r.event_id, r.job_ref, r.tenant_id, r.source_app,
       r.source_app_version, r.environment,
       r.timestamp_start AS run_timestamp_start,
       r.timestamp_end AS run_timestamp_end,
       j.internal_id, j.country, j.catalog_id, j.status,
       j.timestamp_start, j.timestamp_end, j.message, j.additional_info
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
   AND j.internal_id < :cursor_id
 ORDER BY j.internal_id DESC
 LIMIT :lim;
