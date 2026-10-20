-- name: get_runs_jobs_detail(ts_start, ts_end, lim)
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

-- name: get_runs_jobs_detail_with_cursor(ts_start, ts_end, cursor_id, lim)
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

-- name: get_runs_jobs_aggregations(ts_start, ts_end)
-- Window-wide summary aggregates for runs_jobs_detail (job-row grain).
SELECT COUNT(*) AS total_runs,
       COUNT(DISTINCT r.tenant_id) AS total_tenants,
       COUNT(DISTINCT j.catalog_id) AS total_catalogs,
       COUNT(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE j.status = 'failed') AS failed,
       AVG(j.timestamp_end - j.timestamp_start)
           FILTER (WHERE j.timestamp_start IS NOT NULL AND j.timestamp_end IS NOT NULL) AS avg_duration_ms
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end;

-- name: get_runs_jobs_timeline_hourly(ts_start, ts_end)
-- Hourly timeline buckets (non-empty buckets only) for runs_jobs_detail, oldest first.
SELECT (EXTRACT(EPOCH FROM date_trunc('hour', to_timestamp(r.timestamp_start / 1000.0))) * 1000)::bigint
           AS bucket_start,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE j.status = 'failed') AS failed
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
 GROUP BY bucket_start
 ORDER BY bucket_start ASC;

-- name: get_runs_jobs_timeline_daily(ts_start, ts_end)
-- Daily timeline buckets (non-empty buckets only) for runs_jobs_detail, oldest first.
SELECT (EXTRACT(EPOCH FROM date_trunc('day', to_timestamp(r.timestamp_start / 1000.0))) * 1000)::bigint
           AS bucket_start,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE j.status = 'failed') AS failed
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
 GROUP BY bucket_start
 ORDER BY bucket_start ASC;
