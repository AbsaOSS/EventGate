-- name: get_runs_jobs_aggregations(ts_start, ts_end, source_app, tenant_id)
-- Window-wide summary aggregates for runs_jobs_detail (job-row grain).
SELECT COUNT(*) AS total_runs,
       COUNT(DISTINCT r.tenant_id) AS total_tenants,
       COUNT(DISTINCT j.catalog_id) AS total_catalogs,
       COUNT(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE j.status = 'failed') AS failed,
       COUNT(*) FILTER (WHERE j.status = 'killed') AS killed,
       COUNT(*) FILTER (WHERE j.status = 'skipped') AS skipped,
       AVG(j.timestamp_end - j.timestamp_start)
           FILTER (WHERE j.timestamp_start IS NOT NULL AND j.timestamp_end IS NOT NULL) AS avg_duration_ms
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
   AND (:source_app IS NULL OR r.source_app = :source_app)
   AND (:tenant_id IS NULL OR r.tenant_id = :tenant_id);

-- name: get_runs_jobs_timeline_hourly(ts_start, ts_end, source_app, tenant_id)
-- Hourly timeline buckets (non-empty buckets only) for runs_jobs_detail, oldest first.
SELECT (EXTRACT(EPOCH FROM date_trunc('hour', to_timestamp(r.timestamp_start / 1000.0), 'UTC')) * 1000)::bigint
           AS bucket_start,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE j.status = 'failed') AS failed,
       COUNT(*) FILTER (WHERE j.status = 'killed') AS killed,
       COUNT(*) FILTER (WHERE j.status = 'skipped') AS skipped
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
   AND (:source_app IS NULL OR r.source_app = :source_app)
   AND (:tenant_id IS NULL OR r.tenant_id = :tenant_id)
 GROUP BY bucket_start
 ORDER BY bucket_start ASC;

-- name: get_runs_jobs_timeline_daily(ts_start, ts_end, source_app, tenant_id)
-- Daily timeline buckets (non-empty buckets only) for runs_jobs_detail, oldest first.
SELECT (EXTRACT(EPOCH FROM date_trunc('day', to_timestamp(r.timestamp_start / 1000.0), 'UTC')) * 1000)::bigint
           AS bucket_start,
       COUNT(*) AS total,
       COUNT(*) FILTER (WHERE j.status = 'succeeded') AS succeeded,
       COUNT(*) FILTER (WHERE j.status = 'failed') AS failed,
       COUNT(*) FILTER (WHERE j.status = 'killed') AS killed,
       COUNT(*) FILTER (WHERE j.status = 'skipped') AS skipped
  FROM public_cps_za_runs_jobs j
 INNER JOIN public_cps_za_runs r ON j.event_id = r.event_id
 WHERE r.timestamp_start >= :ts_start AND r.timestamp_start <= :ts_end
   AND (:source_app IS NULL OR r.source_app = :source_app)
   AND (:tenant_id IS NULL OR r.tenant_id = :tenant_id)
 GROUP BY bucket_start
 ORDER BY bucket_start ASC;
