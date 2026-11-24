-- name: runs_jobs_detail_breakdowns(ts_start, ts_end, source_app, tenant_id)
-- Complete breakdowns at the same job-row grain as runs_jobs_detail.
SELECT GROUPING(r.tenant_id) AS is_source,
       (CASE WHEN GROUPING(r.tenant_id) = 0 THEN r.tenant_id ELSE r.source_app END) COLLATE "C" AS identifier,
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
 GROUP BY GROUPING SETS ((r.tenant_id), (r.source_app))
 ORDER BY is_source ASC, identifier ASC;
