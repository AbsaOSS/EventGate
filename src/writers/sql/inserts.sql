-- name: insert_dlchange(event_id, tenant_id, source_app, source_app_version, environment, timestamp_event, country, catalog_id, operation, location, format, format_options, additional_info)!
-- Insert a dlchange-style event row.
INSERT INTO public_cps_za_dlchange
    (event_id, tenant_id, source_app, source_app_version, environment,
     timestamp_event, country, catalog_id, operation,
     "location", "format", format_options, additional_info)
VALUES
    (:event_id, :tenant_id, :source_app, :source_app_version, :environment,
     :timestamp_event, :country, :catalog_id, :operation,
     :location, :format, :format_options, :additional_info);

-- name: insert_run(event_id, job_ref, tenant_id, source_app, source_app_version, environment, timestamp_start, timestamp_end)!
-- Insert a run event row.
INSERT INTO public_cps_za_runs
    (event_id, job_ref, tenant_id, source_app, source_app_version,
     environment, timestamp_start, timestamp_end)
VALUES
    (:event_id, :job_ref, :tenant_id, :source_app, :source_app_version,
     :environment, :timestamp_start, :timestamp_end);

-- name: insert_run_job(event_id, country, catalog_id, status, timestamp_start, timestamp_end, message, additional_info)!
-- Insert a run job row.
INSERT INTO public_cps_za_runs_jobs
    (event_id, country, catalog_id, status,
     timestamp_start, timestamp_end, message, additional_info)
VALUES
    (:event_id, :country, :catalog_id, :status,
     :timestamp_start, :timestamp_end, :message, :additional_info);

-- name: insert_test(event_id, tenant_id, source_app, environment, timestamp_event, additional_info)!
-- Insert a test event row.
INSERT INTO public_cps_za_test
    (event_id, tenant_id, source_app, environment,
     timestamp_event, additional_info)
VALUES
    (:event_id, :tenant_id, :source_app, :environment,
     :timestamp_event, :additional_info);
