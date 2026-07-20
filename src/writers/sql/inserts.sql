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

-- name: upsert_status_change(job_id, job_group_id, parent_job_id, initial_job_id, job_ref, job_name, definition_id, definition_version, tenant_id, country, source_app, source_app_version, environment, platform, platform_metadata, input_arguments, additional_context, attempt_number, status_type, status_subtype, status_detail, created_at, started_at, finished_at, last_updated_at)!
-- Upsert a status_change event into the aggregated job table using merge logic per ADR 001.
INSERT INTO public_cps_za_status_change_aggregated_job AS t (
    job_id, job_group_id, parent_job_id, initial_job_id,
    job_ref, job_name, definition_id, definition_version,
    tenant_id, country, source_app, source_app_version, environment,
    platform, platform_metadata, input_arguments, additional_context,
    attempt_number, status_type, status_subtype, status_detail,
    created_at, started_at, finished_at, last_updated_at
)
VALUES (
    :job_id, :job_group_id, :parent_job_id, :initial_job_id,
    :job_ref, :job_name, :definition_id, :definition_version,
    :tenant_id, :country, :source_app, :source_app_version, :environment,
    :platform, :platform_metadata, :input_arguments, :additional_context,
    :attempt_number, :status_type, :status_subtype, :status_detail,
    :created_at, :started_at, :finished_at, :last_updated_at
)
ON CONFLICT (job_id) DO UPDATE SET
    job_group_id       = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.job_group_id, t.job_group_id)
                           ELSE COALESCE(t.job_group_id, EXCLUDED.job_group_id)
                         END,
    parent_job_id      = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.parent_job_id, t.parent_job_id)
                           ELSE COALESCE(t.parent_job_id, EXCLUDED.parent_job_id)
                         END,
    initial_job_id     = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.initial_job_id, t.initial_job_id)
                           ELSE COALESCE(t.initial_job_id, EXCLUDED.initial_job_id)
                         END,
    job_ref            = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.job_ref, t.job_ref)
                           ELSE COALESCE(t.job_ref, EXCLUDED.job_ref)
                         END,
    job_name           = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.job_name, t.job_name)
                           ELSE COALESCE(t.job_name, EXCLUDED.job_name)
                         END,
    definition_id      = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.definition_id, t.definition_id)
                           ELSE COALESCE(t.definition_id, EXCLUDED.definition_id)
                         END,
    definition_version = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.definition_version, t.definition_version)
                           ELSE COALESCE(t.definition_version, EXCLUDED.definition_version)
                         END,
    tenant_id          = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.tenant_id, t.tenant_id)
                           ELSE COALESCE(t.tenant_id, EXCLUDED.tenant_id)
                         END,
    country            = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.country, t.country)
                           ELSE COALESCE(t.country, EXCLUDED.country)
                         END,
    source_app         = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.source_app, t.source_app)
                           ELSE COALESCE(t.source_app, EXCLUDED.source_app)
                         END,
    source_app_version = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.source_app_version, t.source_app_version)
                           ELSE COALESCE(t.source_app_version, EXCLUDED.source_app_version)
                         END,
    environment        = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.environment, t.environment)
                           ELSE COALESCE(t.environment, EXCLUDED.environment)
                         END,
    platform           = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.platform, t.platform)
                           ELSE COALESCE(t.platform, EXCLUDED.platform)
                         END,
    platform_metadata  = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.platform_metadata, t.platform_metadata)
                           ELSE COALESCE(t.platform_metadata, EXCLUDED.platform_metadata)
                         END,
    input_arguments    = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.input_arguments, t.input_arguments)
                           ELSE COALESCE(t.input_arguments, EXCLUDED.input_arguments)
                         END,
    additional_context = CASE
                           WHEN EXCLUDED.additional_context IS NULL THEN t.additional_context
                           WHEN t.additional_context IS NULL        THEN EXCLUDED.additional_context
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN t.additional_context || EXCLUDED.additional_context
                           ELSE EXCLUDED.additional_context || t.additional_context
                         END,
    attempt_number     = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(NULLIF(EXCLUDED.attempt_number, 1), t.attempt_number, 1)
                           ELSE COALESCE(NULLIF(t.attempt_number, 1), EXCLUDED.attempt_number, 1)
                         END,
    status_type        = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN EXCLUDED.status_type
                           ELSE t.status_type
                         END,
    status_subtype     = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN EXCLUDED.status_subtype
                           ELSE t.status_subtype
                         END,
    status_detail      = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN EXCLUDED.status_detail
                           ELSE t.status_detail
                         END,
    created_at         = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.created_at, t.created_at)
                           ELSE COALESCE(t.created_at, EXCLUDED.created_at)
                         END,
    started_at         = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.started_at, t.started_at)
                           ELSE COALESCE(t.started_at, EXCLUDED.started_at)
                         END,
    finished_at        = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.finished_at, t.finished_at)
                           ELSE t.finished_at
                         END,
    last_updated_at    = GREATEST(EXCLUDED.last_updated_at, t.last_updated_at);
