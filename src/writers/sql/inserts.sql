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
                           ELSE t.job_group_id
                         END,
    parent_job_id      = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.parent_job_id, t.parent_job_id)
                           ELSE t.parent_job_id
                         END,
    initial_job_id     = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.initial_job_id, t.initial_job_id)
                           ELSE t.initial_job_id
                         END,
    job_ref            = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.job_ref, t.job_ref)
                           ELSE t.job_ref
                         END,
    job_name           = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.job_name, t.job_name)
                           ELSE t.job_name
                         END,
    definition_id      = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.definition_id, t.definition_id)
                           ELSE t.definition_id
                         END,
    definition_version = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.definition_version, t.definition_version)
                           ELSE t.definition_version
                         END,
    tenant_id          = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.tenant_id, t.tenant_id)
                           ELSE t.tenant_id
                         END,
    country            = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.country, t.country)
                           ELSE t.country
                         END,
    source_app         = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.source_app, t.source_app)
                           ELSE t.source_app
                         END,
    source_app_version = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.source_app_version, t.source_app_version)
                           ELSE t.source_app_version
                         END,
    environment        = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at AND EXCLUDED.environment <> ''
                           THEN EXCLUDED.environment
                           ELSE t.environment
                         END,
    platform           = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.platform, t.platform)
                           ELSE t.platform
                         END,
    platform_metadata  = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.platform_metadata, t.platform_metadata)
                           ELSE t.platform_metadata
                         END,
    input_arguments    = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.input_arguments, t.input_arguments)
                           ELSE t.input_arguments
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
                           THEN COALESCE(EXCLUDED.attempt_number, t.attempt_number)
                           ELSE t.attempt_number
                         END,
    status_type        = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.status_type, t.status_type)
                           ELSE t.status_type
                         END,
    status_subtype     = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.status_subtype, t.status_subtype)
                           ELSE t.status_subtype
                         END,
    status_detail      = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.status_detail, t.status_detail)
                           ELSE t.status_detail
                         END,
    created_at         = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.created_at, t.created_at)
                           ELSE t.created_at
                         END,
    started_at         = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.started_at, t.started_at)
                           ELSE t.started_at
                         END,
    finished_at        = CASE
                           WHEN EXCLUDED.last_updated_at >= t.last_updated_at
                           THEN COALESCE(EXCLUDED.finished_at, t.finished_at)
                           ELSE t.finished_at
                         END,
    last_updated_at    = GREATEST(EXCLUDED.last_updated_at, t.last_updated_at);
