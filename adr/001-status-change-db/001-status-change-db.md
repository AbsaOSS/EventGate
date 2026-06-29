# ADR 001. Event Bus -- Status change database

## Decision

## Background
The status change topic receives job status events, i.e. it is an event log
For monitoring, we need a DB with the current status, i.e. we need to merge
events into the latest state

This ADR shall describe the merge logic as well as the database schema and some of the queries that the database should support.

## Database schema

```sql
CREATE TABLE job (
    job_id               UUID PRIMARY KEY,
    job_group_id         UUID,
    parent_job_id        UUID,
    initial_job_id       UUID,

    -- Identity / business attributes
    job_ref              TEXT,
    job_name             TEXT,
    definition_id        TEXT,
    definition_version   TEXT,
    tenant_id            TEXT,
    country              TEXT,
    source_app           TEXT,
    source_app_version   TEXT,
    environment          TEXT NOT NULL,

    -- Execution context (latest known snapshot)
    platform             TEXT,
    platform_metadata    JSONB,
    input_arguments      JSONB,
    additional_context   JSONB,

    -- Current lifecycle status (latest known snapshot)
    attempt_number       INTEGER NOT NULL CHECK (attempt_number > 0),
    status_type          TEXT CHECK (status_type IN ('WAITING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED')),
    status_subtype       TEXT,
    status_detail        TEXT,

    -- Lifecycle timestamps derived from events
    created_at           TIMESTAMPTZ,
    started_at           TIMESTAMPTZ,
    finished_at          TIMESTAMPTZ,
    last_updated_at      TIMESTAMPTZ NOT NULL,

   CONSTRAINT fk_job_parent
       FOREIGN KEY (parent_job_id)
           REFERENCES job (job_id)
           ON DELETE RESTRICT,
    -- Prevent self-parenting
   CONSTRAINT chk_no_self_parent
       CHECK (parent_job_id IS NULL OR parent_job_id <> job_id),


   CONSTRAINT fk_job_initial
       FOREIGN KEY (initial_job_id)
           REFERENCES job (job_id)
           ON DELETE RESTRICT,
    -- Prevent self-retry
   CONSTRAINT chk_no_self_initial
       CHECK (initial_job_id IS NULL OR initial_job_id <> job_id)
);
```

Notes:
- This table stores the latest merged state per `job_id`, not the full event log.

## Merge logic

### Assumptions
- Most events are received in order per `job_id`, but some events may be received out-of-order (during failures scenarios).
- Duplicate events can occur and must be handled idempotently.
- The `job` table stores the latest snapshot, not historical versions.
- No concurrent inserts / updates?

### Field merge policy
- Structural identifiers (`job_group_id`, `parent_job_id`, `initial_job_id`) are set when present in the event.
- Descriptive fields (`job_ref`, `job_name`, `definition_id`, `definition_version`, `tenant_id`, `country`, `source_app`, `source_app_version`, `platform`) are updated only when incoming values are non-null.
- The JSON fields `platform_metadata`, `input_arguments` are replaced by the latest non-null value.
- The JSON field `additional_context` is merged in a shallow way, i.e. top-level fields are merged, while nested fields will be replaced.
- `attempt_number` is updated to the latest non-null value; if missing on insert, default to `1`.
- `status_type`, `status_subtype`, and `status_detail` are updated from the latest event when provided.
- `last_updated_at` is set to the incoming `timestamp_event` on every successful merge.

### Out-of-order handling
- The constant fields (`job_ref`, `job_name`, `definition_id`, `definition_version`, `tenant_id`, `country`, `source_app`, `source_app_version`, `platform`) are assumed to be constant per job-id, therefore they should be updated if they are not null.
- The variable fields `platform_metadata` and `input_arguments` should only be updated if the event timestamp is newer than the event timestamp in the database
- The variable field `additional_context` will always be merged, where the value with the newer timestamp overrides older timestamps in case of key conflicts
- `status_type`, `status_subtype`, `status_detail`, `last_updated_at` are only updated if the incoming timestamp is  newer than the existing one
- Any field updates should only be performed if the new timestamp

### Event-type update rules

| event_type | Core updates |
| --- | --- |
| JobCreatedEvent | Set `created_at`, set `status_type` to `WAITING` when not provided. |
| JobCreatedAndStartedEvent | Set `created_at`, set `started_at`, set `status_type` to `RUNNING` when not provided. |
| JobStartedEvent | Ensure row exists, set `started_at` if null, set `status_type` to `RUNNING` when not provided. |
| JobUpdatedEvent | Ensure row exists, merge provided fields only, set `updated_at`. |
| JobFinishedEvent | Ensure row exists, set `finished_at` if null, set terminal `status_type` (`SUCCEEDED`, `FAILED`, `KILLED`), update `status_subtype` and `status_detail` when provided. |

### Timestamp conversion
Timestamps in events are epoch milliseconds, which should be converted to `TIMESTAMPTZ` in Postgres. Using a dedicated timestamp type greatly simplifies querying and reading from the table, especially in BI-tools.

### Idempotency for duplicates
Idempotency is achieved directly in the `job` upsert logic.

Processing rule:
- For duplicate events with the same payload, the merge computes the same target state.


## Sample query patterns
**Get all jobs grouped by job group id (including retries)**
```sql
SELECT
    j.*,
    MIN(started_at) OVER (PARTITION BY job_group_id) AS group_started_at
FROM job j
ORDER BY
    group_started_at,
    job_group_id,
    started_at;
```

**Get job hierarchy roots, including only latest attempt**
```sql
WITH latest_root_attempts AS (
    SELECT *
    FROM (
             SELECT
                 j.*,
                 ROW_NUMBER() OVER (
                     PARTITION BY COALESCE(initial_job_id, job_id)
                     ORDER BY attempt_number DESC, started_at DESC
                     ) AS rn
             FROM job j
             WHERE job_name = 'Aqueduct Ingestion'
         ) t
    WHERE rn = 1
)
SELECT
    *
FROM latest_root_attempts
ORDER BY
    job_group_id,
    started_at;
```    

**Get all jobs in a job hierarchy given a specific job group id, including only latest attempts**
```sql
SELECT *
FROM (
         SELECT
             j.*,
             ROW_NUMBER() OVER (
                 PARTITION BY COALESCE(initial_job_id, job_id)
                 ORDER BY attempt_number DESC, started_at DESC
                 ) AS rn
         FROM job j
         WHERE job_group_id = '11111111-1111-4111-8111-111111111111'::uuid
     ) t
WHERE rn = 1;
```

**Get all jobs in all job hierarchies, including only latest attempts**
```sql
WITH RECURSIVE latest_attempts AS (
    SELECT *
    FROM (
             SELECT
                 j.*,
                 ROW_NUMBER() OVER (
                     PARTITION BY COALESCE(initial_job_id, job_id)
                     ORDER BY attempt_number DESC, started_at DESC
                     ) AS rn
             FROM job j
         ) t
    WHERE rn = 1
),
latest_root AS (
    SELECT job_id
    FROM latest_attempts
    WHERE parent_job_id IS NULL
    ORDER BY attempt_number DESC, started_at DESC
    LIMIT 1
),
tree AS (
    -- start from latest root
    SELECT la.*
    FROM latest_attempts la
            JOIN latest_root r ON la.job_id = r.job_id

    UNION ALL

    -- traverse hierarchy
    SELECT child.*
    FROM latest_attempts child
            JOIN tree parent
                    ON child.parent_job_id = parent.job_id
)
SELECT *
FROM tree
ORDER BY started_at;
```


## Appendix A. SQL-like merge pseudocode
Please note that the following is only meant to illustrate the merge logic. In reality, the upsert query may have suboptimal performance and should be replaced by dedicated insert and update queries, depending on the event type.


```sql
BEGIN;
INSERT INTO job (
    job_id,
    job_group_id,
    parent_job_id,
    initial_job_id,
    job_ref,
    job_name,
    definition_id,
    definition_version,
    tenant_id,
    country,
    source_app,
    source_app_version,
    environment,
    platform,
    platform_metadata,
    input_arguments,
    additional_context,
    attempt_number,
    status_type,
    status_subtype,
    status_detail,
    created_at,
    started_at,
    finished_at,
    last_updated_at
)
VALUES (
    :job_id,
    :job_group_id,
    :parent_job_id,
    :initial_job_id,
    :job_ref,
    :job_name,
    :definition_id,
    :definition_version,
    :tenant_id,
    :country,
    :source_app,
    :source_app_version,
    :environment,
    :platform,
    :platform_metadata,
    :input_arguments,
    :additional_context,
    COALESCE(:attempt_number, 1),
    CASE
        WHEN :event_type = 'JobFinishedEvent' THEN :status_type
        WHEN :event_type IN ('JobCreatedAndStartedEvent', 'JobStartedEvent') THEN COALESCE(:status_type, 'RUNNING')
        WHEN :event_type = 'JobCreatedEvent' THEN COALESCE(:status_type, 'WAITING')
        ELSE :status_type
    END,
    :status_subtype,
    :status_detail,
    CASE WHEN :event_type IN ('JobCreatedEvent', 'JobCreatedAndStartedEvent') THEN to_timestamp(:timestamp_event / 1000.0) END,
    CASE WHEN :event_type IN ('JobCreatedAndStartedEvent', 'JobStartedEvent') THEN to_timestamp(:timestamp_event / 1000.0) END,
    CASE WHEN :event_type = 'JobFinishedEvent' THEN to_timestamp(:timestamp_event / 1000.0) END,
    to_timestamp(:timestamp_event / 1000.0)
)
ON CONFLICT (job_id) DO UPDATE SET
    job_group_id       = COALESCE(EXCLUDED.job_group_id, job.job_group_id),
    parent_job_id      = COALESCE(EXCLUDED.parent_job_id, job.parent_job_id),
    initial_job_id     = COALESCE(EXCLUDED.initial_job_id, job.initial_job_id),
    job_ref            = COALESCE(EXCLUDED.job_ref, job.job_ref),
    job_name           = COALESCE(EXCLUDED.job_name, job.job_name),
    definition_id      = COALESCE(EXCLUDED.definition_id, job.definition_id),
    definition_version = COALESCE(EXCLUDED.definition_version, job.definition_version),
    tenant_id          = COALESCE(EXCLUDED.tenant_id, job.tenant_id),
    country            = COALESCE(EXCLUDED.country, job.country),
    source_app         = COALESCE(EXCLUDED.source_app, job.source_app),
    source_app_version = COALESCE(EXCLUDED.source_app_version, job.source_app_version),
    environment        = COALESCE(EXCLUDED.environment, job.environment),
    platform           = COALESCE(EXCLUDED.platform, job.platform),
    platform_metadata  = COALESCE(EXCLUDED.platform_metadata, job.platform_metadata),
    input_arguments    = COALESCE(EXCLUDED.input_arguments, job.input_arguments),
    additional_context = CASE
        WHEN EXCLUDED.additional_context IS NULL THEN job.additional_context
        ELSE COALESCE(job.additional_context, '{}'::jsonb) || EXCLUDED.additional_context
    END,
    attempt_number     = COALESCE(EXCLUDED.attempt_number, job.attempt_number),
    status_type        = COALESCE(EXCLUDED.status_type, job.status_type),
    status_subtype     = COALESCE(EXCLUDED.status_subtype, job.status_subtype),
    status_detail      = COALESCE(EXCLUDED.status_detail, job.status_detail),
    created_at         = COALESCE(job.created_at, EXCLUDED.created_at),
    started_at         = COALESCE(job.started_at, EXCLUDED.started_at),
    finished_at        = COALESCE(job.finished_at, EXCLUDED.finished_at),
    last_updated_at    = to_timestamp(:timestamp_event / 1000.0)
COMMIT;
```

