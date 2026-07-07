# ADR 001. Event Bus -- Status change database

## Decision
Aggregate events and insert into newly created database table.

## Background
The status change topic receives job status events, i.e. it is an event log.
For monitoring, a DB with the current status is needed, i.e. events need to be aggregated into the latest state

This ADR shall describe the database schema as well as the merge logic and some of the queries that the database should support.

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
    environment          TEXT,

    -- Execution context (latest known snapshot)
    platform             TEXT,
    platform_metadata    JSONB,
    input_arguments      JSONB,
    additional_context   JSONB,
    attempt_number       INTEGER NOT NULL CHECK (attempt_number > 0),

    -- Current lifecycle status (latest known snapshot)
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

## Event insertion and aggregation

### Assumptions
- Most events are received in order per `job_id`, but some events may be received out-of-order (during failures scenarios).
- Duplicate events can occur and must be handled idempotently.
- The `job` table stores the latest snapshot, not historical versions.

### Field merge strategy
The field merge strategy takes into account out-of-order updates and the idempotency of duplicates. The true event order is determined by the field `last_updated_at`. There are three merge strategies:
- Take latest non-null: If either the incoming or the current field value is non-null, then the non-null value is accepted. If both incoming and current value are non-null, then the newer value (determined by `last_updated_at`) is accepted. This means that a value that has been set, will never be set to null again. This merge strategy applies to most fields.

- Take latest: The newer value is accepted, even if it is null. This applies to the `status_subtype` and `status_detail` fields, as they are coupled to the `status_type` field, which is mandatory for all events. If an initial status like `RUNNING` has a `status_detail` (for whatever reason), then it should not be kept when the status changes to `FINISHED`, but instead set to null. However, usually only terminal statuses should have `status_detail` and `status_subtype`

- Take latest non-default: For fields with default values, default values are treated like null values in terms of the merge. This means that non-default values are preferred, even if they are not the latest value. Once a value different to the default has been set, it is not possible to set it back to the default value. This applies to the field `attempt_number`.

- Cumulative merge: For `additional_context`, the merge strategy is a cumulative merge, i.e. fields from both the incoming and current record are retained. Note that this is not the case for `input_arguments` and `platform_metadata`

### Timestamp conversion
Timestamps in events are epoch milliseconds, which should be converted to `TIMESTAMPTZ` in Postgres. Using a dedicated timestamp type greatly simplifies querying and reading from the table, especially in BI-tools.

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
