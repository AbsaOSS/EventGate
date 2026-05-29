# ADR 000. Event Bus -- Status change topic

## Decision

Create Kafka Topics for Pipeline Status change topics.

### Background
The pipeline runs topic has been introduced earlier. The main use-case behind that topic was to facilitate event-driven job executions in CurateApp, based on events emitted by IngestApp.

The purpose of the status change topic is monitoring of job executions. The data model should allow the monitoring of executions in progress and updates to the status of an execution. Furthermore, the data model should allow grouping and nesting of executions, and model retries. The data model should be expressive enough to model dependencies across multiple applications (IngestApp, CurateApp, ProcessApp)

While the runs topic is related, its schema assumes a run is finished. There is no support for status changes, to track a job execution while it is running.

Eventually, the status-change topic may provide all the functionality, which is needed to serve the event-driven execution use-case, and thus make the runs topic redundant. However, changing the runs topic to support status updates is hardly possible in a backwards compatible way. Therefore, the status-change should be created as a dedicated new topic.


### Status change topic

A status change event represents a point-in-time update about a single job execution. Instead of sending one final event at the end of a run, producers emit lifecycle events as the execution progresses.

The five lifecycle event types are:

- JobCreatedEvent: emitted when an execution is registered and potentially sent to a queue
- JobStartedEvent: emitted when processing actually begins.
- JobCreatedAndStartedEvent: emitted when an execution is started immediately. This replaces the previous two events.
- JobUpdatedEvent: emitted for in-flight updates that are relevant for monitoring (for example progress or contextual metadata enrichment).
- JobFinishedEvent: emitted when execution reaches a terminal state (SUCCEEDED, FAILED, or KILLED).

Events should be sent whenever the lifecycle changes or monitoring-relevant information changes. At minimum, producers should send one created JobCreatedEvent or JobCreatedAndStartedEvent and one JobFinishedEvent event, with started/updated events included when available.

Many fields are intentionally optional. In distributed execution environments, many values are only known eventually (for example platform IDs or enriched context). This schema allows progressive disclosure over time while still enforcing a minimal event envelope.

#### Job hierarchies

Nested jobs are modeled by setting `parent_job_id` to the parent `job_id`. To optimize analysis, it is advisable to additionally set the same `job_group_id` for all jobs in the hierarchy. It can be set to the root parent id, for example.

#### Retries

Retries can be modeled by setting the `initial_job_id` to the job id of the retried job and increasing the `attempt_number`. In case of retries of nested jobs, the `parent_job_id` should be set to the parent of the retried job, and the `initial_job_id` should be set to the job id of the retried job. In the job hierarchy, retried jobs should be siblings, and not have a parent-child relationship.

## Examples

Example `JobCreatedAndStartedEvent`:

```json
{
    "event_type": "JobCreatedAndStartedEvent",
    "event_id": "6f4d8f0f-c9e3-4f53-8d5f-c16ff41f0f90",
    "tenant_id": "abcd",
    "source_app": "ingestapp",
    "source_app_version": "2.14.0",
    "environment": "dev",
    "timestamp_event": 1747646400000,
    "country": "za",
    "job_id": "e5b8f1cc-e40a-4923-8a77-9e36b1a7bdb0",
    "job_name": "IngestApp Ingestion",
    "platform": "aws.glue",
    "platform_metadata": {
        "numberOfWorkers": 8,
        "glueVersion": "5.0"
    },
    "input_arguments": {
        "pipeline_id": "1234",
        "current_date": "2026-05-19"
    },
    "definition_id": "42",
    "status_type": "RUNNING",
    "additional_context": {
        "pipeline_name": "MYPIPELINE"
    }
}
```

Example `JobFinishedEvent` (FAILED):

```json
{
    "event_type": "JobFinishedEvent",
    "event_id": "f4c2f92d-b6fb-4ce4-a58c-7ba28632a73c",
    "timestamp_event": 1747646705000,
    "job_id": "e5b8f1cc-e40a-4923-8a77-9e36b1a7bdb0",
    "status_type": "FAILED",
    "status_subtype": "NO_DATA",
    "status_detail": "No data at the source"
}
```

More examples can be found in the appendix.


## Alternatives Considered
- Reusing the runs topic

  Pros: The runs topic appears similar at first glance, and reusing it would avoid introducing a new topic.

  Cons: The runs schema assumes a completed run, while status-change events need to support live tracking. It also has limited support for job hierarchies. Finally, the primary concern differs: runs events are aimed at downstream triggering, while status-change events focus on detailed monitoring.

- Separate schema for job steps versus nested jobs

  Pros: A dedicated step schema could reduce repeated fields such as tenant and country.

  Cons: Splitting hierarchy data across separate entities makes traversal and analysis harder. It also increases operational complexity by adding another schema/topic to manage, and introduces ambiguity in deciding whether something should be modeled as a step or as a nested job.

## Limitations
- A job can only be assigned to one job group id. With this model, it's not possible to assign a job to multiple groups or to define group hierarchies. This may be a limitation when linking jobs across IngestApp, CurateApp and ProcessApp.

## Future Consideration
- Merging runs topic into status-change topic. It depends on the concrete use-cases of the runs topic,
whether this is desirable and feasible. However, the status-change deliberately uses many of the same fields
that the runs topic uses.

- Linking jobs across IngestApp, CurateApp and ProcessApp. The model allows this in principle, both as a parent-child relationship or a sibling relationship. For example, CurateApp jobs could reference an IngestApp job as a parent job id, or an artificial common parent job could be defined. In any case, the CurateApp job would need to know the parent job id.

## Appendix A: Json schema
The json schema is located here: [status_change.json](../../conf/topic_schemas/status_change.json/)

## Appendix B: The list of allowed country codes

If a producer sends a code which is not on the list, it should be a
warning, not an error. This is because this list is not final and is
going to be updated.

| Code | Country |
| --- | --- |
| \* | Multiple countries |
| bw | Botswana |
| gh | Ghana |
| ke | Kenya |
| mu | Mauritius (legacy, prefer offshre/onshore when possible) |
| of-mu | Mauritius (offshore) |
| on-mu | Mauritius (onshore) |
| mz | Mozambique |
| sc | Seychelles |
| tz | Tanzania (legacy, prefer bank-specific versions below when possible) |
| abt-tz | Tanzania (Absa Bank Tanzania) |
| nbc-tz | Tanzania (National Bank of Commerce) |
| ug | Uganda |
| za | South African Republic |
| zm | Zambia |

This list is not complete, EventGate should allow other values.

## Appendix C: Example nested jobs

Example hierarchy:

- IngestApp Ingestion
  - Create Pramen Config Lambda
  - Pramen Glue Job
    - Land
    - Standardize
    - Publish to Hive
    - Add control metrics

Example event sequence (same `job_group_id` used for the full hierarchy):

The events are stored in separate files under `adr/000-status-change/examples/appendix-c-nested-jobs/`:

- [01-job-created-and-started-ingestion.json](examples/appendix-c-nested-jobs/01-job-created-and-started-ingestion.json)
- [02-job-created-and-started-create-pramen-config.json](examples/appendix-c-nested-jobs/02-job-created-and-started-create-pramen-config.json)
- [03-job-finished-create-pramen-config-succeeded.json](examples/appendix-c-nested-jobs/03-job-finished-create-pramen-config-succeeded.json)
- [04-job-updated-ingestion-running.json](examples/appendix-c-nested-jobs/04-job-updated-ingestion-running.json)
- [05-job-created-and-started-pramen.json](examples/appendix-c-nested-jobs/05-job-created-and-started-pramen.json)
- [06-job-created-and-started-land.json](examples/appendix-c-nested-jobs/06-job-created-and-started-land.json)
- [07-job-finished-land-succeeded.json](examples/appendix-c-nested-jobs/07-job-finished-land-succeeded.json)
- [08-job-created-and-started-publish-to-hive.json](examples/appendix-c-nested-jobs/08-job-created-and-started-publish-to-hive.json)
- [09-job-finished-publish-to-hive-failed.json](examples/appendix-c-nested-jobs/09-job-finished-publish-to-hive-failed.json)
- [10-job-finished-ingestion-failed.json](examples/appendix-c-nested-jobs/10-job-finished-ingestion-failed.json)

### Appendix D: Example job retry
This example builds upon the example in appendix C. In this scenario the failed job is retried with special configuration that only executes the Publish to Hive step in the Pramen job.

The retried job is referenced using the `initial_job_id`, and the `attempt_number` is increased to 2.

Example retry event sequence:

The events are stored in separate files under `adr/000-status-change/examples/appendix-d-job-retry/`:

- [01-job-created-and-started-ingestion-retry.json](examples/appendix-d-job-retry/01-job-created-and-started-ingestion-retry.json)
- [02-job-created-and-started-create-pramen-config.json](examples/appendix-d-job-retry/02-job-created-and-started-create-pramen-config.json)
- [03-job-finished-create-pramen-config-succeeded.json](examples/appendix-d-job-retry/03-job-finished-create-pramen-config-succeeded.json)
- [04-job-created-and-started-pramen.json](examples/appendix-d-job-retry/04-job-created-and-started-pramen.json)
- [05-job-updated-ingestion-running.json](examples/appendix-d-job-retry/05-job-updated-ingestion-running.json)
- [06-job-created-and-started-publish-to-hive.json](examples/appendix-d-job-retry/06-job-created-and-started-publish-to-hive.json)
- [07-job-finished-publish-to-hive-succeeded.json](examples/appendix-d-job-retry/07-job-finished-publish-to-hive-succeeded.json)
- [08-job-finished-pramen-succeeded.json](examples/appendix-d-job-retry/08-job-finished-pramen-succeeded.json)
- [09-job-finished-ingestion-succeeded.json](examples/appendix-d-job-retry/09-job-finished-ingestion-succeeded.json)