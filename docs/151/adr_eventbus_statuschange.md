# ADR. Event Bus -- Status change topic

## Decision

Create Kafka Topics for Pipeline Status change topics.

### Background
A previous ADR has already introduced a pipeline runs topic. The main use-case behind that topic was to facilitate event-driven job executions in CurateApp, based on events emitted by IngestApp.

The purpose of the status change topic is monitoring of job executions. The data model should allow the monitoring of executions in progress and updates to the status of an execution. Furthermore, the data model should allow grouping and nesting of executions, and model retries. The data model should be expressive enough to model dependencies across multiple applications (IngestApp, CurateApp, ProcessApp)

While the runs topic is related, its schema assumes a run is finished. There is no support for status changes, to track a job execution while it is running.

Eventually, the status-change topic may provide all the functionality, which is needed to serve the event-driven execution use-case, and thus make the runs topic redundant. However, changing the runs topic to support status updates is hardly possible in a backwards compatible way. Therefore, the status-change should be created as a dedicated new topic.


### Status change topic

A status change event represents a point-in-time update about a single job execution. Instead of sending one final event at the end of a run, producers emit lifecycle events as the execution progresses.

The five lifecycle event types are:

- JobCreatedEvent: emitted when an execution is registered and potentially sent to a queue
- JobStartedEvent: emitted when processing actually begins.
- JobCreatedAndStartedEvent: emitted when an execution is started immediately. This replaces the previous two events.
- JobUpdatedEvent: emitted for in-flight updates that are relevant for monitoring (for example progress, reclassification, or contextual metadata enrichment).
- JobFinishedEvent: emitted when execution reaches a terminal state (SUCCEEDED, FAILED, or KILLED).

Events should be sent whenever the lifecycle changes or monitoring-relevant information changes. At minimum, producers should send one created JobCreatedEvent or JobCreatedAndStartedEvent and one JobFinishedEvent event, with started/updated events included when available.

Most fields are intentionally optional. In distributed execution environments, many values are only known eventually (for example platform IDs, final status details, or enriched context). This schema allows progressive disclosure over time while still enforcing a minimal event envelope.

#### Job hierarchies

Nested jobs are modeled by setting `parent_job_id` to the parent `job_id`. To optimize analysis, it is advisable to additionally set the same `job_group_id` for all jobs in the hierarchy. It can be set to the root parent id, for example.

#### Retries

Retries can be modeled by setting the `initial_job_id` to the job id of the retried job and increasing the `attempt_number`. In case of retries of nested jobs, the `parent_job_id` should be set to the parent of the retried job, and the `initial_job_id` should be set to the job id of the retried job. In the job hierarchy, retried jobs should be siblings, and not have a parent-child relationship.

### Ordering guarantee
At-least-once delivery guarantee can be tolerated, but the order of events with the same job id must be guaranteed.

## Examples

Example `JobCreatedAndStartedEvent`:

```json
{
    "event_type": "JobCreatedAndStartedEvent",
    "event_id": "6f4d8f0f-c9e3-4f53-8d5f-c16ff41f0f90",
    "job_id": "e5b8f1cc-e40a-4923-8a77-9e36b1a7bdb0",
    "timestamp_event": 1747646400000,
    "source_app": "ingestapp",
    "source_app_version": "2.14.0",
    "environment": "dev",
    "tenant_id": "abcd",
    "country_code": "za",
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
    "job_id": "e5b8f1cc-e40a-4923-8a77-9e36b1a7bdb0",
    "timestamp_event": 1747646705000,
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
```json
{
    "type": "object",
    "properties": {
        "event_type": {
            "type": "string",
            "enum": [
                "JobCreatedEvent",
                "JobCreatedAndStartedEvent",
                "JobStartedEvent",
                "JobUpdatedEvent",
                "JobFinishedEvent"
            ],
            "description": "Lifecycle event type for job status changes."
        },
        "event_id": {
            "type": "string",
            "format": "uuid",
            "description": "Unique identifier for the event (UUID)"
        },
        "job_ref": {
            "type": [
                "string",
                "null"
            ],
            "description": "Identifier of the job in it's respective system (e.g. Spark Application Id, Glue Job Id, EMR Step Id, etc)."
        },
        "tenant_id": {
            "type": [
                "string",
                "null"
            ],
            "description": "Application ID or ServiceNow identifier"
        },
        "source_app": {
            "type": [
                "string",
                "null"
            ],
            "description": " Standardized source application name (ingestapp, curateapp, processapp, etc)"
        },
        "source_app_version": {
            "type": [
                "string",
                "null"
            ],
            "description": "Source application version (SemVer preferred)"
        },
        "environment": {
            "type": [
                "string",
                "null"
            ],
            "description": "Environment (dev, uat, pre-prod, prod, test or others)"
        },
        "timestamp_event": {
            "type": "number",
            "description": "Timestamp of the event in epoch milliseconds"
        },
        "country": {
            "type": [
                "string",
                "null"
            ],
            "description": "The country the data is related to, e.g. za, ke, on-mu, nbc-tz, etc."
        },
        "job_id": {
            "type": "string",
            "format": "uuid",
            "description": "Primary job identifier (UUID)."
        },
        "parent_job_id": {
            "type": [
                "string",
                "null"
            ],
            "format": "uuid",
            "description": "Optional parent job identifier (UUID), to represent nested job hierarchies."
        },
        "initial_job_id": {
            "type": [
                "string",
                "null"
            ],
            "format": "uuid",
            "description": "Optional initial job identifier (UUID), to represent retried or replayed jobs."
        },
        "job_group_id": {
            "type": [
                "string",
                "null"
            ],
            "format": "uuid",
            "description": "Job group identifier (UUID), may or may not reference a job id."
        },
        "job_name": {
            "type": [
                "string",
                "null"
            ],
            "description": "Human-readable job name."
        },
        "attempt_number": {
            "type": [
                "integer",
                "null"
            ],
            "minimum": 1,
            "description": "Attempt number for this job."
        },
        "platform": {
            "type": [
                "string",
                "null"
            ],
            "description": "Platform, e.g. aws.emr, aws.glue, aws.lambda."
        },
        "platform_metadata": {
            "type": [
                "object",
                "null"
            ],
            "description": "Platform-specific metadata (e.g. {\"cluster_id\": \"j-...\"})."
        },
        "input_arguments": {
            "type": [
                "object",
                "null"
            ],
            "description": "Arguments passed to the job."
        },
        "definition_id": {
            "type": [
                "string",
                "null"
            ],
            "description": "Definition (Pipeline, Domain, Process) identifier."
        },
        "definition_version": {
            "type": [
                "string",
                "null"
            ],
            "description": "Optional definition version."
        },
        "status_type": {
            "type": [
                "string",
                "null"
            ],
            "enum": [
                "WAITING",
                "RUNNING",
                "SUCCEEDED",
                "FAILED",
                "KILLED",
                null
            ],
            "description": "High-level status type for the current lifecycle event."
        },
        "status_subtype": {
            "type": [
                "string",
                "null"
            ],
            "description": "Optional status subtype, e.g. NO_DATA or error code."
        },
        "status_detail": {
            "type": [
                "string",
                "null"
            ],
            "description": "Optional human-readable status detail, e.g. short error message."
        },
        "additional_context": {
            "type": [
                "object",
                "null"
            ],
            "description": "Additional context payload."
        }
    },
    "required": [
        "event_type",
        "event_id",
        "job_id",
        "timestamp_event"
    ],
    "allOf": [
        {
            "if": {
                "properties": {
                    "event_type": {
                        "enum": [
                            "JobCreatedEvent",
                            "JobCreatedAndStartedEvent"
                        ]
                    }
                }
            },
            "then": {
                "required": [
                    "source_app",
                    "source_app_version",
                    "environment",
                    "platform",
                    "input_arguments"
                ]
            }
        },
        {
            "if": {
                "properties": {
                    "event_type": {
                        "const": "JobFinishedEvent"
                    }
                }
            },
            "then": {
                "required": [
                    "status_type"
                ],
                "properties": {
                    "status_type": {
                        "enum": [
                            "SUCCEEDED",
                            "FAILED",
                            "KILLED"
                        ]
                    }
                }
            }
        }
    ]
}
```

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

```json
[
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "ea4ef752-2a2b-4f6c-93ee-f31a2f9b7d10",
        "job_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_name": "IngestApp Ingestion",
        "timestamp_event": 1747646400000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.stepfunctions",
        "definition_id": "1234",
        "input_arguments": {
            "pipelineId": 1234,
            "currentDate": "2026-05-19",
            "triggerType": "SCHEDULE"
        }
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "ad766149-2d27-4c80-833d-b2f5d7f8108f",
        "job_id": "0002aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646410000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.lambda",
        "definition_id": "1234",
        "job_name": "Create Pramen Config",
        "input_arguments": {
            "pipelineId": 1234,
            "currentDate": "2026-05-19"
        }
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "ff1714bf-a3f6-4b0d-9b4b-5662b8e9c42e",
        "job_id": "0002aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "tenant_id": "abcd",
        "country": "za",
        "timestamp_event": 1747646445000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobUpdatedEvent",
        "event_id": "1f2456d2-f2bf-40f7-a8be-e46f3dc9b4b3",
        "job_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646447000,
        "status_type": "RUNNING",
        "additional_context": {
            "pipeline_name": "MYPIPELINE"
        }
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "ec36ab3b-0f97-4e81-8f4d-b88f493f6ca2",
        "job_id": "0003aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646450000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Pramen"
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "4a4c2f5c-8be8-4b26-a5ff-60f6fcb32f43",
        "job_id": "0004aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0003aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646490000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Land"
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "5f5f5f5f-5a5a-4f5f-8a5a-5f5f5f5f5f5f",
        "job_id": "0004aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_ref": "jr_5ecc2b7609d0e9c665abdcb8af8059489fcd2e60c916bd52476dcfa41e1417af",
        "timestamp_event": 1747646520000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "20e93bd4-84c4-426d-9865-f91ec28d2f89",
        "job_id": "0005aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0003aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646545000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Standardize"
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "6f6f6f6f-6b6b-4f6f-8b6b-6f6f6f6f6f6f",
        "job_id": "0005aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646580000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "6b885f6b-ac7f-4abf-8d20-8bb58f9bd12a",
        "job_id": "0006aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0003aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646602000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Publish to Hive"
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "7f7f7f7f-7c7c-4f7f-8c7c-7f7f7f7f7f7f",
        "job_id": "0006aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646635000,
        "status_type": "FAILED",
        "status_subtype": "HIVE_TABLE_UPDATE_FAILED",
        "status_detail": "sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target: Unable to execute HTTP request: PKIX path building failed."
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "a6de5fb1-8858-4e7f-87c0-6cb3f9c77799",
        "job_id": "0007aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0003aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646660000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Add control metrics",
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "8f8f8f8f-8d8d-4f8f-8d8d-8f8f8f8f8f8f",
        "job_id": "0007aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646685000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "7baea31d-b630-451f-a59e-699e0c4f53b3",
        "job_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646705000,
        "status_type": "FAILED",
        "status_subtype": "HIVE_TABLE_UPDATE_FAILED",
        "status_detail": "sun.security.provider.certpath.SunCertPathBuilderException: unable to find valid certification path to requested target: Unable to execute HTTP request: PKIX path building failed."
    }
]
```

### Appendix D: Example job retry
This example builds upon the example in appendix C. In this scenario the failed job is retried with special configuration that only executes the Publish to Hive step in the Pramen job.

The retried job is referenced using the `initial_job_id`, and the `attempt_number` is increased to 2.

Example retry event sequence:

```json
[
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "9b7d2a40-6f12-4f9d-9c5e-8f2c0f524e11",
        "job_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_name": "IngestApp Ingestion",
        "initial_job_id": "0001aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "attempt_number": 2,
        "job_group_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646800000,
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "input_arguments": {
            "pipelineId": 1234,
            "currentDate": "2026-05-19"
        }
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "f2f0cf47-a738-489c-b900-671f74c6f58b",
        "job_id": "0012aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646810000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.lambda",
        "definition_id": "1234",
        "job_name": "Create Pramen Config",
        "input_arguments": {
            "pipelineId": 1234,
            "currentDate": "2026-05-19"
        }
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "4b77d3f1-62d1-42b4-a7f5-83f36fd24818",
        "job_id": "0012aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646824000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "ec36ab3b-0f97-4e81-8f4d-b88f493f6ca2",
        "job_id": "0013aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "job_group_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646825000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Pramen",
        "input_arguments": {
            "pipelineId": 1234,
            "currentDate": "2026-05-19"
        }
    },
    {
        "event_type": "JobUpdatedEvent",
        "event_id": "9dbf71c1-5f71-4a92-8747-3db90d6904e4",
        "job_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "tenant_id": "abcd",
        "country": "za",
        "timestamp_event": 1747646826000,
        "status_type": "RUNNING",
        "additional_context": {
            "pipeline_name": "MYPIPELINE"
        }
    },
    {
        "event_type": "JobCreatedAndStartedEvent",
        "event_id": "2e6f31c8-76b8-4bcf-9066-b27cbd5564e1",
        "job_id": "0014aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "parent_job_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "attempt_number": 1,
        "job_group_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646830000,
        "tenant_id": "abcd",
        "country": "za",
        "source_app": "ingestapp",
        "source_app_version": "2.14.0",
        "environment": "dev",
        "platform": "aws.glue",
        "definition_id": "1234",
        "job_name": "Publish to Hive",
        "input_arguments": {
            "pipelineId": 1234,
            "currentDate": "2026-05-19"
        }
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "e6207f4e-f2cd-4d9c-b2f0-f6490c15f89f",
        "job_id": "0014aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646865000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "e6207f4e-f2cd-4d9c-b2f0-f6490c15f89f",
        "job_id": "0013aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646865000,
        "status_type": "SUCCEEDED"
    },
    {
        "event_type": "JobFinishedEvent",
        "event_id": "2fe09e1f-25d0-4d89-911e-f5fbf699d1f5",
        "job_id": "0011aaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaaaaaa",
        "timestamp_event": 1747646870000,
        "status_type": "SUCCEEDED"
    }
]
```