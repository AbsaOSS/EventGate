# ADR 001. Observability -- structured logging and request correlation

## Decision

Adopt [AWS Lambda Powertools for Python](https://docs.aws.amazon.com/powertools/python/latest/core/logger/) as the logging backend for both EventGate lambdas. Logs are emitted as JSON, enriched with the Lambda execution context, and carry a request correlation id that is also returned to the caller. AWS X-Ray tracing is deliberately **not** adopted at this point.

### Background

EventGate ran with 79 log statements, of which 52 were `DEBUG` and none were `INFO`. Production runs with `LOG_LEVEL=INFO`, so the service emitted effectively nothing per request: no request line, no outcome, no reason for a rejection. Raising the level to `DEBUG` was the only alternative and produced unusable output, because the level was applied to the root logger and `botocore`, `urllib3` and `s3transfer` flooded the log group.

The concrete operational failures this caused:

- A client receiving `401`, `403` or `400` produced **no log line at all**. Support could not answer "why was I rejected?".
- A partial fan-out failure (Kafka accepted, Postgres failed) returned `500` without recording which sink had already accepted the message, so reconciliation had to be done by hand.
- Nothing tied log lines from one invocation together, and nothing tied EventGate's lines to the caller's job or run.
- No durations were recorded anywhere, so "which writer is slow" was unanswerable.

See issue [#193](https://github.com/AbsaOSS/EventGate/issues/193).

### Considered options

**1. Plain standard library logging with a hand-written JSON formatter.**
No new dependency, but the Lambda execution context, cold start detection, correlation id plumbing and log sampling would all have to be written and maintained here.

**2. AWS Lambda Powertools (chosen).**
Purpose-built for this runtime. Provides the JSON formatter, `cold_start`, `function_request_id`, correlation id handling and sampling out of the box. The lambdas are deployed as container images, so it is a single `requirements.txt` entry with no layer ARNs to pin per region. Cost is one production dependency and roughly 2-4 MB of image size, negligible next to the librdkafka build already in the image.

**3. Powertools plus X-Ray tracing.**
Deferred. X-Ray usage in the target account is not decided, it carries its own cost, and `confluent-kafka` is not patchable by the X-Ray SDK, so Kafka spans would need manual instrumentation. The correlation id covers the immediate need, which is joining log lines across an invocation and across services.

### Integration approach

Powertools' `Logger` is created once in `src/utils/observability.py`, and its handler is attached to the **root** logger by `configure_root_logging()`. Modules keep using `logging.getLogger(__name__)`.

This matters for two reasons:

- Existing call sites did not have to change to gain JSON output, so the change stayed reviewable.
- Persistent keys (including `correlation_id`) live on the shared `LambdaPowertoolsFormatter` instance, so a key appended once per request appears on every module's log lines without threading a logger through the call stack.

The alternative, `copy_config_to_registered_loggers()`, sets `propagate = False` on each module logger, which breaks `caplog` in the unit tests. Attaching at the root avoids that.

`inject_lambda_context` is **not** used as a decorator. It raises `AttributeError` when the Lambda context is `None`, which is how both unit and integration tests invoke `lambda_handler`. `bind_request_context()` performs the same binding and tolerates a missing context.

### Log level policy

| Level     | Content                                                                                        |
|-----------|------------------------------------------------------------------------------------------------|
| `TRACE`   | Full message payloads, redacted and size capped. Never enabled by default.                       |
| `DEBUG`   | Configuration loading, lazy initialization, per-writer send attempts, connection reuse.          |
| `INFO`    | One line per request outcome, cold start initialization, accepted messages, stats query results. |
| `WARNING` | Rejected requests (auth, authorization, validation), degraded health, Kafka flush retries.       |
| `ERROR`   | Writer failures, partial fan-out failures, failed queries, unhandled request errors.             |

The binding rule is: **every non-2xx response produces exactly one log line explaining the cause.**

Third-party loggers (`boto3`, `botocore`, `urllib3`, `s3transfer`, `aiosql`, `confluent_kafka`) are capped at `WARNING` so that running the service at `DEBUG` or `TRACE` stays readable.

The custom `TRACE` level (5) introduced earlier is retained. It is registered with the standard `logging` module before the Powertools `Logger` is constructed, and the level is passed numerically, so Powertools' level validation resolves it. A unit test pins this behaviour, because an unrecognised level would silently fall back to `INFO` and disable payload logging without any error.

### Correlation id contract

Resolved per request, in order:

1. `X-Correlation-ID` request header.
2. `X-Request-ID` request header.
3. API Gateway request id (`requestContext.requestId`).

Header values are accepted only when they match `^[A-Za-z0-9._:-]{1,128}$`. Inbound header values are written into the log stream, so newlines and control characters are rejected to prevent log injection, and the length is capped so the id cannot bloat every line.

The resolved id is returned in the `X-Correlation-ID` response header of every response, success or error, so a client can quote a single id when reporting a problem. It is stamped centrally in `dispatch_request()`, which is the single point every response passes through.

### Consequences

- Log output changes from plain text to JSON. Nothing downstream parses these logs today; they are read by humans and queried in CloudWatch Logs Insights, where JSON is strictly easier to query.
- `dispatch_request()` now catches `Exception` at the boundary instead of a fixed list of six exception types. Previously a `psycopg2.Error` or `KafkaException` escaping a handler reached the runtime, producing an unstructured traceback and an opaque `502` from API Gateway. `SystemExit` still propagates, so the `/terminate` route is unaffected.
- A malformed request body on `POST /topics/{topic_name}` now returns `400 validation` instead of `500 internal`. It is a client error and was only reaching the internal error path because `json.loads` was left to the boundary handler.
- Callers gain a response header they did not have before. This is additive and does not change the response body contract.
- CloudWatch cost rises slightly. `INFO` stays at roughly one to three lines per invocation; `POWERTOOLS_LOGGER_SAMPLE_RATE` is available if `DEBUG` detail is wanted in production without enabling it for every invocation.

### Not in scope

- **X-Ray tracing.** If it is enabled later, `Tracer` from the same library plugs into `observability.py`; the function needs `Tracing: Active` and the `xray:PutTraceSegments` permission, and Kafka spans need manual subsegments.
- **EMF metrics** (writer failures, auth failures, cold starts) as alarm sources. Worth a separate issue.
- **Downstream correlation propagation**: Kafka message headers and the EventBridge `TraceHeader`. Useful, but only once a consumer is ready to read them.
