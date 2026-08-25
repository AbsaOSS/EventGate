# ADR-002: Observability — structured logging and request correlation

## Status

**ACCEPTED** — August 2026

## Context and Problem Statement

EventGate ran with 79 log statements: 52 at `DEBUG`, none at `INFO`. Production runs with `LOG_LEVEL=INFO`, so service emitted effectively nothing per request: no request line, outcome, or rejection reason. Raising level to `DEBUG` produced unusable output because root logger also enabled `botocore`, `urllib3`, and `s3transfer` noise.

Operational failures:

- Client `401`, `403`, or `400` responses produced no log line. Support could not answer why request was rejected.
- Partial fan-out failure—Kafka accepted, Postgres failed—returned `500` without recording sink state. Reconciliation required manual work.
- No invocation-wide or caller-to-service correlation.
- No durations; slow writer could not be identified.

## Decision Outcome

Adopt [AWS Lambda Powertools for Python](https://docs.aws.amazon.com/powertools/python/latest/core/logger/) as logging backend for both EventGate Lambdas. Emit JSON logs enriched with Lambda execution context and request correlation ID. Return correlation ID to caller. Do not adopt AWS X-Ray tracing now.

### Integration approach

Create Powertools `Logger` once in `src/utils/observability.py`. Attach its handler to root logger through `configure_root_logging()`. Modules continue using `logging.getLogger(__name__)`.

This keeps existing call sites unchanged and sends JSON output through shared root handler. Persistent keys, including `correlation_id`, live on shared `LambdaPowertoolsFormatter`; appending once per request adds key to every module log line without threading logger through call stack.

Do not use `copy_config_to_registered_loggers()`: it sets `propagate = False` on module loggers and breaks unit-test `caplog`. Root attachment avoids this.

Do not use `inject_lambda_context` decorator: it raises `AttributeError` when Lambda context is `None`, as in unit and integration tests invoking `lambda_handler`. `bind_request_context()` supplies equivalent binding and tolerates missing context.

### Log-level policy

| Level | Content |
|---|---|
| `TRACE` | Full message payloads, redacted and size-capped. Never enabled by default. |
| `DEBUG` | Configuration loading, lazy initialization, per-writer send attempts, connection reuse. |
| `INFO` | One line per request outcome, cold-start initialization, accepted messages, stats query results. |
| `WARNING` | Rejected requests—auth, authorization, validation—degraded health, Kafka flush retries. |
| `ERROR` | Writer failures, partial fan-out failures, failed queries, unhandled request errors. |

Every non-2xx response must produce exactly one log line explaining cause.

Cap third-party loggers (`boto3`, `botocore`, `urllib3`, `s3transfer`, `aiosql`, `confluent_kafka`) at `WARNING` so `DEBUG` and `TRACE` remain readable.

Retain custom numeric `TRACE` level (`5`). Register it with standard `logging` before constructing Powertools `Logger`; pass level numerically so Powertools validates it. Unit test pins behavior: unrecognized level silently falls back to `INFO`, disabling payload logging.

### Correlation-ID contract

Resolve ID for each request, in order:

1. `X-Correlation-ID` request header.
2. `X-Request-ID` request header.
3. API Gateway request ID (`requestContext.requestId`).

Accept headers only when matching `^[A-Za-z0-9._:-]{1,128}$`. Reject newlines and control characters to prevent log injection; cap length to prevent every log line from being bloated.

Return resolved ID in `X-Correlation-ID` response header for every success and error response. Stamp it centrally in `dispatch_request()`, single point through which every response passes.

### Consequences

- Logs change from plain text to JSON. No downstream log parser exists; JSON is easier for human reading and CloudWatch Logs Insights queries.
- `dispatch_request()` catches `Exception` at boundary instead of fixed list of six exception types. Escaping `psycopg2.Error` or `KafkaException` now becomes structured error rather than runtime traceback and opaque API Gateway `502`. `SystemExit` still propagates; `/terminate` remains unaffected.
- Malformed request body on `POST /topics/{topic_name}` returns `400 validation`, not `500 internal`.
- Callers receive additive `X-Correlation-ID` response header; response body contract unchanged.
- CloudWatch cost rises slightly. `INFO` emits roughly one to three lines per invocation. `POWERTOOLS_LOGGER_SAMPLE_RATE` allows sampled production `DEBUG` detail.

### Not in scope

- **X-Ray tracing.** If enabled later, Powertools `Tracer` plugs into `observability.py`; function needs `Tracing: Active` and `xray:PutTraceSegments`. Kafka spans need manual subsegments.
- **EMF metrics** for writer failures, auth failures, and cold starts. Track separately.
- **Downstream correlation propagation** through Kafka message headers and EventBridge `TraceHeader`. Add when consumer can read it.

## Alternatives Considered

1. **Plain standard-library logging with hand-written JSON formatter.** No new dependency, but Lambda context, cold-start detection, correlation-ID plumbing, and log sampling would require local implementation and maintenance.
2. **AWS Lambda Powertools (chosen).** Purpose-built for Lambda. Provides JSON formatter, `cold_start`, `function_request_id`, correlation-ID handling, and sampling. Lambdas deploy as container images, so change is one `requirements.txt` entry with no layer ARN per region. Cost: one production dependency and roughly 2–4 MB image size, negligible next to librdkafka build.
3. **Powertools plus X-Ray tracing.** Deferred. Target-account X-Ray usage remains undecided; it adds cost; `confluent-kafka` cannot be patched by X-Ray SDK, so Kafka spans need manual instrumentation. Correlation ID meets immediate need: joining log lines within invocation and across services.

## Related Tickets

* [#193](https://github.com/AbsaOSS/EventGate/issues/193)

## References

* [AWS Lambda Powertools for Python Logger](https://docs.aws.amazon.com/powertools/python/latest/core/logger/)
* [AWS X-Ray SDK for Python](https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-python.html)
