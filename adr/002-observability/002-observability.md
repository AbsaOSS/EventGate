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

Do not use `inject_lambda_context` decorator: it raises `AttributeError` when Lambda context is `None`, as in unit and integration tests invoking `lambda_handler`. `bind_request_context()` supplies equivalent binding and tolerates missing context. The one thing the decorator does that binding must reproduce is the per-invocation sampling re-draw; `refresh_sampled_log_level()` covers it.

### Logging strategy

Guiding rule: **log volume tracks trouble, not traffic.** A request that works costs a fixed, small number of lines no matter how much work it did. A request that fails spends as many lines as needed to explain itself. Steady-state cost stays flat as traffic grows, and detail is bought on demand instead of paid for continuously.

Three rules implement this on the happy path.

**1. One `INFO` line per request.** `dispatch_request()` emits `Request completed.` with `status_code` and `duration_ms` after every invocation, success or failure. Handlers emit no `INFO` outcome line of their own. Count holds regardless of writer count, message size, or result-set size.

**2. Outcome data rides on that line as fields, not as extra lines.** Handlers attach results through `append_request_context()`—`writers_ok`, `message_key`, `row_count`, `has_more`—so completion line carries them. Recording one more fact about a request must not cost one more line. Prefer new key over new log call.

**3. Step detail is `DEBUG` and stays off in production.** Per-writer sends, connection reuse, configuration loading, token refresh, schema resolution. Answers "how did this request get here", which matters while investigating and is worthless at steady state.

Failure path inverts the budget, and splits in two. The classes are disjoint: a request takes the shape of one or the other, never both.

**Rejections (4xx).** Exactly one rejection `WARNING` names the cause—auth, authorization, validation, unknown route—reason always in fields. No `ERROR`. Support answers "why was my request rejected" from that single line without reproducing it. "Exactly one" counts rejection lines only; operational `WARNING`s—Kafka flush retries, missing key fields, token refresh failure—may legitimately co-occur on the same request.

**Failures (5xx).** Exactly one `ERROR` per failed request: the record emitted by whichever frame converts the failure into the 5xx response. For fan-out that is the aggregated dispatch failure in `_post_topic_message()`, listing every failing writer in a single record. For a handler that builds its own 500, such as `handler_stats`, it is that handler's own `logger.exception`. For anything escaping to the boundary it is `dispatch_request()`. This is a discipline each handler upholds, not a property the boundary can enforce.

Per-writer failure detail stays at `WARNING`, carrying `exc_info=True`, because the aggregate is emitted after `_write_to_all()` returns—outside the `except` block—and can no longer reach the traceback.

The invariant worth alarming on follows from the split:

> Among records emitted during a single invocation, the count of `ERROR` records equals the count of 5xx responses the service built to report a failed request.

An alarm on `level = "ERROR"` then counts failed requests, not log lines. Both counts—the one `INFO` line and the `ERROR` total—are scoped to a single invocation, and four things sit outside them by design:

- **Initialization and import-time errors** belong to no request. A failing cold start is retried by the Lambda runtime, so one broken deployment emits `ERROR`s at container rate, not request rate. `log_uncaught_exceptions=True` can add a second record for the same escaping exception.
- **`/terminate`** leaves through `SystemExit`, which the boundary deliberately does not catch: no completion line, no `ERROR`, and neither is wanted. An exception raised inside `bind_request_context()` escapes the same way, because the binding call runs before the `try`.
- **A degraded `/health` probe** returns `503` and logs one `WARNING`. The `503` is the probe answering correctly, not the request failing, and what deserves an alarm is the dependency it names.
- **Platform 5xx**—timeout, out-of-memory, throttling, API Gateway `502` from a malformed response shape—reach the caller without the service logging anything.

An `ERROR` alarm therefore detects service-built request failures only. Cold-start breakage, dependency degradation, and platform faults each need their own signal; none is covered here.

| Level | Content | Expected frequency |
|---|---|---|
| `TRACE` | Full message payloads, redacted and size-capped. | Never enabled by default. |
| `DEBUG` | Per-step detail: writer sends, config loading, connection reuse, token refresh, routing. | Off in production. |
| `INFO` | Request outcome (`Request completed.`); lifecycle events—lambda initialization, cold-start context, Postgres connection established, token public keys loaded. | Once per request, plus lifecycle lines that are tied to container or connection age, not to request count. |
| `WARNING` | Rejected requests—auth, authorization, validation—degraded health, per-writer failures, Kafka flush retries. | Once per rejection, plus once per failed sub-step. |
| `ERROR` | The record converting a failure into a 5xx: aggregated dispatch failure, handler-built 500, unhandled request errors. Plus misconfiguration preventing service, which is outside request scope. | Once per failed request. |

Cap third-party loggers (`boto3`, `botocore`, `urllib3`, `s3transfer`, `aiosql`, `confluent_kafka`) at `WARNING` so `DEBUG` and `TRACE` remain readable.

Retain custom numeric `TRACE` level (`5`). Register it with standard `logging` before constructing Powertools `Logger`; pass level numerically so Powertools validates it. Unit test pins behavior: unrecognized level falls back to `INFO` and logs one `WARNING` naming the rejected level, disabling payload logging.

### Attaching tracebacks

One rule governs every exception log, and it concerns ownership rather than level:

> A traceback is logged exactly once, by the frame that converts the exception into a response or swallows it. A frame that wraps and re-raises does not log it above `DEBUG`.

Nothing is lost by deferring: `raise WriteError(...) from exc` plus `exc_info=True` upstream formats the full `__cause__` chain.

Given that rule, the call to use follows from where the code sits and which level the budget allows:

| Situation | Call |
|---|---|
| Inside `except`, owning the failure, logging at `ERROR` | `logger.exception(msg)` |
| Inside `except`, owning the failure, logging at `WARNING` or `DEBUG` | `logger.warning(msg, exc_info=True)`, `logger.debug(msg, exc_info=True)` |
| Outside `except`, exception captured earlier | `logger.error(msg, exc_info=captured_exc)` |
| Error state found by inspection, no exception exists | plain `logger.error(msg, extra={...})`, no `exc_info` |

`logger.exception()` hardcodes `ERROR`. That is why mixed usage across the codebase is not an inconsistency to normalize away: per-writer failures sit at `WARNING` precisely to keep the `ERROR` count equal to the failed-request count, and `WARNING` is unreachable through `logger.exception()`. Row three exists because `logger.exception()` outside an `except` block reads an empty `sys.exc_info()` and logs no traceback at all; `writer_kafka` collects the exception across its produce and flush steps, then logs once after both. Its `exc_info=captured_exception` is `None` when the failure arrived through the delivery callback rather than a raised exception—intended, since `exc_info=None` correctly omits a traceback that does not exist. It must not be "fixed" to `exc_info=True`.

### Raising detail when something is wrong

Production runs at `INFO`. When one line per request is not enough:

- Set `LOG_LEVEL=DEBUG` on the function to replay step detail. Third-party capping keeps output readable, which is what made `DEBUG` unusable before this ADR.
- Set `LOG_LEVEL=TRACE` for redacted, size-capped payloads. Never leave enabled.

- Set `POWERTOOLS_LOGGER_SAMPLE_RATE` to collect `DEBUG` detail for a fraction of production traffic without paying for it on every request.

Sampling needs two things wired, and `refresh_sampled_log_level()` supplies both from `bind_request_context()`. Powertools draws the lottery when the `Logger` is constructed and re-draws it only from `refresh_sample_rate_calculation()`, which ships inside the rejected `inject_lambda_context` decorator; calling it per invocation is what makes the draw per request instead of per container. The draw then sets the level of the Powertools logger alone, so the outcome is mirrored onto the root logger—otherwise module loggers, which resolve their level from root, drop their `DEBUG` records before the shared handler sees them. Mirroring takes the lower of the two levels, because sampling may only add detail: a run already at `TRACE` is left alone rather than pulled up to `DEBUG`.

Recommended value: **`0`**, the default, as the standing production setting—one line per request plus the correlation ID answers most questions, and `LOG_LEVEL=DEBUG` covers the rest on demand. Use **`0.05`** while chasing an intermittent failure that cannot be reproduced on demand, which is the case sampling exists for. Cost is predictable: roughly 15 `DEBUG` lines per request on the fan-out `POST` path against a one-line `INFO` baseline puts average volume near `1 + 15r`, so `0.01` costs +15% and `0.05` costs +75%.

Correlation ID is what makes a quiet default workable: one ID joins every line of an invocation and ties them to caller's own ID, so a request is reconstructed from few lines rather than many.

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
- CloudWatch cost rises, but by a bounded amount: `INFO` emits one line per invocation plus a few initialization lines per container, instead of nothing. Cost scales with request count only—not with message size, writer count, or result-set size—so it stays predictable as traffic grows.
- Adding a fact to a request is free in line count but not in line width. The completion line grows as handlers attach keys, which is the intended trade: one wider record beats several narrow ones for CloudWatch Logs Insights queries.

### Not in scope

- **X-Ray tracing.** If enabled later, Powertools `Tracer` plugs into `observability.py`; function needs `Tracing: Active` and `xray:PutTraceSegments`. Kafka spans need manual subsegments.
- **EMF metrics** for writer failures, auth failures, and cold starts. Track separately.
- **Downstream correlation propagation** through Kafka message headers and EventBridge `TraceHeader`. Add when consumer can read it.

## Alternatives Considered

1. **Plain standard-library logging with hand-written JSON formatter.** No new dependency, but Lambda context, cold-start detection, correlation-ID plumbing, and log sampling would require local implementation and maintenance.
2. **AWS Lambda Powertools (chosen).** Purpose-built for Lambda. Provides JSON formatter, `cold_start`, `function_request_id`, and correlation-ID handling. It also ships `DEBUG` sampling. Lambdas deploy as container images, so change is one `requirements.txt` entry with no layer ARN per region. Cost: one production dependency and roughly 2–4 MB image size, negligible next to librdkafka build.
3. **Powertools plus X-Ray tracing.** Deferred. Target-account X-Ray usage remains undecided; it adds cost; `confluent-kafka` cannot be patched by X-Ray SDK, so Kafka spans need manual instrumentation. Correlation ID meets immediate need: joining log lines within invocation and across services.

## Related Tickets

* [#193](https://github.com/AbsaOSS/EventGate/issues/193)
* [#219](https://github.com/AbsaOSS/EventGate/issues/219) — log ownership: a raising frame must not log the traceback
* [#220](https://github.com/AbsaOSS/EventGate/issues/220) — Kafka flush timeout returns `202` with delivery unconfirmed

## References

* [AWS Lambda Powertools for Python Logger](https://docs.aws.amazon.com/powertools/python/latest/core/logger/)
* [AWS X-Ray SDK for Python](https://docs.aws.amazon.com/xray/latest/devguide/xray-sdk-python.html)
