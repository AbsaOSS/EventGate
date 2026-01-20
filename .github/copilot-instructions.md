Copilot instructions for EventGate

Purpose
EventGate is an AWS Lambda-based event gateway that receives messages via API Gateway and dispatches them to Kafka, EventBridge, and PostgreSQL.

Context
- AWS Lambda entry point: `src/event_gate_lambda.py`
- Handlers: `HandlerApi`, `HandlerToken`, `HandlerTopic`, `HandlerHealth` in `src/handlers/`
- Writers: `WriterEventBridge`, `WriterKafka`, `WriterPostgres` in `src/writers/` (inherit from `Writer` base class)
- Config: `conf/config.json`, `conf/access.json`, `conf/topic_schemas/*.json`
- Routes defined in `ROUTE_HANDLERS` dict in `event_gate_lambda.py`

Coding guidelines
- Keep changes small and focused
- Prefer clear, explicit code over clever tricks
- Do not change existing error messages or API response formats without approval
- Keep API Gateway response structure stable: `{"statusCode": int, "headers": {...}, "body": "..."}`

Python and style
- Target Python 3.13
- Add type hints for all public functions and classes
- Use `logging.getLogger(__name__)`, not print
- Use lazy % formatting in logging: `logger.info("msg %s", var)`
- All imports at top of file

Patterns
- Handlers: class with `__init__` (no exceptions)
- Writers: inherit from `Writer(ABC)`, implement `write(topic, message) -> (bool, str|None)` and `check_health() -> (bool, str)`
- Use lazy initialization in writers; keep `__init__` exception-free
- Route dispatch via `ROUTE_HANDLERS` dict mapping routes to handler functions

Testing
- Use pytest with tests in `tests/`
- Mock external services via `conftest.py`: Kafka, EventBridge, PostgreSQL, S3
- No real API/DB calls in unit tests
- Test structure: `tests/handlers/`, `tests/writers/`, `tests/utils/`

Quality gates
- Black formatting
- Pylint >= 9.5
- mypy clean
- pytest coverage >= 80%

Run `./ci_local.sh` before every commit to verify all quality gates pass.

File overview
- `src/event_gate_lambda.py`: Lambda entry point, route dispatch
- `src/handlers/`: Handler classes (api, token, topic, health)
- `src/writers/`: Writer classes inheriting from `Writer` base
- `src/utils/`: Utilities (logging, config path, error responses)
- `conf/`: Configuration files and topic schemas
- `tests/`: pytest tests mirroring src structure

Architecture notes
- Lambda receives API Gateway events, dispatches via `ROUTE_HANDLERS`
- Handlers manage request/response logic
- Writers handle message delivery to external systems
- All external dependencies mocked in tests

Learned rules
- Keep error response format stable: `{"success": false, "statusCode": int, "errors": [...]}`
- Handler `__init__` must be exception-free
- Writers use lazy initialization
