Copilot instructions for EventGate

Purpose
AWS Lambda event gateway that receives messages via API Gateway and dispatches them to Kafka, EventBridge, and PostgreSQL.

Structure
- Entry point: `src/event_gate_lambda.py`
- Handlers: `src/handlers/` (HandlerApi, HandlerToken, HandlerTopic, HandlerHealth)
- Writers: `src/writers/` (inherit from `Writer` base class)
- Config: `conf/config.json`, `conf/access.json`, `conf/topic_schemas/*.json`
- Terraform scripts are not part of this repository

Python style
- Python 3.13
- Type hints for public functions and classes
- Use `logging.getLogger(__name__)`, not print
- Lazy % formatting in logging: `logger.info("msg %s", var)`
- F-strings in exceptions: `raise ValueError(f"Error {var}")`
- All imports at top of file (never inside functions)

Patterns
- Classes with `__init__` cannot throw exceptions
- Writers: inherit from `Writer(ABC)`, implement `write(topic, message) -> (bool, str|None)` and `check_health() -> (bool, str)`
- Writers use lazy initialization
- Route dispatch via `ROUTES` dict mapping routes to handler functions
- Preserve existing formatting and conventions
- Keep API Gateway response structure stable: `{"statusCode": int, "headers": {...}, "body": "..."}`
- Keep error response format stable: `{"success": false, "statusCode": int, "errors": [...]}`

Testing
- Mirror src structure: `src/handlers/` -> `tests/handlers/`
- Mock external services via `conftest.py`: Kafka, EventBridge, PostgreSQL, S3
- No real API/DB calls in unit tests
- Use `mocker.patch("module.dependency")` or `mocker.patch.object(Class, "method")`
- Assert pattern: `assert expected == actual`

Quality gates (run after changes, fix only if below threshold)
- black .
- mypy .
- pylint $(git ls-files '*.py') >= 9.5
- pytest tests/ >= 80% coverage
