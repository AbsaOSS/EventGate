Copilot instructions for EventGate

Purpose
AWS Lambda event gateway that receives messages via API Gateway and dispatches them to Kafka, EventBridge, and PostgreSQL.

Structure
- Main lambda (event ingestion): `src/event_gate_lambda.py`
- Stats lambda (read-only queries): `src/event_stats_lambda.py`
- Shared config loading: `src/utils/config_loader.py`
- Handlers: `src/handlers/` (HandlerApi, HandlerToken, HandlerTopic, HandlerHealth, HandlerStats)
- Writers: `src/writers/` (inherit from `Writer` base class)
- Readers: `src/readers/` (read-only database access for stats)
- Config: `conf/config.json`, `conf/access.json`, `conf/topic_schemas/*.json`
- Production Terraform scripts are not part of this repository; `terraform_examples/` for reference configurations only

Python style
- Python 3.13
- Type hints for public functions and classes
- Use built-in generics for type hints
- Use `logging.getLogger(__name__)`, not print
- Lazy % formatting in logging: `logger.info("msg %s", var)`
- F-strings in exceptions: `raise ValueError(f"Error {var}")`
- All imports at top of file (never inside functions)
- Apache 2.0 license header in every .py file (including `__init__.py`)
- Docstrings must start with a short summary line
- No blank lines between docstring sections (summary, Args, Returns, Raises)
- Use single backticks in docstrings (`value`), never double backticks (`` ``value`` ``)
- Do not use `# -----------` separator comments to divide sections
- End all log messages with a period: `logger.info("Message.")`

Patterns
- `__init__` methods must not raise exceptions; defer validation and connection to first use (lazy init)
- Writers: inherit from `Writer(ABC)`, implement `write(topic, message) -> (bool, str|None)` and `check_health() -> (bool, str)`
- Route dispatch via `ROUTE_MAP` dict mapping routes to handler functions in `event_gate_lambda.py` and `event_stats_lambda.py`
- Separate business logic from environment access (env vars, file I/O, network calls)
- No duplicate validation; centralize parsing in one layer where practical
- Preserve existing formatting and conventions
- Keep API Gateway response structure stable: `{"statusCode": int, "headers": {...}, "body": "..."}`
- Keep error response format stable: `{"success": false, "statusCode": int, "errors": [...]}`

Testing
- Mirror src structure: `src/handlers/` -> `tests/unit/handlers/`
- Test modules (`test_*.py`) must not have module-level docstrings
- Unit tests: mock external services via `conftest.py` (Kafka, EventBridge, PostgreSQL, S3)
- Integration tests: call `lambda_handler` directly with real containers (testcontainers-python for Kafka, PostgreSQL, LocalStack)
- No real API/DB calls in unit tests
- Use `mocker.patch("module.dependency")` or `mocker.patch.object(Class, "method")`
- Assert pattern: `assert expected == actual`

Quality gates (run after changes, fix only if below threshold)
- Always run quality gates using the project virtual environment: `.venv/bin/python -m <tool>`
- Once a quality gate passes, do not re-run it in different scenarios
- black .
- mypy .
- pylint $(git ls-files '*.py') >= 9.5
- pytest tests/ >= 80% coverage
