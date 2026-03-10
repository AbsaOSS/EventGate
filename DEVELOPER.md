# EventGate for Developers

- [Get Started](#get-started)
- [Set Up Python Environment](#set-up-python-environment)
- [Run Pylint Tool Locally](#run-pylint-tool-locally)
- [Run Black Tool Locally](#run-black-tool-locally)
- [Run mypy Tool Locally](#run-mypy-tool-locally)
- [Run Unit Test Locally](#run-unit-test-locally)
- [Code Coverage](#code-coverage)
- [Run Integration Test Locally](#run-integration-test-locally)

## Get Started

Clone the repository and navigate to the project directory:

```shell
git clone https://github.com/AbsaOSS/EventGate.git
cd EventGate
```

### Project Structure
EventGate ships two Lambda functions:
- **Event Gate Lambda** (`src/event_gate_lambda.py`) — the main API surface. Serves the OpenAPI spec, token provider redirect, health check, topic schema catalogue, and event ingestion (`POST /topics/{topicName}`). Ingestion includes JWT authorization, JSON Schema validation, and parallel fanout to Kafka, EventBridge, and PostgreSQL.
- **Event Stats Lambda** (`src/event_stats_lambda.py`) — serves read-only queries via `POST /stats/{topicName}` with filtering, sorting, and cursor-based pagination backed by PostgreSQL.

## Prerequisites
- Python 3.13 (current required runtime)
- PostgreSQL client dev package
  - For local development, you may temporarily switch to the commented `psycopg2-binary==2.9.10` in `requirements.txt`.

## Set Up Python Environment
```shell
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
```

## Run Pylint Tool Locally

This project uses the Pylint tool for static code analysis. Pylint analyzes your code without actually running it. It checks for errors, enforces coding standards, looks for code smells, etc.

Pylint displays a global evaluation score for the code, rated out of a maximum score of 10.0. We aim to keep our code quality above 9.5.

### Run Pylint
Run Pylint on all files currently tracked by Git in the project.
```shell
pylint $(git ls-files '*.py')
```

To run Pylint on a specific file, follow the pattern `pylint <path_to_file>/<name_of_file>.py`.

Example:
```shell
pylint src/event_gate_lambda.py
``` 

## Run Black Tool Locally
This project uses the [Black](https://github.com/psf/black) tool for code formatting.
Black aims for consistency, generality, readability, and reducing git diffs.
The coding style used can be viewed as a strict subset of PEP 8.

The project root file `pyproject.toml` defines the Black tool configuration.
In this project, we are accepting a line length of 120 characters.

Follow these steps to format your code with Black locally:

### Run Black
Run Black on all files currently tracked by Git in the project.
```shell
black $(git ls-files '*.py')
```

To run Black on a specific file, follow the pattern `black <path_to_file>/<name_of_file>.py`.

Example:
```shell
black src/writers/writer_kafka.py
``` 

### Expected Output
This is the console's expected output example after running the tool:
```
All done! ✨ 🍰 ✨
1 file reformatted.
```

## Run mypy Tool Locally

This project uses the [mypy](https://mypy.readthedocs.io/en/stable/) tool, a static type checker for Python.

> Type checkers help ensure that you correctly use variables and functions in your code.
> With mypy, add type hints (PEP 484) to your Python programs,
> and mypy will warn you when you use those types incorrectly.
mypy configuration is in `pyproject.toml` file.

Follow these steps to type-check your code with mypy locally:

### Run mypy

Run mypy on all files in the project.
```shell
mypy .
```

To run mypy on a specific file, follow the pattern `mypy <path_to_file>/<name_of_file>.py --check-untyped-defs`.

Example:
```shell
mypy src/handlers/handler_token.py
```

## Run Unit Test Locally

Unit tests are written using pytest. To run the tests, use the following command:

```shell
pytest tests/unit/
```

This will execute all unit tests located in the tests/unit/ directory.

### Focused / Selective Test Runs
Run a single test file:
```shell
pytest tests/unit/writers/test_writer_kafka.py
```
Filter by keyword expression:
```shell
pytest -k kafka
```
Run a single test function (node id):
```shell
pytest tests/unit/writers/test_writer_eventbridge.py::test_write_success
```

## Code Coverage

Code coverage is collected using the pytest-cov coverage tool. To run the tests and collect coverage information, use the following command:

```shell
pytest --cov=. -v tests/unit/ --cov-fail-under=80 --cov-report=html
```

This will execute all tests in the tests directory and generate a code coverage report with missing line details and enforce a minimum 80% threshold.

Open the HTML coverage report:
```shell
open htmlcov/index.html
```

## Run Integration Test Locally

Integration tests validate EventGate against real service dependencies using testcontainers-python.

### Integration Test Approach

EventGate uses a **direct invocation approach** for integration testing:
- **Lambda handler is called directly** in Python (not run in a container)
- **External dependencies run in Docker containers**: Kafka, PostgreSQL, LocalStack (EventBridge)
- **Mock JWT provider runs in-process** as a background thread (no container)
- Test configuration is dynamically generated and injected via environment variables

### Prerequisites
- Docker running (Docker Desktop on macOS/Windows, or Docker Engine on Linux)
- Python 3.13 with dependencies installed

### Run Integration Tests

Containers start and stop automatically:
```shell
pytest tests/integration/ -v
```

With detailed logging:
```shell
pytest tests/integration/ -v --log-cli-level=INFO
```

### Run Specific Integration Tests

Run a single test file:
```shell
pytest tests/integration/test_health_endpoint.py -v
```

Run a specific test function:
```shell
pytest tests/integration/test_topics_endpoint.py::TestPostEventEndpoint::test_post_event_with_valid_token_returns_202 -v
```

### Troubleshooting

If containers fail to start, check Docker is running:
```shell
docker info
```

If image pulls fail with TLS or timeout errors, pre-pull the required images manually:
```shell
docker pull testcontainers/ryuk:0.8.1
docker pull postgres:16
docker pull confluentinc/cp-kafka:7.6.0
docker pull localstack/localstack:latest
```

View container logs in pytest output by increasing log level:
```shell
pytest tests/integration/ -v --log-cli-level=DEBUG
```
