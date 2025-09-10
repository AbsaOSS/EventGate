# Event Gate—for Developers

- [Get Started](#get-started)
- [Set Up Python Environment](#set-up-python-environment)
- [Run Static Code Analysis](#running-static-code-analysis)
- [Run Black Tool Locally](#run-black-tool-locally)
- [Run mypy Tool Locally](#run-mypy-tool-locally)
- [Run Unit Test](#running-unit-test)
- [Run Action Locally](#run-action-locally)

## Get Started

Clone the repository and navigate to the project directory:

```shell
git clone https://github.com/AbsaOSS/EventGate.git
cd EventGate
```

## Set Up Python Environment
```shell
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running Static Code Analysis

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
pylint src/writer_kafka.py
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
black src/writer_kafka.py
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
mypy src/writer_kafka.py
``` 

## Running Unit Test

Unit tests are written using pytest. To run the tests, use the following command:

```shell
pytest tests/
```

This will execute all tests located in the tests directory.

### Focused / Selective Test Runs
Run a single test file:
```shell
pytest tests/test_writer_kafka.py -q
```
Filter by keyword expression:
```shell
pytest -k kafka -q
```
Run a single test function (node id):
```shell
pytest tests/test_event_gate_lambda.py::test_post_multiple_writer_failures -q
```

## Code Coverage

Code coverage is collected using the pytest-cov coverage tool. To run the tests and collect coverage information, use the following command:

```shell
pytest --cov=. -v tests/ --cov-fail-under=80 --cov-report=term-missing
```

This will execute all tests in the tests directory and generate a code coverage report with missing line details and enforce a minimum 80% threshold.

Open the HTML coverage report:
```shell
open htmlcov/index.html
```

### Test Environment Variables
The following environment variables influence runtime behavior under test:
- LOG_LEVEL – override logging verbosity (default INFO)
- KAFKA_FLUSH_TIMEOUT – seconds passed to confluent_kafka Producer.flush(timeout) (default 5)
- POSTGRES_SECRET_NAME / POSTGRES_SECRET_REGION – when set, writer_postgres.init() fetches secret via AWS Secrets Manager (tests usually leave unset or patch boto3)

### Mocking / Isolation Strategy
- External network calls (requests.get for token public key, S3 object fetch, EventBridge put_events, Kafka producer) are monkeypatched or patched with unittest.mock before importing modules that perform side-effect initialization (notably src/event_gate_lambda).
- Tests that need to exercise alternate import-time paths (e.g., local vs S3 access_config) patch builtins.open and reload the module (see tests/test_event_gate_lambda_local_access.py).
- Optional dependencies (confluent_kafka, psycopg2) are stubbed only when truly absent using importlib.util.find_spec to avoid masking real installations.
- Kafka producer errors are simulated via callback error injection or custom stub classes; flush timeout path covered by setting KAFKA_FLUSH_TIMEOUT and custom producer returning non-zero remaining messages.

### Type Checking / Lint / Format
Run static type checks:
```shell
mypy .
```
Run lint (pylint over tracked Python files):
```shell
pylint $(git ls-files '*.py')
```
Auto-format:
```shell
black $(git ls-files '*.py')
```

### Quick CI Smoke (local approximation)
```shell
mypy . && pylint $(git ls-files '*.py') && pytest --cov=. --cov-fail-under=80 -q
```
