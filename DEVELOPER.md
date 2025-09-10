# Event Gate—for Developers

- [Get Started](#get-started)
- [Set Up Python Environment](#set-up-python-environment)
- [Run Static Code Analysis](#running-static-code-analysis)
- [Run Black Tool Locally](#run-black-tool-locally)
- [Run mypy Tool Locally](#run-mypy-tool-locally)
- [Run Unit Test](#running-unit-test)

## Get Started

Clone the repository and navigate to the project directory:

```shell
git clone https://github.com/AbsaOSS/EventGate.git
cd EventGate
```

## Set Up Python Environment
```shell
python3 -m venv .venv
source .venv/bin/activate
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
