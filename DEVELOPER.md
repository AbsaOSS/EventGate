# Event Gate—for Developers

- [Get Started](#get-started)
- [Set Up Python Environment](#set-up-python-environment)
- [Run Pylint Tool Locally](#run-pylint-tool-locally)
- [Run Black Tool Locally](#run-black-tool-locally)
- [Run mypy Tool Locally](#run-mypy-tool-locally)
- [Run TFLint Tool Locally](#run-tflint-tool-locally)
- [Run Trivy Tool Locally](#run-trivy-tool-locally)
- [Run Unit Test](#running-unit-test)
- [Code Coverage](#code-coverage)

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

## Run TFLint Tool Locally

This project uses the [TFLint](https://github.com/terraform-linters/tflint) tool for static analysis of Terraform code.
We are forcing to eliminate **all** errors reported by TFLint. Any detected warnings and notices should be corrected as well as a best practice.

- Find possible errors (like invalid instance types) for Major Cloud providers (AWS/Azure/GCP).
- Warn about deprecated syntax, unused declarations. 
- Enforce best practices, naming conventions.

> For installation instructions, please refer to the [following link.](https://github.com/terraform-linters/tflint)

### Run TFLint

For running TFLint you need to be in the `terraform/` directory. From the root file run the following commands:
```shell
cd terraform
tflint --init
tflint
cd ..
```

## Run Trivy Tool Locally

This project uses the [Trivy](https://trivy.dev/latest/) tool to scan Infrastructure as Code (terraform files) for security issues and misconfigurations.
It is an open‑source security scanner maintained by Aqua Security (AquaSec).

> For installation instructions, please refer to the [following link.](https://trivy.dev/latest/getting-started/installation/)

### Run Trivy

For running Trivy tool locally run the following command from the root file:
```shell
trivy config terraform/ # Default table output (all severities)
trivy config --severity HIGH,CRITICAL terraform/ # Show only HIGH and CRITICAL severities
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
