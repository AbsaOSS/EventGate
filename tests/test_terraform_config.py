#
# Copyright 2025 ABSA Group Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import re
from glob import glob

import pytest

TERRAFORM_DIR = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "terraform"
)
EXPECTED_PYTHON_RUNTIME = "python3.11"


@pytest.fixture(scope="module")
def terraform_files():
    """Get all Terraform configuration files."""
    files = glob(os.path.join(TERRAFORM_DIR, "*.tf"))
    assert files, "No Terraform files found"
    return files


def test_terraform_directory_exists():
    """Ensure terraform directory exists."""
    assert os.path.exists(TERRAFORM_DIR), "terraform directory not found"
    assert os.path.isdir(TERRAFORM_DIR), "terraform path is not a directory"


def test_lambda_tf_exists():
    """Ensure lambda.tf exists."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    assert os.path.exists(path), "lambda.tf not found"


def test_lambda_tf_python_runtime():
    """Verify Lambda function uses correct Python runtime."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Should have runtime specification
    assert "runtime" in content, "lambda.tf should specify runtime"

    # Extract runtime value
    runtime_match = re.search(r'runtime\s*=\s*"([^"]+)"', content)
    assert runtime_match, "Could not find runtime value in lambda.tf"

    runtime = runtime_match.group(1)
    assert runtime == EXPECTED_PYTHON_RUNTIME, (
        f"Lambda runtime should be {EXPECTED_PYTHON_RUNTIME}, found {runtime}"
    )


def test_lambda_tf_resource_structure():
    """Validate Lambda resource structure in lambda.tf."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Should have Lambda function resource
    assert 'resource "aws_lambda_function"' in content, (
        "lambda.tf should define aws_lambda_function resource"
    )

    # Extract resource name
    resource_match = re.search(
        r'resource "aws_lambda_function" "([^"]+)"', content
    )
    assert resource_match, "Could not find Lambda function resource name"

    resource_name = resource_match.group(1)
    assert resource_name, "Lambda function resource name should not be empty"


def test_lambda_tf_required_attributes():
    """Verify Lambda function has required attributes."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    required_attributes = [
        "function_name",
        "handler",
        "role",
        "runtime",
        "timeout",
    ]

    for attr in required_attributes:
        assert f"{attr}" in content, f"lambda.tf should specify {attr}"


def test_lambda_tf_architecture():
    """Verify Lambda function architecture setting."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Should specify architectures
    assert "architectures" in content, "lambda.tf should specify architectures"

    # Should use x86_64
    assert "x86_64" in content, "Lambda should use x86_64 architecture"


def test_lambda_tf_timeout():
    """Verify Lambda function timeout is reasonable."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Extract timeout value
    timeout_match = re.search(r"timeout\s*=\s*(\d+)", content)
    assert timeout_match, "Could not find timeout value in lambda.tf"

    timeout = int(timeout_match.group(1))
    assert timeout > 0, "Lambda timeout must be positive"
    assert timeout <= 900, "Lambda timeout cannot exceed 15 minutes (900 seconds)"


def test_lambda_tf_package_type():
    """Verify Lambda package_type is properly configured."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Should specify package_type
    assert "package_type" in content, "lambda.tf should specify package_type"


def test_lambda_tf_deployment_config():
    """Verify Lambda has deployment configuration."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Should have S3 bucket configuration for Zip deployments
    assert "s3_bucket" in content, "lambda.tf should have s3_bucket configuration"


def test_terraform_files_valid_syntax(terraform_files):
    """Basic syntax validation for Terraform files."""
    for path in terraform_files:
        with open(path, "r") as f:
            content = f.read()

        # Should not be empty
        assert content.strip(), f"{path} is empty"

        # Check for balanced braces
        open_braces = content.count("{")
        close_braces = content.count("}")
        assert open_braces == close_braces, (
            f"{path} has unbalanced braces: {open_braces} open, {close_braces} close"
        )


def test_terraform_files_have_resources_or_variables(terraform_files):
    """Ensure Terraform files define resources, variables, or outputs."""
    for path in terraform_files:
        with open(path, "r") as f:
            content = f.read()

        # File should have at least one of: resource, variable, output, data, locals
        has_definition = any(
            keyword in content
            for keyword in ["resource ", "variable ", "output ", "data ", "locals "]
        )
        assert has_definition, f"{path} should define resources, variables, or outputs"


def test_lambda_tf_variables_referenced():
    """Verify Lambda configuration uses variables where appropriate."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # Should use var. references for configuration
    assert "var." in content, "lambda.tf should use variables for configuration"

    # Check for specific variable usage
    expected_vars = [
        "var.lambda_role_arn",
        "var.lambda_package_type",
    ]

    for var in expected_vars:
        assert var in content, f"lambda.tf should reference {var}"


def test_lambda_tf_conditional_s3_config():
    """Verify Lambda has conditional S3 configuration based on package type."""
    path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(path, "r") as f:
        content = f.read()

    # S3 bucket should be conditional based on package_type
    assert 's3_bucket' in content, "lambda.tf should have s3_bucket"

    # Should have ternary condition for Zip vs Image
    s3_bucket_line = [line for line in content.split("\n") if "s3_bucket" in line]
    assert s3_bucket_line, "Could not find s3_bucket line"

    s3_line = s3_bucket_line[0]
    assert "?" in s3_line, "s3_bucket should use conditional expression"
    assert "Zip" in s3_line or "zip" in s3_line, (
        "s3_bucket condition should check for Zip package type"
    )


def test_terraform_no_hardcoded_sensitive_values(terraform_files):
    """Ensure no hardcoded sensitive values in Terraform files."""
    sensitive_patterns = [
        r'password\s*=\s*"[^"]+"',
        r'secret\s*=\s*"[^"]+"',
        r'api_key\s*=\s*"[^"]+"',
        r'access_key\s*=\s*"[^"]+"',
    ]

    for path in terraform_files:
        with open(path, "r") as f:
            content = f.read()

        for pattern in sensitive_patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            assert not matches, (
                f"{path} contains hardcoded sensitive values: {matches}"
            )


def test_python_runtime_consistency():
    """Verify Python runtime in Terraform matches project configuration."""
    lambda_path = os.path.join(TERRAFORM_DIR, "lambda.tf")
    with open(lambda_path, "r") as f:
        content = f.read()

    runtime_match = re.search(r'runtime\s*=\s*"python(\d+\.\d+)"', content)
    assert runtime_match, "Could not extract Python version from lambda.tf runtime"

    runtime_version = runtime_match.group(1)

    # Should match the expected Python version (3.11)
    expected_version = "3.11"
    assert runtime_version == expected_version, (
        f"Lambda runtime python{runtime_version} doesn't match expected python{expected_version}"
    )