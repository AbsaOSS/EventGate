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
import pytest

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
EXPECTED_PYTHON_VERSION = "3.11"


def test_pyproject_toml_exists():
    """Ensure pyproject.toml exists."""
    path = os.path.join(PROJECT_ROOT, "pyproject.toml")
    assert os.path.exists(path), "pyproject.toml not found"


def test_pyproject_toml_black_config():
    """Validate Black configuration in pyproject.toml."""
    path = os.path.join(PROJECT_ROOT, "pyproject.toml")
    with open(path, "r") as f:
        content = f.read()
    
    assert "[tool.black]" in content, "pyproject.toml missing [tool.black] section"
    assert "line-length = 120" in content, "Black line-length should be 120"
    assert f"target-version = ['py{EXPECTED_PYTHON_VERSION.replace('.', '')}']" in content, (
        f"Black target-version should be py{EXPECTED_PYTHON_VERSION.replace('.', '')}"
    )


def test_pyproject_toml_coverage_config():
    """Validate coverage configuration in pyproject.toml."""
    path = os.path.join(PROJECT_ROOT, "pyproject.toml")
    with open(path, "r") as f:
        content = f.read()
    
    assert "[tool.coverage.run]" in content, "pyproject.toml missing [tool.coverage.run] section"
    assert 'omit = ["tests/*"]' in content, "Coverage should omit tests directory"


def test_pyproject_toml_mypy_config():
    """Validate mypy configuration in pyproject.toml."""
    path = os.path.join(PROJECT_ROOT, "pyproject.toml")
    with open(path, "r") as f:
        content = f.read()
    
    assert "[tool.mypy]" in content, "pyproject.toml missing [tool.mypy] section"
    assert "check_untyped_defs = true" in content, "mypy should check untyped defs"
    assert 'exclude = "tests"' in content, "mypy should exclude tests"
    assert "ignore_missing_imports = true" in content, "mypy should ignore missing imports"
    assert f'python_version = "{EXPECTED_PYTHON_VERSION}"' in content, (
        f"mypy python_version should be {EXPECTED_PYTHON_VERSION}"
    )
    assert 'packages = ["src"]' in content, "mypy should check src package"
    assert "explicit_package_bases = true" in content, "mypy should use explicit package bases"


def test_requirements_txt_exists():
    """Ensure requirements.txt exists."""
    path = os.path.join(PROJECT_ROOT, "requirements.txt")
    assert os.path.exists(path), "requirements.txt not found"


def test_requirements_txt_format():
    """Validate requirements.txt format and content."""
    path = os.path.join(PROJECT_ROOT, "requirements.txt")
    with open(path, "r") as f:
        lines = f.readlines()
    
    # Should not be empty
    assert len(lines) > 0, "requirements.txt should not be empty"
    
    # Check for required packages
    content = "".join(lines)
    required_packages = [
        "pytest",
        "pytest-cov",
        "pytest-mock",
        "pylint",
        "black",
        "mypy",
        "boto3",
        "PyJWT",
        "requests",
        "psycopg2",
    ]
    
    for package in required_packages:
        assert package in content, f"requirements.txt missing {package}"


def test_requirements_txt_no_commented_alternatives():
    """Ensure no commented alternative packages in requirements.txt."""
    path = os.path.join(PROJECT_ROOT, "requirements.txt")
    with open(path, "r") as f:
        lines = f.readlines()
    
    # Should not have commented psycopg2-binary
    for line in lines:
        if line.strip().startswith("#"):
            assert "psycopg2-binary" not in line, (
                "Commented psycopg2-binary should be removed from requirements.txt"
            )


def test_requirements_txt_pinned_versions():
    """Verify all packages have pinned versions."""
    path = os.path.join(PROJECT_ROOT, "requirements.txt")
    with open(path, "r") as f:
        lines = f.readlines()
    
    for line in lines:
        line = line.strip()
        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue
        
        # Each line should have == for version pinning
        assert "==" in line, f"Package should have pinned version: {line}"
        
        # Verify version format
        parts = line.split("==")
        assert len(parts) == 2, f"Invalid package format: {line}"
        package_name, version = parts
        assert package_name.strip(), f"Empty package name: {line}"
        assert version.strip(), f"Empty version: {line}"
        
        # Version should be a valid semantic version
        version_pattern = r'^\d+\.\d+(\.\d+)?$'
        assert re.match(version_pattern, version.strip()), (
            f"Invalid version format for {package_name}: {version}"
        )


def test_requirements_txt_psycopg2_not_binary():
    """Ensure psycopg2 (not binary) is used in requirements."""
    path = os.path.join(PROJECT_ROOT, "requirements.txt")
    with open(path, "r") as f:
        content = f.read()
    
    # Should have psycopg2 without -binary suffix
    assert "psycopg2==" in content, "requirements.txt should include psycopg2"
    assert "psycopg2-binary" not in content, (
        "requirements.txt should not use psycopg2-binary for production"
    )


def test_gitignore_exists():
    """Ensure .gitignore exists."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    assert os.path.exists(path), ".gitignore not found"


def test_gitignore_python_patterns():
    """Validate Python-related patterns in .gitignore."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    with open(path, "r") as f:
        content = f.read()
    
    required_patterns = [
        "__pycache__",
        ".venv",
        ".ipynb_checkpoints",
    ]
    
    for pattern in required_patterns:
        assert pattern in content, f".gitignore missing pattern: {pattern}"


def test_gitignore_terraform_patterns():
    """Validate Terraform-related patterns in .gitignore."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    with open(path, "r") as f:
        content = f.read()
    
    terraform_patterns = [
        "/terraform/*.tfvars",
        "/terraform/*.tfstate",
        "/terraform/.terraform",
    ]
    
    for pattern in terraform_patterns:
        assert pattern in content, f".gitignore missing Terraform pattern: {pattern}"


def test_gitignore_sarif_files():
    """Ensure .sarif files are ignored."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    with open(path, "r") as f:
        content = f.read()
    
    assert "*.sarif" in content, ".gitignore should ignore SARIF files"


def test_gitignore_build_artifacts():
    """Validate build artifacts are ignored."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    with open(path, "r") as f:
        content = f.read()
    
    build_patterns = [
        "/dependencies",
        "/lambda_function.zip",
    ]
    
    for pattern in build_patterns:
        assert pattern in content, f".gitignore missing build pattern: {pattern}"


def test_gitignore_ide_patterns():
    """Validate IDE-related patterns in .gitignore."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    with open(path, "r") as f:
        content = f.read()
    
    assert "/.idea/" in content, ".gitignore should ignore .idea directory"


def test_gitignore_organized_sections():
    """Verify .gitignore has organized sections with comments."""
    path = os.path.join(PROJECT_ROOT, ".gitignore")
    with open(path, "r") as f:
        content = f.read()
    
    # Check for section comments
    assert "# Terraform" in content, ".gitignore should have Terraform section comment"


def test_codeowners_exists():
    """Ensure CODEOWNERS file exists."""
    path = os.path.join(PROJECT_ROOT, ".github", "CODEOWNERS")
    assert os.path.exists(path), "CODEOWNERS file not found"


def test_codeowners_format():
    """Validate CODEOWNERS file format."""
    path = os.path.join(PROJECT_ROOT, ".github", "CODEOWNERS")
    with open(path, "r") as f:
        lines = f.readlines()
    
    # Should not be empty
    assert len(lines) > 0, "CODEOWNERS should not be empty"
    
    for line in lines:
        line = line.strip()
        # Skip empty lines and comments
        if not line or line.startswith("#"):
            continue
        
        # Each line should have pattern and at least one owner
        parts = line.split()
        assert len(parts) >= 2, f"CODEOWNERS line must have pattern and owner(s): {line}"
        
        # Pattern should be first
        pattern = parts[0]
        assert pattern, "CODEOWNERS pattern cannot be empty"
        
        # Owners should start with @
        owners = parts[1:]
        for owner in owners:
            assert owner.startswith("@"), f"CODEOWNERS owner must start with @: {owner}"


def test_codeowners_default_pattern():
    """Verify CODEOWNERS has a default pattern."""
    path = os.path.join(PROJECT_ROOT, ".github", "CODEOWNERS")
    with open(path, "r") as f:
        content = f.read()
    
    # Should have a default pattern (*)
    assert content.strip().startswith("*"), "CODEOWNERS should start with default pattern (*)"


def test_python_version_consistency():
    """Verify Python version is consistent across configuration files."""
    # Check pyproject.toml
    pyproject_path = os.path.join(PROJECT_ROOT, "pyproject.toml")
    with open(pyproject_path, "r") as f:
        pyproject_content = f.read()
    
    # Extract Python version from Black config
    black_version_match = re.search(r"target-version = \['py(\d+)'\]", pyproject_content)
    assert black_version_match, "Could not find Black target-version in pyproject.toml"
    black_version = f"{black_version_match.group(1)[0]}.{black_version_match.group(1)[1:]}"
    
    # Extract Python version from mypy config
    mypy_version_match = re.search(r'python_version = "(\d+\.\d+)"', pyproject_content)
    assert mypy_version_match, "Could not find mypy python_version in pyproject.toml"
    mypy_version = mypy_version_match.group(1)
    
    # Both should match expected version
    assert black_version == EXPECTED_PYTHON_VERSION, (
        f"Black target-version {black_version} doesn't match expected {EXPECTED_PYTHON_VERSION}"
    )
    assert mypy_version == EXPECTED_PYTHON_VERSION, (
        f"mypy python_version {mypy_version} doesn't match expected {EXPECTED_PYTHON_VERSION}"
    )