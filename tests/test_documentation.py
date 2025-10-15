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


def test_developer_md_exists():
    """Ensure DEVELOPER.md exists."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    assert os.path.exists(path), "DEVELOPER.md not found"


def test_developer_md_has_content():
    """Verify DEVELOPER.md is not empty."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    with open(path, "r") as f:
        content = f.read()

    assert content.strip(), "DEVELOPER.md should not be empty"
    assert len(content) > 100, "DEVELOPER.md should have substantial content"


def test_developer_md_has_sections():
    """Verify DEVELOPER.md has proper markdown sections."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    with open(path, "r") as f:
        content = f.read()

    # Should have markdown headers
    assert re.search(
        r"^#{1,3}\s+\w+", content, re.MULTILINE
    ), "DEVELOPER.md should have markdown sections (headers)"


def test_developer_md_clone_instructions():
    """Verify DEVELOPER.md includes repository clone instructions."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    with open(path, "r") as f:
        content = f.read()

    assert "git clone" in content, "DEVELOPER.md should include git clone instructions"
    assert "EventGate" in content, "DEVELOPER.md should reference EventGate repository"


def test_developer_md_python_venv_setup():
    """Verify DEVELOPER.md includes Python virtual environment setup."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    with open(path, "r") as f:
        content = f.read()

    # Should mention venv setup
    assert (
        "venv" in content
        or "virtual environment" in content.lower()
    ), "DEVELOPER.md should include virtual environment setup"

    # Should have command to create venv
    assert (
        "python3 -m venv" in content
        or "python -m venv" in content
    ), "DEVELOPER.md should include venv creation command"


def test_developer_md_no_outdated_python_version():
    """Ensure DEVELOPER.md doesn't reference outdated Python version."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    with open(path, "r") as f:
        content = f.read()

    # Should not mention Python 3.13 as requirement
    assert (
        "Python 3.13" not in content
    ), "DEVELOPER.md should not reference Python 3.13 after downgrade to 3.11"

    # Should not have a Prerequisites section mentioning specific Python version
    # (as this was removed in the diff)
    prerequisites_section = re.search(
        r"##\s+Prerequisites.*?(?=##|$)",
        content,
        re.DOTALL | re.IGNORECASE,
    )

    if prerequisites_section:
        section_content = prerequisites_section.group(0)
        # If Prerequisites section exists, it should not mandate a specific Python version
        assert (
            "Python 3.13" not in section_content
        ), "Prerequisites section should not require Python 3.13"


def test_developer_md_code_blocks():
    """Verify DEVELOPER.md has properly formatted code blocks."""
    path = os.path.join(PROJECT_ROOT, "DEVELOPER.md")
    with open(path, "r") as f:
        content = f.read()

    # Should have code blocks (triple backticks)
    assert "```" in content, "DEVELOPER.md should have code blocks"

    # Code blocks should be balanced
    code_block_markers = content.count("```")
    assert code_block_markers % 2 == 0, "DEVELOPER.md has unbalanced code blocks"


def test_readme_exists():
    """Ensure README.md exists."""
    path = os.path.join(PROJECT_ROOT, "README.md")
    assert os.path.exists(path), "README.md not found"


def test_readme_has_title():
    """Verify README.md has a title."""
    path = os.path.join(PROJECT_ROOT, "README.md")
    with open(path, "r") as f:
        content = f.read()

    # Should start with a level 1 heading
    assert re.match(
        r"^#\s+\w+", content
    ), "README.md should start with a title (# heading)"


def test_markdown_files_valid_links():
    """Check for obviously broken links in markdown files."""
    markdown_files = [
        os.path.join(PROJECT_ROOT, "README.md"),
        os.path.join(PROJECT_ROOT, "DEVELOPER.md"),
    ]

    for path in markdown_files:
        if not os.path.exists(path):
            continue

        with open(path, "r") as f:
            content = f.read()

        # Find markdown links [text](url)
        links = re.findall(r"\[([^\]]+)\]\(([^)]+)\)", content)

        for _text, url in links:
            # Check for relative file links
            if not url.startswith(("http://", "https://", "#", "mailto:")):
                # It's a relative path
                if url.startswith("/"):
                    # Absolute path from repo root
                    file_path = os.path.join(PROJECT_ROOT, url.lstrip("/"))
                else:
                    # Relative to markdown file
                    file_path = os.path.join(os.path.dirname(path), url)

                # Remove any anchor
                file_path = file_path.split("#")[0]

                if file_path:
                    assert os.path.exists(
                        file_path
                    ), f"{os.path.basename(path)} has broken link: {url} -> {file_path}"


def test_license_file_exists():
    """Ensure LICENSE file exists."""
    path = os.path.join(PROJECT_ROOT, "LICENSE")
    assert os.path.exists(path), "LICENSE file not found"


def test_license_is_apache():
    """Verify LICENSE is Apache License 2.0."""
    path = os.path.join(PROJECT_ROOT, "LICENSE")
    with open(path, "r") as f:
        content = f.read()

    assert "Apache License" in content, "LICENSE should be Apache License"
    assert "Version 2.0" in content, "LICENSE should be version 2.0"


def test_python_files_have_license_header():
    """Verify Python files have Apache license header."""
    # Check a few key Python files
    src_dir = os.path.join(PROJECT_ROOT, "src")
    test_dir = os.path.join(PROJECT_ROOT, "tests")

    python_files = []

    # Get Python files from src
    if os.path.exists(src_dir):
        for root, _dirs, files in os.walk(src_dir):
            for file in files:
                if file.endswith(".py"):
                    python_files.append(os.path.join(root, file))

    # Get a sample of test files
    if os.path.exists(test_dir):
        for file in os.listdir(test_dir):
            if file.endswith(".py") and file.startswith("test_"):
                python_files.append(os.path.join(test_dir, file))
                if len([f for f in python_files if test_dir in f]) >= 3:
                    break

    for path in python_files:
        with open(path, "r") as f:
            content = f.read(1000)  # Read first 1000 chars

        # Should have copyright notice
        assert "Copyright" in content, f"{path} missing copyright notice"
        assert (
            "ABSA Group Limited" in content
        ), f"{path} missing ABSA Group Limited in copyright"
        assert "Apache License" in content, f"{path} missing Apache License reference"