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
from glob import glob
import pytest

try:
    import yaml
except ImportError:
    yaml = None

WORKFLOWS_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".github", "workflows")
EXPECTED_PYTHON_VERSION = "3.11"


def load_yaml(path):
    """Load and parse a YAML file."""
    if yaml is None:
        pytest.skip("PyYAML not installed")
    with open(path, "r") as f:
        return yaml.safe_load(f)


@pytest.fixture(scope="module")
def workflow_files():
    """Get all workflow YAML files."""
    files = glob(os.path.join(WORKFLOWS_DIR, "*.yml")) + glob(os.path.join(WORKFLOWS_DIR, "*.yaml"))
    assert files, "No workflow files found in .github/workflows/"
    return files


def test_workflow_files_exist():
    """Ensure critical workflow files exist."""
    required_workflows = [
        "test.yml",
        "check_terraform.yml",
        "check_pr_release_notes.yml",
        "release_draft.yml",
    ]
    for workflow in required_workflows:
        path = os.path.join(WORKFLOWS_DIR, workflow)
        assert os.path.exists(path), f"Required workflow {workflow} not found"


def test_workflows_are_valid_yaml(workflow_files):
    """Validate that all workflow files contain valid YAML."""
    for path in workflow_files:
        data = load_yaml(path)
        assert data is not None, f"{path} is empty or invalid YAML"
        assert isinstance(data, dict), f"{path} must be a YAML mapping (object)"


def test_workflows_have_name(workflow_files):
    """Ensure all workflows have a name field."""
    for path in workflow_files:
        data = load_yaml(path)
        assert "name" in data, f"{path} missing 'name' field"
        assert isinstance(data["name"], str), f"{path} 'name' must be a string"
        assert data["name"].strip(), f"{path} 'name' cannot be empty"


def test_workflows_have_on_trigger(workflow_files):
    """Ensure all workflows have an 'on' trigger."""
    for path in workflow_files:
        data = load_yaml(path)
        assert "on" in data, f"{path} missing 'on' trigger"
        trigger = data["on"]
        assert trigger, f"{path} 'on' trigger cannot be empty"


def test_workflows_have_jobs(workflow_files):
    """Ensure all workflows have at least one job."""
    for path in workflow_files:
        data = load_yaml(path)
        assert "jobs" in data, f"{path} missing 'jobs' section"
        jobs = data["jobs"]
        assert isinstance(jobs, dict), f"{path} 'jobs' must be a mapping"
        assert len(jobs) > 0, f"{path} must have at least one job"


def test_python_version_consistency(workflow_files):
    """Verify Python version is consistent across all workflows."""
    for path in workflow_files:
        data = load_yaml(path)
        jobs = data.get("jobs", {})
        
        for job_name, job_spec in jobs.items():
            steps = job_spec.get("steps", [])
            for step in steps:
                # Check for setup-python actions
                if isinstance(step.get("uses"), str) and "setup-python@" in step["uses"]:
                    with_config = step.get("with", {})
                    if "python-version" in with_config:
                        version = with_config["python-version"]
                        assert version == EXPECTED_PYTHON_VERSION, (
                            f"{path}: job '{job_name}' uses Python {version}, "
                            f"expected {EXPECTED_PYTHON_VERSION}"
                        )


def test_test_workflow_structure():
    """Validate the structure of test.yml workflow."""
    path = os.path.join(WORKFLOWS_DIR, "test.yml")
    data = load_yaml(path)
    
    assert data["name"] == "Test", "test.yml should be named 'Test'"
    
    jobs = data["jobs"]
    expected_jobs = ["unit-tests", "mypy-check", "black-check", "pylint-check"]
    for job in expected_jobs:
        assert job in jobs, f"test.yml missing required job: {job}"


def test_check_terraform_workflow_structure():
    """Validate the structure of check_terraform.yml workflow."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    assert data["name"] == "Static Terraform Check"
    
    jobs = data["jobs"]
    assert "trivy" in jobs, "check_terraform.yml missing 'trivy' job"
    assert "tflint" in jobs, "check_terraform.yml missing 'tflint' job"
    
    # Verify Trivy job steps
    trivy_steps = jobs["trivy"]["steps"]
    step_names = [step.get("name", step.get("uses", "")) for step in trivy_steps]
    assert any("Trivy" in name for name in step_names), "Trivy job missing Trivy scan step"
    
    # Verify TFLint job steps
    tflint_steps = jobs["tflint"]["steps"]
    step_names = [step.get("name", step.get("uses", "")) for step in tflint_steps]
    assert any("TFLint" in name for name in step_names), "TFLint job missing TFLint run step"


def test_check_terraform_workflow_triggers():
    """Validate triggers for check_terraform.yml."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    triggers = data["on"]
    assert "pull_request" in triggers, "check_terraform.yml should trigger on pull_request"
    assert "push" in triggers, "check_terraform.yml should trigger on push"
    assert "workflow_dispatch" in triggers, "check_terraform.yml should support manual dispatch"
    
    # Verify path filters
    pr_config = triggers["pull_request"]
    assert "paths" in pr_config, "pull_request should have path filters"
    assert "terraform/**" in pr_config["paths"], "Should filter for terraform directory"
    
    push_config = triggers["push"]
    assert "paths" in push_config, "push should have path filters"
    assert "terraform/**" in push_config["paths"], "Should filter for terraform directory"


def test_check_terraform_concurrency():
    """Verify concurrency settings in check_terraform.yml."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    assert "concurrency" in data, "check_terraform.yml should have concurrency settings"
    concurrency = data["concurrency"]
    assert "group" in concurrency, "Concurrency must specify a group"
    assert "cancel-in-progress" in concurrency, "Concurrency should specify cancel-in-progress"
    assert concurrency["cancel-in-progress"] is True, "cancel-in-progress should be true"


def test_check_terraform_permissions():
    """Verify permissions in check_terraform.yml."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    assert "permissions" in data, "check_terraform.yml should have permissions"
    permissions = data["permissions"]
    assert "contents" in permissions, "Must specify contents permission"
    assert "security-events" in permissions, "Must specify security-events permission"
    assert permissions["contents"] == "read", "Contents should have read permission"
    assert permissions["security-events"] == "write", "Security events should have write permission"


def test_trivy_sarif_upload():
    """Ensure Trivy results are uploaded as SARIF."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    trivy_job = data["jobs"]["trivy"]
    steps = trivy_job["steps"]
    
    # Find SARIF upload step
    sarif_upload = None
    for step in steps:
        if "upload-sarif" in step.get("uses", "").lower():
            sarif_upload = step
            break
    
    assert sarif_upload is not None, "Trivy job missing SARIF upload step"
    assert "with" in sarif_upload, "SARIF upload step missing 'with' configuration"
    assert "sarif_file" in sarif_upload["with"], "SARIF upload missing sarif_file parameter"


def test_tflint_sarif_upload():
    """Ensure TFLint results are uploaded as SARIF."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    tflint_job = data["jobs"]["tflint"]
    steps = tflint_job["steps"]
    
    # Find SARIF upload step
    sarif_upload = None
    for step in steps:
        if "upload-sarif" in step.get("uses", "").lower():
            sarif_upload = step
            break
    
    assert sarif_upload is not None, "TFLint job missing SARIF upload step"
    assert "with" in sarif_upload, "SARIF upload step missing 'with' configuration"
    assert "sarif_file" in sarif_upload["with"], "SARIF upload missing sarif_file parameter"


def test_workflows_use_checkout_v4(workflow_files):
    """Ensure workflows use actions/checkout@v4 or later."""
    for path in workflow_files:
        data = load_yaml(path)
        jobs = data.get("jobs", {})
        
        for job_name, job_spec in jobs.items():
            steps = job_spec.get("steps", [])
            for step in steps:
                if isinstance(step.get("uses"), str) and "actions/checkout@" in step["uses"]:
                    version = step["uses"].split("@")[1]
                    major_version = version.split(".")[0].lstrip("v")
                    assert major_version.isdigit(), f"Invalid version format in {path}"
                    assert int(major_version) >= 4, (
                        f"{path}: job '{job_name}' should use actions/checkout@v4 or later, "
                        f"found {step['uses']}"
                    )


def test_check_pr_release_notes_structure():
    """Validate check_pr_release_notes.yml structure."""
    path = os.path.join(WORKFLOWS_DIR, "check_pr_release_notes.yml")
    data = load_yaml(path)
    
    assert "on" in data
    assert "pull_request" in data["on"]
    
    jobs = data["jobs"]
    assert "check" in jobs, "check_pr_release_notes.yml missing 'check' job"
    
    check_job = jobs["check"]
    steps = check_job["steps"]
    
    # Should setup Python
    has_python_setup = any(
        "setup-python" in step.get("uses", "")
        for step in steps
    )
    assert has_python_setup, "check job should setup Python"
    
    # Should use release notes check action
    has_release_check = any(
        "release-notes-presence-check" in step.get("uses", "")
        for step in steps
    )
    assert has_release_check, "check job should use release-notes-presence-check action"


def test_release_draft_structure():
    """Validate release_draft.yml structure."""
    path = os.path.join(WORKFLOWS_DIR, "release_draft.yml")
    data = load_yaml(path)
    
    assert "on" in data
    assert "push" in data["on"]
    push_config = data["on"]["push"]
    assert "tags" in push_config, "release_draft should trigger on tags"
    
    jobs = data["jobs"]
    assert len(jobs) > 0, "release_draft.yml should have at least one job"


def test_workflow_runs_on_ubuntu(workflow_files):
    """Ensure all jobs run on ubuntu-latest."""
    for path in workflow_files:
        data = load_yaml(path)
        jobs = data.get("jobs", {})
        
        for job_name, job_spec in jobs.items():
            assert "runs-on" in job_spec, f"{path}: job '{job_name}' missing 'runs-on'"
            runs_on = job_spec["runs-on"]
            if isinstance(runs_on, str):
                assert "ubuntu" in runs_on.lower(), (
                    f"{path}: job '{job_name}' should run on Ubuntu"
                )


def test_workflows_have_valid_step_structure(workflow_files):
    """Validate that all workflow steps have proper structure."""
    for path in workflow_files:
        data = load_yaml(path)
        jobs = data.get("jobs", {})
        
        for job_name, job_spec in jobs.items():
            steps = job_spec.get("steps", [])
            assert isinstance(steps, list), f"{path}: job '{job_name}' steps must be a list"
            
            for i, step in enumerate(steps):
                assert isinstance(step, dict), (
                    f"{path}: job '{job_name}' step {i} must be a mapping"
                )
                # Each step should have either 'uses' or 'run'
                has_uses = "uses" in step
                has_run = "run" in step
                assert has_uses or has_run, (
                    f"{path}: job '{job_name}' step {i} must have 'uses' or 'run'"
                )


def test_trivy_severity_levels():
    """Verify Trivy scans for HIGH and CRITICAL severities."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    trivy_job = data["jobs"]["trivy"]
    steps = trivy_job["steps"]
    
    # Find Trivy scan step
    trivy_scan = None
    for step in steps:
        if step.get("name") and "Trivy" in step["name"] and "scan" in step["name"].lower():
            trivy_scan = step
            break
    
    assert trivy_scan is not None, "Trivy scan step not found"
    assert "run" in trivy_scan, "Trivy scan should be a run command"
    
    run_command = trivy_scan["run"]
    assert "HIGH" in run_command or "CRITICAL" in run_command, (
        "Trivy should scan for HIGH and CRITICAL severities"
    )


def test_terraform_working_directory():
    """Verify Terraform checks use correct working directory."""
    path = os.path.join(WORKFLOWS_DIR, "check_terraform.yml")
    data = load_yaml(path)
    
    jobs = data["jobs"]
    
    # Check Trivy job
    trivy_steps = jobs["trivy"]["steps"]
    for step in trivy_steps:
        if "Trivy" in step.get("name", ""):
            if "working-directory" in step:
                assert step["working-directory"] == "terraform", (
                    "Trivy should use terraform working directory"
                )
    
    # Check TFLint job
    tflint_steps = jobs["tflint"]["steps"]
    for step in tflint_steps:
        if "TFLint" in step.get("name", ""):
            if "working-directory" in step:
                assert step["working-directory"] == "terraform", (
                    "TFLint should use terraform working directory"
                )