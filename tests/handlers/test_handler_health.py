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

import json
from unittest.mock import MagicMock

from src.handlers.handler_health import HandlerHealth


def _create_mock_writer(check_health_return):
    """Create a mock writer with check_health returning the specified value."""
    mock = MagicMock()
    mock.check_health.return_value = check_health_return
    return mock


### get_health()


## Minimal healthy state (just kafka)
def test_get_health_minimal_kafka_healthy():
    """Health check returns 200 when Kafka is initialized and optional writers are disabled."""
    writers = {
        "kafka": _create_mock_writer((True, "ok")),
        "eventbridge": _create_mock_writer((True, "not configured")),
        "postgres": _create_mock_writer((True, "not configured")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "ok"
    assert "uptime_seconds" in body


## Healthy state with all writers enabled
def test_get_health_all_writers_enabled_and_healthy():
    """Health check returns 200 when all writers are enabled and properly configured."""
    writers = {
        "kafka": _create_mock_writer((True, "ok")),
        "eventbridge": _create_mock_writer((True, "ok")),
        "postgres": _create_mock_writer((True, "ok")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "ok"
    assert "uptime_seconds" in body


## Degraded state with all writers failing
def test_get_health_kafka_not_initialized():
    """Health check returns 503 when Kafka writer is not initialized."""
    writers = {
        "kafka": _create_mock_writer((False, "producer initialization failed")),
        "eventbridge": _create_mock_writer((False, "client initialization failed")),
        "postgres": _create_mock_writer((False, "host not configured")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    assert response["statusCode"] == 503
    body = json.loads(response["body"])
    assert body["status"] == "degraded"
    assert "kafka" in body["failures"]
    assert "eventbridge" in body["failures"]
    assert "postgres" in body["failures"]


## Healthy when eventbridge is disabled
def test_get_health_eventbridge_disabled():
    """Health check returns 200 when EventBridge is disabled (empty event_bus_arn)."""
    writers = {
        "kafka": _create_mock_writer((True, "ok")),
        "eventbridge": _create_mock_writer((True, "not configured")),
        "postgres": _create_mock_writer((True, "ok")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    assert response["statusCode"] == 200


## Healthy when postgres is disabled
def test_get_health_postgres_disabled():
    """Health check returns 200 when PostgreSQL is disabled (empty database)."""
    writers = {
        "kafka": _create_mock_writer((True, "ok")),
        "eventbridge": _create_mock_writer((True, "ok")),
        "postgres": _create_mock_writer((True, "not configured")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    assert response["statusCode"] == 200


## Degraded state - postgres host not configured
def test_get_health_postgres_host_not_configured():
    """Health check returns 503 when PostgreSQL host is not configured."""
    writers = {
        "kafka": _create_mock_writer((True, "ok")),
        "eventbridge": _create_mock_writer((True, "ok")),
        "postgres": _create_mock_writer((False, "host not configured")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    assert response["statusCode"] == 503
    body = json.loads(response["body"])
    assert body["failures"]["postgres"] == "host not configured"


## Uptime calculation
def test_get_health_uptime_is_positive():
    """Verify uptime_seconds is calculated and is a positive integer."""
    writers = {
        "kafka": _create_mock_writer((True, "ok")),
        "eventbridge": _create_mock_writer((True, "ok")),
        "postgres": _create_mock_writer((True, "ok")),
    }
    handler = HandlerHealth(writers)

    response = handler.get_health()

    body = json.loads(response["body"])
    assert "uptime_seconds" in body
    assert isinstance(body["uptime_seconds"], int)
    assert body["uptime_seconds"] >= 0


## Integration test with event_gate_module
def test_health_endpoint_integration(event_gate_module, make_event):
    """Test /health endpoint through lambda_handler."""
    event = make_event("/health")
    resp = event_gate_module.lambda_handler(event)

    # Should return 200 since writers are mocked as initialized in conftest
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert body["status"] == "ok"
    assert "uptime_seconds" in body
