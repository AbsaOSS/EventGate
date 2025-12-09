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
from unittest.mock import MagicMock, patch
import logging

from src.handlers.handler_health import HandlerHealth


## get_health() - healthy state
def test_get_health_all_dependencies_healthy():
    """Health check returns 200 when all writer STATEs are properly initialized."""
    logger = logging.getLogger("test")
    config = {}
    handler = HandlerHealth(logger, config)

    # Mock all writers as healthy
    with (
        patch("src.handlers.handler_health.writer_kafka.STATE", {"logger": logger, "producer": MagicMock()}),
        patch(
            "src.handlers.handler_health.writer_eventbridge.STATE",
            {
                "logger": logger,
                "client": MagicMock(),
                "event_bus_arn": "arn:aws:events:us-east-1:123456789012:event-bus/my-bus",
            },
        ),
        patch("src.handlers.handler_health.writer_postgres.logger", logger),
    ):
        response = handler.get_health()

    assert response["statusCode"] == 200
    body = json.loads(response["body"])
    assert body["status"] == "ok"
    assert "uptime_seconds" in body
    assert isinstance(body["uptime_seconds"], int)
    assert body["uptime_seconds"] >= 0


## get_health() - degraded state - kafka
def test_get_health_kafka_not_initialized():
    """Health check returns 503 when Kafka writer is not initialized."""
    logger = logging.getLogger("test")
    config = {}
    handler = HandlerHealth(logger, config)

    # Mock Kafka as not initialized (missing producer key)
    with (
        patch("src.handlers.handler_health.writer_kafka.STATE", {"logger": logger}),
        patch(
            "src.handlers.handler_health.writer_eventbridge.STATE",
            {"logger": logger, "client": MagicMock(), "event_bus_arn": "arn"},
        ),
        patch("src.handlers.handler_health.writer_postgres.logger", logger),
    ):
        response = handler.get_health()

    assert response["statusCode"] == 503
    body = json.loads(response["body"])
    assert body["status"] == "degraded"
    assert "details" in body
    assert "kafka" in body["details"]
    assert body["details"]["kafka"] == "not_initialized"


## get_health() - degraded state - eventbridge
def test_get_health_eventbridge_not_initialized():
    """Health check returns 503 when EventBridge writer is not initialized."""
    logger = logging.getLogger("test")
    config = {}
    handler = HandlerHealth(logger, config)

    # Mock EventBridge as not initialized (missing client key)
    with (
        patch("src.handlers.handler_health.writer_kafka.STATE", {"logger": logger, "producer": MagicMock()}),
        patch("src.handlers.handler_health.writer_eventbridge.STATE", {"logger": logger}),
        patch("src.handlers.handler_health.writer_postgres.logger", logger),
    ):
        response = handler.get_health()

    assert response["statusCode"] == 503
    body = json.loads(response["body"])
    assert body["status"] == "degraded"
    assert "eventbridge" in body["details"]
    assert body["details"]["eventbridge"] == "not_initialized"


## get_health() - degraded state - multiple failures
def test_get_health_multiple_dependencies_not_initialized():
    """Health check returns 503 when multiple writers are not initialized."""
    logger = logging.getLogger("test")
    config = {}
    handler = HandlerHealth(logger, config)

    # Mock multiple writers as not initialized
    with (
        patch("src.handlers.handler_health.writer_kafka.STATE", {}),
        patch("src.handlers.handler_health.writer_eventbridge.STATE", {}),
        patch("src.handlers.handler_health.writer_postgres", spec=[]),  # spec=[] makes logger not exist
    ):
        response = handler.get_health()

    assert response["statusCode"] == 503
    body = json.loads(response["body"])
    assert body["status"] == "degraded"
    assert len(body["details"]) >= 2  # At least kafka and eventbridge
    assert "kafka" in body["details"]
    assert "eventbridge" in body["details"]


## get_health() - uptime calculation
def test_get_health_uptime_is_positive():
    """Verify uptime_seconds is calculated and is a positive integer."""
    logger = logging.getLogger("test")
    config = {}
    handler = HandlerHealth(logger, config)

    with (
        patch("src.handlers.handler_health.writer_kafka.STATE", {"logger": logger, "producer": MagicMock()}),
        patch(
            "src.handlers.handler_health.writer_eventbridge.STATE",
            {"logger": logger, "client": MagicMock(), "event_bus_arn": "arn"},
        ),
        patch("src.handlers.handler_health.writer_postgres.logger", logger),
    ):
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
