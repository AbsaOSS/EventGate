#
# Copyright 2026 ABSA Group Limited
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

from src.utils.observability import CORRELATION_ID_RESPONSE_HEADER
from tests.integration.conftest import EventGateTestClient


class TestHealthEndpoint:
    """Tests for the /health endpoint."""

    def test_get_health_returns_200(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /health returns successful response."""
        response = eventgate_client.get_health()

        assert 200 == response["statusCode"]

    def test_get_health_status_ok(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /health returns ok status when all writers healthy."""
        response = eventgate_client.get_health()

        body = json.loads(response["body"])
        assert "ok" == body["status"]

    def test_get_health_includes_uptime(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /health includes uptime in response."""
        response = eventgate_client.get_health()

        body = json.loads(response["body"])
        assert "uptime_seconds" in body
        assert isinstance(body["uptime_seconds"], (int, float))
        assert body["uptime_seconds"] >= 0


class TestCorrelationId:
    """Tests for the request correlation id contract."""

    def test_caller_correlation_id_is_returned(self, eventgate_client: EventGateTestClient) -> None:
        """Test a caller supplied correlation id is echoed back in the response headers."""
        response = eventgate_client.invoke("/health", "GET", headers={"X-Correlation-ID": "run-42"})

        assert "run-42" == response["headers"][CORRELATION_ID_RESPONSE_HEADER]

    def test_malformed_correlation_id_is_not_echoed(self, eventgate_client: EventGateTestClient) -> None:
        """Test a malformed correlation id is rejected instead of being written back."""
        response = eventgate_client.invoke("/health", "GET", headers={"X-Correlation-ID": "bad value\ninjected"})

        assert CORRELATION_ID_RESPONSE_HEADER not in response["headers"]

    def test_error_responses_carry_the_correlation_id(self, eventgate_client: EventGateTestClient) -> None:
        """Test the correlation id is present on error responses too."""
        response = eventgate_client.invoke("/unknown", "GET", headers={"X-Correlation-ID": "run-43"})

        assert 404 == response["statusCode"]
        assert "run-43" == response["headers"][CORRELATION_ID_RESPONSE_HEADER]
