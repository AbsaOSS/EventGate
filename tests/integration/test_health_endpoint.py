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
