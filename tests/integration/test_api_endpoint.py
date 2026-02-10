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

"""Integration tests for GET /api endpoint."""

from tests.integration.conftest import EventGateTestClient


class TestApiEndpoint:
    """Tests for the /api endpoint."""

    def test_get_api_returns_200(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /api returns successful response."""
        response = eventgate_client.get_api()

        assert 200 == response["statusCode"]

    def test_get_api_returns_openapi_content_type(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /api returns correct content type for OpenAPI."""
        response = eventgate_client.get_api()

        assert "Content-Type" in response["headers"]
        assert "application/yaml" in response["headers"]["Content-Type"]

    def test_get_api_body_contains_openapi_spec(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /api returns valid OpenAPI specification."""
        response = eventgate_client.get_api()

        body = response["body"]
        assert "openapi:" in body
        assert "paths:" in body
        assert "/topics" in body
