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


class TestDocsEndpoint:
    """Tests for the /docs endpoint."""

    def test_get_docs_returns_200(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /docs returns successful response."""
        response = eventgate_client.get_docs()

        assert 200 == response["statusCode"]

    def test_get_docs_returns_html_content_type(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /docs returns text/html content type."""
        response = eventgate_client.get_docs()

        assert "Content-Type" in response["headers"]
        assert "text/html" in response["headers"]["Content-Type"]

    def test_get_docs_body_contains_swagger_ui(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /docs body contains Swagger UI markers and references ./api."""
        response = eventgate_client.get_docs()

        body = response["body"]
        assert "swagger" in body.lower()
        assert "SwaggerUIBundle" in body
        assert "./api" in body

    def test_get_docs_uses_self_hosted_assets(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /docs inlines its assets and makes no CDN requests."""
        response = eventgate_client.get_docs()

        body = response["body"]
        assert "<style>" in body
        assert "<script>" in body
        assert "cdn.jsdelivr.net" not in body
        assert "./docs/swagger-ui" not in body
