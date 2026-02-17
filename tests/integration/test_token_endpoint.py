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

"""Integration tests for GET /token endpoint."""

from tests.integration.conftest import EventGateTestClient


class TestTokenEndpoint:
    """Tests for the /token endpoint."""

    def test_get_token_returns_redirect(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /token returns 303 redirect."""
        response = eventgate_client.get_token()

        assert 303 == response["statusCode"]

    def test_get_token_has_location_header(self, eventgate_client: EventGateTestClient) -> None:
        """Test GET /token includes Location header."""
        response = eventgate_client.get_token()

        assert "Location" in response["headers"]
        assert response["headers"]["Location"]
