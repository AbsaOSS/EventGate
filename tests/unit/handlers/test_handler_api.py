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

import pytest
from unittest.mock import mock_open

from src.handlers.handler_api import HandlerApi


def test_load_api_definition_success(mocker):
    """Test successful loading of API definition."""
    mock_content = "openapi: 3.0.0\ninfo:\n  title: Test API"
    mocker.patch("builtins.open", mock_open(read_data=mock_content))
    handler = HandlerApi().with_api_definition_loaded()
    assert handler.api_spec == mock_content


def test_load_api_definition_file_not_found(mocker):
    """Test that RuntimeError is raised when api.yaml doesn't exist."""
    mocker.patch("builtins.open", side_effect=FileNotFoundError("api.yaml not found"))
    handler = HandlerApi()
    with pytest.raises(RuntimeError, match="API specification initialization failed"):
        handler.with_api_definition_loaded()


def test_load_api_definition_empty_file_raises(mocker):
    """Test that RuntimeError is raised when api.yaml is empty."""
    mocker.patch("builtins.open", mock_open(read_data=""))
    handler = HandlerApi()
    with pytest.raises(RuntimeError, match="API specification initialization failed"):
        handler.with_api_definition_loaded()


def test_get_api_returns_correct_response(mocker):
    """Test get_api returns correct response structure."""
    mock_content = "openapi: 3.0.0"
    mocker.patch("builtins.open", mock_open(read_data=mock_content))
    handler = HandlerApi().with_api_definition_loaded()
    response = handler.get_api()

    assert 200 == response["statusCode"]
    assert "application/yaml" == response["headers"]["Content-Type"]
    assert mock_content == response["body"]


def test_get_docs_returns_200(mocker):
    """Test get_docs returns status code 200."""
    mocker.patch("builtins.open", mock_open(read_data="openapi: 3.0.0"))
    handler = HandlerApi().with_api_definition_loaded()
    response = handler.get_docs()

    assert 200 == response["statusCode"]


def test_get_docs_returns_html_content_type(mocker):
    """Test get_docs returns text/html content type."""
    mocker.patch("builtins.open", mock_open(read_data="openapi: 3.0.0"))
    handler = HandlerApi().with_api_definition_loaded()
    response = handler.get_docs()

    assert "text/html" == response["headers"]["Content-Type"]


def test_get_docs_body_contains_swagger_ui_markers(mocker):
    """Test get_docs body contains Swagger UI markers and references ./api."""
    mocker.patch("builtins.open", mock_open(read_data="openapi: 3.0.0"))
    handler = HandlerApi().with_api_definition_loaded()
    response = handler.get_docs()

    body = response["body"]
    assert "swagger-ui" in body
    assert "SwaggerUIBundle" in body
    assert "./api" in body


def test_get_docs_body_inlines_assets_not_cdn(mocker):
    """Test get_docs inlines the vendored assets and references no external CDN."""
    mocker.patch("builtins.open", mock_open(read_data="openapi: 3.0.0"))
    handler = HandlerApi().with_api_definition_loaded()
    body = handler.get_docs()["body"]

    assert "<style>" in body
    assert "<script>" in body
    assert "cdn.jsdelivr.net" not in body
    assert "./docs/swagger-ui" not in body


def test_get_docs_inlines_asset_content(mocker):
    """Test get_docs embeds the CSS and JS file contents into the page."""
    mocker.patch("builtins.open", mock_open(read_data="ASSET_CONTENT"))
    handler = HandlerApi()
    body = handler.get_docs()["body"]

    assert "<style>ASSET_CONTENT</style>" in body
    assert "<script>ASSET_CONTENT</script>" in body


def test_get_docs_builds_once_and_caches(mocker):
    """Test get_docs reads the vendored assets once and serves later calls from cache."""
    mock_file = mocker.patch("builtins.open", mock_open(read_data="body"))
    handler = HandlerApi()

    handler.get_docs()
    handler.get_docs()

    assert 2 == mock_file.call_count


def test_get_docs_read_error_raises(mocker):
    """Test get_docs raises RuntimeError when a vendored asset cannot be read."""
    mocker.patch("builtins.open", side_effect=FileNotFoundError("missing"))
    handler = HandlerApi()

    with pytest.raises(RuntimeError, match="Static asset read failed"):
        handler.get_docs()
