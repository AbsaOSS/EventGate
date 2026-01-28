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
from unittest.mock import patch, mock_open

from src.handlers.handler_api import HandlerApi


def test_load_api_definition_success():
    """Test successful loading of API definition."""
    mock_content = "openapi: 3.0.0\ninfo:\n  title: Test API"
    with patch("builtins.open", mock_open(read_data=mock_content)):
        handler = HandlerApi().with_api_definition_loaded()
        assert handler.api_spec == mock_content


def test_load_api_definition_file_not_found():
    """Test that RuntimeError is raised when api.yaml doesn't exist."""
    with patch("builtins.open", side_effect=FileNotFoundError("api.yaml not found")):
        handler = HandlerApi()
        with pytest.raises(RuntimeError, match="API specification initialization failed"):
            handler.with_api_definition_loaded()


def test_get_api_returns_correct_response():
    """Test get_api returns correct response structure."""
    mock_content = "openapi: 3.0.0"
    with patch("builtins.open", mock_open(read_data=mock_content)):
        handler = HandlerApi().with_api_definition_loaded()
        response = handler.get_api()

        assert response["statusCode"] == 200
        assert response["headers"]["Content-Type"] == "application/yaml"
        assert response["body"] == mock_content
