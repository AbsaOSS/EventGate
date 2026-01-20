import pytest
from unittest.mock import patch, mock_open

from src.handlers.handler_api import HandlerApi


def test_load_api_definition_success():
    """Test successful loading of API definition."""
    mock_content = "openapi: 3.0.0\ninfo:\n  title: Test API"
    with patch("builtins.open", mock_open(read_data=mock_content)):
        handler = HandlerApi().load_api_definition()
        assert handler.api_spec == mock_content


def test_load_api_definition_file_not_found():
    """Test that FileNotFoundError is raised when api.yaml doesn't exist."""
    with patch("builtins.open", side_effect=FileNotFoundError("api.yaml not found")):
        handler = HandlerApi()
        with pytest.raises(FileNotFoundError):
            handler.load_api_definition()


def test_get_api_returns_correct_response():
    """Test get_api returns correct response structure."""
    mock_content = "openapi: 3.0.0"
    with patch("builtins.open", mock_open(read_data=mock_content)):
        handler = HandlerApi().load_api_definition()
        response = handler.get_api()

        assert response["statusCode"] == 200
        assert response["headers"]["Content-Type"] == "application/yaml"
        assert response["body"] == mock_content
