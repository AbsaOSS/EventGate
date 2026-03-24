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
from typing import Any
from unittest.mock import MagicMock

import pytest

from src.handlers.handler_stats import HandlerStats


@pytest.fixture
def topics() -> dict[str, dict[str, Any]]:
    """Minimal topics dict matching HandlerTopic.topics."""
    return {
        "public.cps.za.runs": {"type": "object", "properties": {}},
        "public.cps.za.test": {"type": "object", "properties": {}},
    }


@pytest.fixture
def mock_reader() -> MagicMock:
    """Mock ReaderPostgres."""
    mock = MagicMock()
    mock.read_stats.return_value = (
        [{"event_id": "ev1", "internal_id": 1, "status": "succeeded", "run_status": "succeeded"}],
        {"cursor": None, "has_more": False, "limit": 50},
    )
    return mock


@pytest.fixture
def handler(
    topics: dict[str, dict[str, Any]],
    mock_reader: MagicMock,
) -> HandlerStats:
    """Create HandlerStats with mocked dependencies."""
    return HandlerStats(
        topics=topics,
        reader_postgres=mock_reader,
    )


def _make_event(
    topic: str = "public.cps.za.runs",
    body: Any = None,
    headers: Any = None,
) -> dict[str, Any]:
    """Build an API Gateway-style proxy event for stats."""
    if body is None:
        body = {}
    return {
        "resource": "/stats/{topic_name}",
        "httpMethod": "POST",
        "headers": headers or {},
        "body": json.dumps(body) if isinstance(body, dict) else body,
        "pathParameters": {"topic_name": topic},
    }


class TestHandlerStatsSuccess:
    """Tests for successful stats queries."""

    def test_returns_200_with_data(self, handler: HandlerStats) -> None:
        """Test successful query returns 200 with data and pagination."""
        response = handler.handle_request(_make_event())

        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        assert True is body["success"]
        assert "data" in body
        assert "pagination" in body
        assert 1 == len(body["data"])
        assert "ev1" == body["data"][0]["event_id"]

    def test_passes_timestamps_to_reader(self, handler: HandlerStats, mock_reader: MagicMock) -> None:
        """Test that timestamp fields from body are forwarded to reader."""
        event = _make_event(body={"timestamp_start": 1000, "timestamp_end": 2000, "limit": 25})

        handler.handle_request(event)

        call_kwargs = mock_reader.read_stats.call_args.kwargs
        assert 1000 == call_kwargs["timestamp_start"]
        assert 2000 == call_kwargs["timestamp_end"]
        assert 25 == call_kwargs["limit"]

    def test_passes_cursor_to_reader(self, handler: HandlerStats, mock_reader: MagicMock) -> None:
        """Test that cursor from body is forwarded to reader."""
        event = _make_event(body={"cursor": 42})

        handler.handle_request(event)

        call_kwargs = mock_reader.read_stats.call_args.kwargs
        assert 42 == call_kwargs["cursor"]


class TestHandlerStatsValidation:
    """Tests for request validation."""

    def test_unknown_topic_returns_404(self, handler: HandlerStats) -> None:
        """Test that unknown topic returns 404."""
        response = handler.handle_request(_make_event(topic="nonexistent.topic"))

        assert 404 == response["statusCode"]

    def test_missing_path_parameter_returns_400(self, handler: HandlerStats) -> None:
        """Test that missing topic_name path parameter returns 400."""
        event = _make_event()
        event["pathParameters"] = {}

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "topic_name" in body["errors"][0]["message"]

    def test_missing_path_parameters_returns_400(self, handler: HandlerStats) -> None:
        """Test that missing pathParameters dict returns 400."""
        event = _make_event()
        event["pathParameters"] = None

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]

    def test_unsupported_topic_returns_400(self, handler: HandlerStats) -> None:
        """Test that a known but unsupported topic returns 400."""
        response = handler.handle_request(_make_event(topic="public.cps.za.test"))

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "only supported" in body["errors"][0]["message"]

    def test_invalid_json_body_returns_400(self, handler: HandlerStats) -> None:
        """Test that non-JSON body returns 400."""
        event = _make_event()
        event["body"] = "not json"

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]

    def test_non_dict_json_body_returns_400(self, handler: HandlerStats) -> None:
        """Test that a JSON array body returns 400."""
        event = _make_event()
        event["body"] = "[1, 2, 3]"

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "JSON object" in body["errors"][0]["message"]

    def test_invalid_timestamp_start_returns_400(self, handler: HandlerStats) -> None:
        """Test that non-integer timestamp_start returns 400."""
        response = handler.handle_request(_make_event(body={"timestamp_start": "bad"}))

        assert 400 == response["statusCode"]

    def test_invalid_timestamp_end_returns_400(self, handler: HandlerStats) -> None:
        """Test that non-integer timestamp_end returns 400."""
        response = handler.handle_request(_make_event(body={"timestamp_end": "bad"}))

        assert 400 == response["statusCode"]

    def test_invalid_cursor_returns_400(self, handler: HandlerStats) -> None:
        """Test that non-integer cursor returns 400."""
        response = handler.handle_request(_make_event(body={"cursor": "not-an-int"}))

        assert 400 == response["statusCode"]

    def test_invalid_limit_returns_400(self, handler: HandlerStats) -> None:
        """Test that non-positive limit returns 400."""
        response = handler.handle_request(_make_event(body={"limit": -5}))

        assert 400 == response["statusCode"]

    def test_boolean_timestamp_start_returns_400(self, handler: HandlerStats) -> None:
        """Test that boolean timestamp_start is rejected."""
        response = handler.handle_request(_make_event(body={"timestamp_start": True}))

        assert 400 == response["statusCode"]

    def test_boolean_cursor_returns_400(self, handler: HandlerStats) -> None:
        """Test that boolean cursor is rejected."""
        response = handler.handle_request(_make_event(body={"cursor": False}))

        assert 400 == response["statusCode"]

    def test_boolean_limit_returns_400(self, handler: HandlerStats) -> None:
        """Test that boolean limit is rejected."""
        response = handler.handle_request(_make_event(body={"limit": True}))

        assert 400 == response["statusCode"]


class TestHandlerStatsErrors:
    """Tests for error handling."""

    def test_database_error_returns_500(self, handler: HandlerStats, mock_reader: MagicMock) -> None:
        """Test that database RuntimeError returns 500."""
        mock_reader.read_stats.side_effect = RuntimeError("Database query failed")

        response = handler.handle_request(_make_event())

        assert 500 == response["statusCode"]
        body = json.loads(response["body"])
        assert False is body["success"]
        assert "database" == body["errors"][0]["type"]
        assert "Stats query failed." == body["errors"][0]["message"]
