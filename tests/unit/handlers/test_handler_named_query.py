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

from src.handlers.handler_named_query import HandlerNamedQuery, NamedQueryParams


@pytest.fixture
def mock_reader() -> MagicMock:
    """Mock ReaderPostgres."""
    mock = MagicMock()
    mock.read_named_query.return_value = (
        [{"event_id": "ev1", "internal_id": 1, "status": "succeeded", "run_status": "succeeded"}],
        {"cursor": None, "has_more": False, "limit": 50},
    )
    return mock


@pytest.fixture
def handler(
    topics: dict[str, dict[str, Any]],
    mock_reader: MagicMock,
) -> HandlerNamedQuery:
    """Create HandlerNamedQuery with mocked dependencies."""
    return HandlerNamedQuery(
        topics=topics,
        reader_postgres=mock_reader,
    )


def _make_event(
    topic: str = "public.cps.za.runs",
    query_name: str = "runs_jobs_detail",
    body: Any = None,
) -> dict[str, Any]:
    """Build an API Gateway-style proxy event for a named query."""
    if body is None:
        body = {}
    return {
        "resource": "/stats/{topic_name}/query/{query_name}",
        "httpMethod": "POST",
        "headers": {},
        "body": json.dumps(body) if isinstance(body, dict) else body,
        "pathParameters": {"topic_name": topic, "query_name": query_name},
    }


class TestHandlerNamedQuerySuccess:
    """Tests for successful named queries."""

    def test_returns_200_with_data(self, handler: HandlerNamedQuery) -> None:
        """Test successful query returns 200 with data and pagination."""
        response = handler.handle_request(_make_event())

        assert 200 == response["statusCode"]
        body = json.loads(response["body"])
        assert True is body["success"]
        assert "data" in body
        assert "pagination" in body
        assert "ev1" == body["data"][0]["event_id"]

    def test_forwards_query_name_and_params_to_reader(self, handler: HandlerNamedQuery, mock_reader: MagicMock) -> None:
        """Test that query_name and query params are forwarded to the reader."""
        event = _make_event(body={"timestamp_start": 1000, "timestamp_end": 2000, "cursor": 42, "limit": 25})

        handler.handle_request(event)

        call_kwargs = mock_reader.read_named_query.call_args.kwargs
        assert "runs_jobs_detail" == call_kwargs["query_name"]
        assert 1000 == call_kwargs["timestamp_start"]
        assert 2000 == call_kwargs["timestamp_end"]
        assert 42 == call_kwargs["cursor"]
        assert 25 == call_kwargs["limit"]


class TestHandlerNamedQueryValidation:
    """Tests for request validation."""

    def test_unknown_topic_returns_404(self, handler: HandlerNamedQuery) -> None:
        """Test that unknown topic returns 404."""
        response = handler.handle_request(_make_event(topic="nonexistent.topic"))

        assert 404 == response["statusCode"]

    def test_unsupported_topic_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that a known but unsupported topic returns 400."""
        response = handler.handle_request(_make_event(topic="public.cps.za.test"))

        assert 400 == response["statusCode"]

    def test_missing_topic_name_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that missing topic_name path parameter returns 400."""
        event = _make_event()
        event["pathParameters"] = {"query_name": "runs_jobs_detail"}

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "topic_name" in body["errors"][0]["message"]

    def test_missing_query_name_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that missing query_name path parameter returns 400."""
        event = _make_event()
        event["pathParameters"] = {"topic_name": "public.cps.za.runs"}

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "query_name" in body["errors"][0]["message"]

    def test_unknown_query_name_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that an unknown query_name returns 400 (not 500)."""
        response = handler.handle_request(_make_event(query_name="does_not_exist"))

        assert 400 == response["statusCode"]
        body = json.loads(response["body"])
        assert "does_not_exist" in body["errors"][0]["message"]

    def test_invalid_json_body_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that non-JSON body returns 400."""
        event = _make_event()
        event["body"] = "not json"

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]

    def test_non_dict_json_body_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that a JSON array body returns 400."""
        event = _make_event()
        event["body"] = "[1, 2, 3]"

        response = handler.handle_request(event)

        assert 400 == response["statusCode"]

    def test_invalid_timestamp_start_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that noninteger timestamp_start returns 400."""
        response = handler.handle_request(_make_event(body={"timestamp_start": "bad"}))

        assert 400 == response["statusCode"]

    def test_invalid_cursor_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that noninteger cursor returns 400."""
        response = handler.handle_request(_make_event(body={"cursor": "bad"}))

        assert 400 == response["statusCode"]

    def test_invalid_limit_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that non-positive limit returns 400."""
        response = handler.handle_request(_make_event(body={"limit": 0}))

        assert 400 == response["statusCode"]

    def test_boolean_limit_returns_400(self, handler: HandlerNamedQuery) -> None:
        """Test that boolean limit is rejected."""
        response = handler.handle_request(_make_event(body={"limit": True}))

        assert 400 == response["statusCode"]


class TestHandlerNamedQueryErrors:
    """Tests for error handling."""

    def test_database_error_returns_500(self, handler: HandlerNamedQuery, mock_reader: MagicMock) -> None:
        """Test that database RuntimeError returns 500."""
        mock_reader.read_named_query.side_effect = RuntimeError("Database query failed")

        response = handler.handle_request(_make_event())

        assert 500 == response["statusCode"]
        body = json.loads(response["body"])
        assert False is body["success"]
        assert "database" == body["errors"][0]["type"]


class TestValidateEventPathParams:
    """Parametrized tests for `HandlerNamedQuery._validate_event_path_params`."""

    @pytest.mark.parametrize(
        "path_params, expected_status, expected_message_fragment",
        [
            pytest.param({"query_name": "runs_jobs_detail"}, 400, "topic_name", id="missing_topic_name"),
            pytest.param(
                {"topic_name": "nonexistent.topic", "query_name": "runs_jobs_detail"},
                404,
                "nonexistent.topic",
                id="unknown_topic",
            ),
            pytest.param(
                {"topic_name": "public.cps.za.test", "query_name": "runs_jobs_detail"},
                400,
                "is not supported",
                id="known_but_unsupported_topic",
            ),
            pytest.param({"topic_name": "public.cps.za.runs"}, 400, "query_name", id="missing_query_name"),
            pytest.param(
                {"topic_name": "public.cps.za.runs", "query_name": "does_not_exist"},
                400,
                "does_not_exist",
                id="unknown_query_name",
            ),
        ],
    )
    def test_returns_error_response_for_invalid_params(
        self,
        handler: HandlerNamedQuery,
        path_params: dict[str, Any],
        expected_status: int,
        expected_message_fragment: str,
    ) -> None:
        """Test that each invalid path parameter combination returns the expected error."""
        topic_name = path_params.get("topic_name", "").lower()
        query_name = path_params.get("query_name", "").lower()

        error = handler._validate_event_path_params(topic_name, query_name)

        assert error is not None
        assert expected_status == error["statusCode"]
        body = json.loads(error["body"])
        assert expected_message_fragment in body["errors"][0]["message"]

    def test_returns_none_for_valid_params(self, handler: HandlerNamedQuery) -> None:
        """Test that valid path parameters return None (no error)."""
        topic_name = "public.cps.za.runs"
        query_name = "runs_jobs_detail"

        error = handler._validate_event_path_params(topic_name, query_name)

        assert error is None


class TestValidateEventBody:
    """Parametrized tests for `HandlerNamedQuery._validate_event_body`."""

    @pytest.mark.parametrize(
        "body, expected_status, expected_message_fragment",
        [
            pytest.param("not json", 400, "valid JSON", id="invalid_json"),
            pytest.param("[1, 2, 3]", 400, "JSON object", id="non_dict_json"),
            pytest.param(json.dumps({"timestamp_start": "bad"}), 400, "timestamp_start", id="invalid_timestamp_start"),
            pytest.param(json.dumps({"timestamp_end": "bad"}), 400, "timestamp_end", id="invalid_timestamp_end"),
            pytest.param(json.dumps({"cursor": "bad"}), 400, "cursor", id="invalid_cursor"),
            pytest.param(json.dumps({"limit": 0}), 400, "limit", id="non_positive_limit"),
            pytest.param(json.dumps({"limit": True}), 400, "limit", id="boolean_limit"),
        ],
    )
    def test_returns_error_response_for_invalid_body(
        self, body: str, expected_status: int, expected_message_fragment: str
    ) -> None:
        """Test that each invalid body returns the expected error."""
        result = HandlerNamedQuery._validate_event_body(body)

        assert isinstance(result, dict)
        assert expected_status == result["statusCode"]
        parsed_body = json.loads(result["body"])
        assert expected_message_fragment in parsed_body["errors"][0]["message"]

    @pytest.mark.parametrize(
        "body, expected_params",
        [
            pytest.param(None, NamedQueryParams(None, None, None, 50), id="none_body_uses_defaults"),
            pytest.param("{}", NamedQueryParams(None, None, None, 50), id="empty_object_uses_defaults"),
            pytest.param(
                json.dumps({"timestamp_start": 1000, "timestamp_end": 2000, "cursor": 42, "limit": 25}),
                NamedQueryParams(1000, 2000, 42, 25),
                id="full_body",
            ),
        ],
    )
    def test_returns_parsed_params_for_valid_body(self, body: str | None, expected_params: NamedQueryParams) -> None:
        """Test that valid bodies are parsed into the expected `NamedQueryParams`."""
        result = HandlerNamedQuery._validate_event_body(body)

        assert expected_params == result
