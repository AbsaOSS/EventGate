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

"""Handler for the /stats/{topic_name}/query/{query_name} endpoint."""

import json
import logging
from dataclasses import dataclass
from typing import Any

from src.readers.named_query_registry import SUPPORTED_QUERIES
from src.readers.reader_postgres import ReaderPostgres
from src.utils.constants import POSTGRES_DEFAULT_LIMIT, SUPPORTED_STATS_TOPICS
from src.utils.utils import build_error_response, build_success_response

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NamedQueryParams:
    """Validated query parameters ready to pass to `read_named_query`.

    Attributes:
        timestamp_start: Start of time window in epoch milliseconds, or `None` for the default.
        timestamp_end: End of time window in epoch milliseconds, or `None` for the default.
        cursor: Last `internal_id` from previous page, or `None` for the first page.
        limit: Maximum number of rows per page.
    """

    timestamp_start: int | None
    timestamp_end: int | None
    cursor: int | None
    limit: int


class HandlerNamedQuery:
    """Handle predefined named queries for a specific topic."""

    def __init__(
        self,
        topics: dict[str, dict[str, Any]],
        reader_postgres: ReaderPostgres,
    ) -> None:
        self.topics = topics
        self.reader_postgres = reader_postgres

    def handle_request(self, event: dict[str, Any]) -> dict[str, Any]:
        """Handle POST /stats/{topic_name}/query/{query_name} requests.
        Args:
            event: API Gateway proxy event.
        Returns:
            API Gateway response dict.
        """
        path_params = event.get("pathParameters") or {}
        topic_name = path_params.get("topic_name", "").lower()
        query_name = path_params.get("query_name", "").lower()

        if error_response := self._validate_event_path_params(topic_name, query_name):
            return error_response

        body_params = self._validate_event_body(event.get("body"))
        if isinstance(body_params, dict):
            return body_params

        try:
            rows, pagination = self.reader_postgres.read_named_query(
                query_name=query_name,
                timestamp_start=body_params.timestamp_start,
                timestamp_end=body_params.timestamp_end,
                cursor=body_params.cursor,
                limit=body_params.limit,
            )
        except RuntimeError:
            logger.exception("Named query %s failed for topic %s.", query_name, topic_name)
            return build_error_response(500, "database", "Named query failed.")

        return build_success_response(rows, pagination)

    def _validate_event_path_params(self, topic_name: str, query_name: str) -> dict[str, Any] | None:
        """Validate the `topic_name`/`query_name` path parameters.
        Args:
            topic_name: The lower-cased `topic_name` path parameter.
            query_name: The lower-cased `query_name` path parameter.
        Returns:
            An `error_response` dict if validation fails, or `None` if valid.
        """
        if not topic_name:
            return build_error_response(400, "validation", "Missing path parameter 'topic_name'.")

        if topic_name not in self.topics:
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found.")

        if topic_name not in SUPPORTED_STATS_TOPICS:
            return build_error_response(400, "validation", f"Topic '{topic_name}' is not supported.")

        if not query_name:
            return build_error_response(400, "validation", "Missing path parameter 'query_name'.")

        if query_name not in SUPPORTED_QUERIES:
            return build_error_response(400, "validation", f"Query '{query_name}' is not supported. ")

        return None

    @staticmethod
    def _validate_event_body(body: str | None) -> NamedQueryParams | dict[str, Any]:
        """Parse and validate the request body.
        Args:
            body: The raw request body (JSON string) from the API Gateway event, or `None`.
        Returns:
            The parsed_body `NamedQueryParams`, or an `error_response` dict if validation fails.
        """
        try:
            parsed_body = json.loads(body or "{}")
        except (json.JSONDecodeError, TypeError):
            return build_error_response(400, "validation", "Request body must be valid JSON.")

        if not isinstance(parsed_body, dict):
            return build_error_response(400, "validation", "Request body must be a JSON object.")

        timestamp_start = parsed_body.get("timestamp_start")
        timestamp_end = parsed_body.get("timestamp_end")
        cursor = parsed_body.get("cursor")
        limit: int = parsed_body.get("limit", POSTGRES_DEFAULT_LIMIT)

        int_fields = (
            (timestamp_start, "timestamp_start"),
            (timestamp_end, "timestamp_end"),
            (cursor, "cursor"),
        )
        for value, field_name in int_fields:
            if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
                return build_error_response(400, "validation", f"Field '{field_name}' must be an integer.")

        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            return build_error_response(400, "validation", "Field 'limit' must be a positive integer.")

        return NamedQueryParams(
            timestamp_start=timestamp_start, timestamp_end=timestamp_end, cursor=cursor, limit=limit
        )
