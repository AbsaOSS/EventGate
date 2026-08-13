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

"""Handler for the /stats/{topic_name} endpoint."""

import json
import logging
import time
from typing import Any

from src.readers.reader_postgres import ReaderPostgres
from src.utils.constants import POSTGRES_DEFAULT_LIMIT, SUPPORTED_STATS_TOPICS
from src.utils.utils import build_error_response, resolve_request_topic

logger = logging.getLogger(__name__)


class HandlerStats:
    """Handle stats queries for a specific topic."""

    def __init__(
        self,
        topics: dict[str, dict[str, Any]],
        reader_postgres: ReaderPostgres,
    ) -> None:
        self.topics = topics
        self.reader_postgres = reader_postgres

    @staticmethod
    def _log_field_rejected(field_name: str) -> None:
        """Log a rejected request body field.
        Args:
            field_name: Name of the field that failed validation.
        """
        logger.warning("Request rejected: request body field is invalid.", extra={"field": field_name})

    def handle_request(self, event: dict[str, Any]) -> dict[str, Any]:
        """Handle POST /stats/{topic_name} requests.
        Args:
            event: API Gateway proxy event.
        Returns:
            API Gateway response dict.
        """
        topic_name, topic_error = resolve_request_topic(event)
        if topic_error is not None:
            return topic_error

        logger.debug("Handling POST topic stats.")

        if topic_name not in self.topics:
            logger.warning("Request rejected: unknown topic.", extra={"known_topics": sorted(self.topics)})
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found.")

        if topic_name not in SUPPORTED_STATS_TOPICS:
            logger.warning(
                "Request rejected: stats are not supported for the topic.",
                extra={"supported_topics": sorted(SUPPORTED_STATS_TOPICS)},
            )
            return build_error_response(
                400, "validation", f"Stats are only supported for topics '{', '.join(SUPPORTED_STATS_TOPICS)}'."
            )

        # Parse request body. Every field is optional and defaulted, so an absent body is a valid
        # "no filters" query - unlike `/topics`, where the body carries the message itself.
        try:
            body = json.loads(event.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            logger.warning("Request rejected: request body is not valid JSON.")
            return build_error_response(400, "validation", "Request body must be valid JSON.")

        if not isinstance(body, dict):
            logger.warning("Request rejected: request body is not a JSON object.")
            return build_error_response(400, "validation", "Request body must be a JSON object.")

        timestamp_start = body.get("timestamp_start")
        timestamp_end = body.get("timestamp_end")
        cursor = body.get("cursor")
        limit: int = body.get("limit", POSTGRES_DEFAULT_LIMIT)

        if timestamp_start is not None and (isinstance(timestamp_start, bool) or not isinstance(timestamp_start, int)):
            self._log_field_rejected("timestamp_start")
            return build_error_response(400, "validation", "Field 'timestamp_start' must be an integer (epoch ms).")
        if timestamp_end is not None and (isinstance(timestamp_end, bool) or not isinstance(timestamp_end, int)):
            self._log_field_rejected("timestamp_end")
            return build_error_response(400, "validation", "Field 'timestamp_end' must be an integer (epoch ms).")
        if cursor is not None and (isinstance(cursor, bool) or not isinstance(cursor, int)):
            self._log_field_rejected("cursor")
            return build_error_response(400, "validation", "Field 'cursor' must be an integer (internal_id).")
        if not isinstance(limit, int) or isinstance(limit, bool) or limit < 1:
            self._log_field_rejected("limit")
            return build_error_response(400, "validation", "Field 'limit' must be a positive integer.")

        # Execute query
        started_at = time.perf_counter()
        try:
            rows, pagination = self.reader_postgres.read_stats(
                timestamp_start=timestamp_start,
                timestamp_end=timestamp_end,
                cursor=cursor,
                limit=limit,
            )
        except RuntimeError:
            logger.exception(
                "Stats query failed.",
                extra={"query_duration_ms": round((time.perf_counter() - started_at) * 1000, 2)},
            )
            return build_error_response(500, "database", "Stats query failed.")

        logger.info(
            "Stats query completed.",
            extra={
                "query_duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                "row_count": len(rows),
                "has_more": pagination.get("has_more"),
                "limit": pagination.get("limit"),
            },
        )

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(
                {
                    "success": True,
                    "statusCode": 200,
                    "data": rows,
                    "pagination": pagination,
                },
                default=str,
            ),
        }
