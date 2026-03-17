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
from typing import Any

from jwt.exceptions import PyJWTError

from src.handlers.handler_token import HandlerToken
from src.readers.reader_postgres import ReaderPostgres
from src.utils.constants import POSTGRES_DEFAULT_LIMIT, SUPPORTED_TOPICS
from src.utils.utils import build_error_response

logger = logging.getLogger(__name__)


class HandlerStats:
    """Handle stats queries for a specific topic."""

    def __init__(
        self,
        handler_token: HandlerToken,
        access_config: dict[str, list[str]],
        topics: dict[str, dict[str, Any]],
        reader_postgres: ReaderPostgres,
    ) -> None:
        self.handler_token = handler_token
        self.access_config = access_config
        self.topics = topics
        self.reader_postgres = reader_postgres

    def handle_request(self, event: dict[str, Any]) -> dict[str, Any]:
        """Handle POST /stats/{topic_name} requests.
        Args:
            event: API Gateway proxy event.
        Returns:
            API Gateway response dict.
        """
        topic_name = event["pathParameters"]["topic_name"].lower()

        if topic_name not in self.topics:
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found.")

        if topic_name not in SUPPORTED_TOPICS:
            return build_error_response(
                400, "validation", f"Stats are only supported for topics '{', '.join(SUPPORTED_TOPICS)}'."
            )

        # Authentication
        try:
            token_encoded = self.handler_token.extract_token(event.get("headers", {}))
            token: dict[str, Any] = self.handler_token.decode_jwt(token_encoded)
        except (PyJWTError, ValueError, KeyError):
            return build_error_response(401, "auth", "Invalid or missing token.")

        # Authorisation
        user = token.get("sub")
        if topic_name not in self.access_config or user not in self.access_config[topic_name]:
            return build_error_response(403, "auth", "User not authorized for topic.")

        # Parse request body
        try:
            body = json.loads(event.get("body") or "{}")
        except (json.JSONDecodeError, TypeError):
            return build_error_response(400, "validation", "Request body must be valid JSON.")

        if not isinstance(body, dict):
            return build_error_response(400, "validation", "Request body must be a JSON object.")

        timestamp_start = body.get("timestamp_start")
        timestamp_end = body.get("timestamp_end")
        cursor = body.get("cursor")
        limit: int = body.get("limit", POSTGRES_DEFAULT_LIMIT)

        if timestamp_start is not None and not isinstance(timestamp_start, int):
            return build_error_response(400, "validation", "Field 'timestamp_start' must be an integer (epoch ms).")
        if timestamp_end is not None and not isinstance(timestamp_end, int):
            return build_error_response(400, "validation", "Field 'timestamp_end' must be an integer (epoch ms).")
        if cursor is not None and not isinstance(cursor, int):
            return build_error_response(400, "validation", "Field 'cursor' must be an integer (internal_id).")
        if not isinstance(limit, int) or limit < 1:
            return build_error_response(400, "validation", "Field 'limit' must be a positive integer.")

        # Execute query
        try:
            rows, pagination = self.reader_postgres.read_stats(
                timestamp_start=timestamp_start,
                timestamp_end=timestamp_end,
                cursor=cursor,
                limit=limit,
            )
        except RuntimeError as exc:
            logger.exception("Stats query failed for topic %s.", topic_name)
            return build_error_response(500, "database", str(exc))

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
