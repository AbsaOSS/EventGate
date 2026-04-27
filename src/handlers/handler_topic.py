#
# Copyright 2025 ABSA Group Limited
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

"""Topic management, access control, and message dispatch handler."""

import json
import logging
import os
from typing import Any

import jwt
from boto3.resources.base import ServiceResource
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from src.handlers.handler_token import HandlerToken
from src.utils.conf_path import CONF_DIR
from src.utils.config_loader import TopicAccessMap, load_access_config
from src.utils.constants import TOPIC_DLCHANGE, TOPIC_RUNS, TOPIC_TEST
from src.utils.utils import build_error_response
from src.writers.writer import Writer

logger = logging.getLogger(__name__)


class HandlerTopic:
    """Manages topic schemas, access control, and message posting."""

    def __init__(
        self,
        config: dict[str, Any],
        aws_s3: ServiceResource,
        handler_token: HandlerToken,
        writers: dict[str, Writer],
    ):
        self.config = config
        self.aws_s3 = aws_s3
        self.handler_token = handler_token
        self.writers = writers
        self.access_config: TopicAccessMap = {}
        self.topics: dict[str, dict[str, Any]] = {}

    def with_load_access_config(self) -> "HandlerTopic":
        """Load access control configuration from S3 or local file.
        Returns:
            The current instance with loaded access config.
        """
        self.access_config = load_access_config(self.config, self.aws_s3)
        return self

    def with_load_topic_schemas(self) -> "HandlerTopic":
        """Load topic schemas from configuration files.
        Returns:
            The current instance with loaded topic schemas.
        """
        topic_schemas_dir = os.path.join(CONF_DIR, "topic_schemas")
        logger.debug("Loading topic schemas from %s.", topic_schemas_dir)

        with open(os.path.join(topic_schemas_dir, "runs.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_RUNS] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "dlchange.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_DLCHANGE] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "test.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_TEST] = json.load(file)

        logger.debug("Loaded topic schemas successfully.")
        return self

    def get_topics_list(self) -> dict[str, Any]:
        """Return the list of available topics.
        Returns:
            API Gateway response with topic list.
        """
        logger.debug("Handling GET Topics.")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(list(self.topics)),
        }

    def handle_request(self, event: dict[str, Any]) -> dict[str, Any]:
        """Handle GET/POST requests for /topics/{topic_name} resource.
        Args:
            event: The API Gateway event containing path parameters, method, body, and headers.
        Returns:
            API Gateway response.
        """
        topic_name = event["pathParameters"]["topic_name"].lower()
        method = event.get("httpMethod")

        if method == "GET":
            return self._get_topic_schema(topic_name)
        if method == "POST":
            return self._post_topic_message(
                topic_name,
                json.loads(event["body"]),
                self.handler_token.extract_token(event.get("headers", {})),
            )
        return build_error_response(404, "route", "Resource not found")

    def _get_topic_schema(self, topic_name: str) -> dict[str, Any]:
        """Return the JSON schema for a specific topic.
        Args:
            topic_name: The topic whose schema is requested.
        Returns:
            API Gateway response with topic schema or error.
        """
        logger.debug("Handling GET TopicSchema(%s).", topic_name)

        if topic_name not in self.topics:
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(self.topics[topic_name]),
        }

    def _post_topic_message(self, topic_name: str, topic_message: dict[str, Any], token_encoded: str) -> dict[str, Any]:
        """Validate auth and schema; dispatch message to all writers.
        Args:
            topic_name: Target topic name.
            topic_message: JSON message payload.
            token_encoded: Encoded bearer JWT token string.
        Returns:
            API Gateway response indicating success or failure.
        Raises:
            RuntimeError: If access configuration is not loaded.
            jwt.PyJWTError: If token decoding fails.
            ValidationError: If message validation fails.
        """
        logger.debug("Handling POST TopicMessage(%s).", topic_name)

        if not self.access_config:
            logger.error("Access configuration not loaded.")
            raise RuntimeError("Access configuration not loaded")

        try:
            token: dict[str, Any] = self.handler_token.decode_jwt(token_encoded)
        except jwt.PyJWTError:  # type: ignore[attr-defined]
            return build_error_response(401, "auth", "Invalid or missing token")

        if topic_name not in self.topics:
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found")

        user = token.get("sub")
        if topic_name not in self.access_config or user not in self.access_config[topic_name]:
            return build_error_response(403, "auth", "User not authorized for topic")

        allowed, perm_error = self._validate_user_permissions(topic_name, user, topic_message)
        if not allowed:
            return build_error_response(403, "permission", perm_error or "Permission denied")

        try:
            validate(instance=topic_message, schema=self.topics[topic_name])
        except ValidationError as exc:
            return build_error_response(400, "validation", exc.message)

        errors = []
        for writer_name, writer in self.writers.items():
            ok, err = writer.write(topic_name, topic_message)
            if not ok:
                errors.append({"type": writer_name, "message": err})

        if errors:
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"success": False, "statusCode": 500, "errors": errors}),
            }

        return {
            "statusCode": 202,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"success": True, "statusCode": 202}),
        }

    def _validate_user_permissions(
        self,
        topic_name: str,
        user: str,
        message: dict[str, Any],
    ) -> tuple[bool, str | None]:
        """Check message fields against the user's permission constraints.
        Args:
            topic_name: Target topic name.
            user: Authenticated user.
            message: Message payload to validate.
        Returns:
            Tuple of (allowed, error_message). `error_message` is `None` when allowed.
        """
        user_permissions = self.access_config[topic_name][user]
        if not user_permissions:
            return True, None

        for restricted_field, compiled_patterns in user_permissions.items():
            message_value = message.get(restricted_field)
            if message_value is None:
                return False, f"Required field '{restricted_field}' missing from message"
            if not any(pattern.fullmatch(str(message_value)) for pattern in compiled_patterns):
                return False, f"Field '{restricted_field}' value not permitted for user '{user}'"

        return True, None
