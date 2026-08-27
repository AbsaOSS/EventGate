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
import time
from typing import Any

import jwt
from boto3.resources.base import ServiceResource
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from src.handlers.handler_token import HandlerToken
from src.utils.conf_path import CONF_DIR
from src.utils.config_loader import TopicAccessMap, TopicKeyMap, load_access_config, load_topic_keys_config
from src.utils.constants import TOPIC_DLCHANGE, TOPIC_RUNS, TOPIC_STATUS_CHANGE, TOPIC_TEST
from src.utils.observability import append_request_context
from src.utils.utils import build_error_response, resolve_request_topic
from src.writers.writer import WriteError, Writer

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
        self.topic_keys: TopicKeyMap = {}
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
        logger.debug("Loading topic schemas.", extra={"topic_schemas_dir": topic_schemas_dir})

        with open(os.path.join(topic_schemas_dir, "runs.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_RUNS] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "dlchange.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_DLCHANGE] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "test.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_TEST] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "status_change.json"), "r", encoding="utf-8") as file:
            self.topics[TOPIC_STATUS_CHANGE] = json.load(file)

        logger.debug("Loaded topic schemas.", extra={"topics": sorted(self.topics)})
        return self

    def with_load_topic_keys_config(self) -> "HandlerTopic":
        """Load topic key mapping configuration from S3 or local file.
        Returns:
            The current instance with loaded topic key config.
        """
        self.topic_keys = load_topic_keys_config(self.config, self.aws_s3)
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
        topic_name, topic_error = resolve_request_topic(event)
        if topic_error is not None:
            return topic_error

        method = event.get("httpMethod")

        if method == "GET":
            return self._get_topic_schema(topic_name)
        if method == "POST":
            try:
                # A message is mandatory here, so an empty body must fail parsing. `/stats` differs
                # on purpose: there the body only carries optional filters.
                topic_message = json.loads(event.get("body") or "")
            except (json.JSONDecodeError, TypeError):
                logger.warning("Request rejected: message body is not valid JSON.")
                return build_error_response(400, "validation", "Request body must be valid JSON.")
            if not isinstance(topic_message, dict):
                logger.warning("Request rejected: message body is not a JSON object.")
                return build_error_response(400, "validation", "Request body must be a JSON object.")

            return self._post_topic_message(
                topic_name,
                topic_message,
                self.handler_token.extract_token(event.get("headers", {})),
            )

        logger.warning("Request rejected: unsupported HTTP method.")
        return build_error_response(404, "route", "Resource not found")

    def _get_topic_schema(self, topic_name: str) -> dict[str, Any]:
        """Return the JSON schema for a specific topic.
        Args:
            topic_name: The topic whose schema is requested.
        Returns:
            API Gateway response with topic schema or error.
        """
        logger.debug("Handling GET topic schema.")

        if topic_name not in self.topics:
            logger.warning("Request rejected: unknown topic.", extra={"known_topics": sorted(self.topics)})
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
        logger.debug("Handling POST topic message.")

        if not self.access_config:
            logger.error("Access configuration not loaded.")
            raise RuntimeError("Access configuration not loaded")

        try:
            token: dict[str, Any] = self.handler_token.decode_jwt(token_encoded)
        except jwt.PyJWTError as exc:  # type: ignore[attr-defined]
            logger.warning(
                "Request rejected: token verification failed.",
                extra={"auth_error": type(exc).__name__, "token_present": bool(token_encoded)},
            )
            return build_error_response(401, "auth", "Invalid or missing token")

        if topic_name not in self.topics:
            logger.warning("Request rejected: unknown topic.", extra={"known_topics": sorted(self.topics)})
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found")

        user = token.get("sub")
        append_request_context(user=user)

        authorized_user = self._resolve_authorized_user(topic_name, user)
        if authorized_user is None:
            logger.warning("Request rejected: user is not authorized for the topic.")
            return build_error_response(403, "auth", f"User '{user}' is not authorized for topic '{topic_name}'")

        # Log under the configured spelling of the user, since the token casing may differ.
        append_request_context(user=authorized_user)

        allowed, perm_error = self._validate_user_permissions(topic_name, authorized_user, topic_message)
        if not allowed:
            logger.warning("Request rejected: user permissions do not allow the message.", extra={"reason": perm_error})
            return build_error_response(
                403,
                "permission",
                perm_error or f"Permission denied for user '{authorized_user}' for POST to topic '{topic_name}'",
            )

        logger.debug("User authorized for the topic.")

        try:
            validate(instance=topic_message, schema=self.topics[topic_name])
        except ValidationError as exc:
            logger.warning(
                "Request rejected: message does not match the topic schema.",
                extra={"validation_path": str(exc.json_path), "validator": str(exc.validator)},
            )
            return build_error_response(400, "validation", exc.message)

        message_key = self._resolve_message_key(topic_name, topic_message)
        writers_ok, errors = self._write_to_all(topic_name, topic_message, message_key)

        if errors:
            logger.error(
                "Message dispatch failed for at least one writer.",
                extra={
                    "writers_ok": writers_ok,
                    "writers_failed": [error["type"] for error in errors],
                    "writer_errors": errors,
                    "writer_count": len(self.writers),
                    "message_key": message_key,
                },
            )
            return {
                "statusCode": 500,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"success": False, "statusCode": 500, "errors": errors}),
            }

        # The outcome fields are appended to the request context so the single INFO line emitted
        # by `dispatch_request()` ("Request completed.") carries them; no second INFO line here.
        append_request_context(message_key=message_key, writers_ok=writers_ok)
        logger.debug("Message accepted.")
        return {
            "statusCode": 202,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"success": True, "statusCode": 202}),
        }

    def _write_to_all(
        self,
        topic_name: str,
        topic_message: dict[str, Any],
        message_key: str,
    ) -> tuple[list[str], list[dict[str, str]]]:
        """Dispatch a message to every configured writer, collecting per-writer outcomes.
        Args:
            topic_name: Target topic name.
            topic_message: Message payload.
            message_key: Resolved transport key.
        Returns:
            Tuple of (writers_ok, errors) where `writers_ok` lists the writers that accepted the
            message and `errors` holds one entry per failing writer.
        """
        writers_ok: list[str] = []
        errors: list[dict[str, str]] = []

        for writer_name, writer in self.writers.items():
            started_at = time.perf_counter()
            try:
                writer.write(topic_name, topic_message, message_key)
                writers_ok.append(writer_name)
                logger.debug(
                    "Writer accepted the message.",
                    extra={
                        "writer": writer_name,
                        "writer_duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                    },
                )
            except WriteError as exc:
                errors.append({"type": writer_name, "message": str(exc)})
                # WARNING on purpose: the request-level ERROR is emitted once by the caller with
                # every failed writer folded in, so an alert on ERROR counts one failure once.
                logger.warning(
                    "Writer failed to publish the message.",
                    extra={
                        "writer": writer_name,
                        "writer_duration_ms": round((time.perf_counter() - started_at) * 1000, 2),
                        "writer_error": str(exc),
                    },
                )

        return writers_ok, errors

    def _resolve_authorized_user(self, topic_name: str, user: str | None) -> str | None:
        """Match a token user to a configured user for a topic, ignoring case.
        Args:
            topic_name: Target topic name.
            user: User identifier from the token `sub` claim.
        Returns:
            The configured username (original casing) when authorized, otherwise `None`.
        """
        if user is None or topic_name not in self.access_config:
            return None

        for configured_user in self.access_config[topic_name]:
            if configured_user.casefold() == user.casefold():
                return configured_user

        return None

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

    def _resolve_message_key(self, topic_name: str, message: dict[str, Any]) -> str:
        """Resolve topic key value from message using configured field mapping.
        Args:
            topic_name: Target topic name.
            message: Topic payload.
        Returns:
            String key value for writers, or empty string when no key is configured/resolvable.
        """
        key_field = self.topic_keys.get(topic_name)
        if not key_field:
            return ""

        key_value = message.get(key_field)
        if key_value is None:
            logger.warning("Topic key field is missing from the message.", extra={"key_field": key_field})
            return ""

        if isinstance(key_value, (dict, list)):
            logger.warning("Topic key field is not a scalar value.", extra={"key_field": key_field})
            return ""

        return str(key_value)
