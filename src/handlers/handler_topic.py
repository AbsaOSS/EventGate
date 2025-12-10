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

"""
This module provides the HandlerTopic class for managing topic-related operations.
"""
import json
import logging
import os
from typing import Dict, Any

import jwt
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from src.handlers.handler_token import HandlerToken
from src.utils.utils import build_error_response
from src.writers import writer_eventbridge, writer_kafka, writer_postgres

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class HandlerTopic:
    """
    HandlerTopic manages topic schemas, access control, and message posting.
    """

    def __init__(self, conf_dir: str, access_config: Dict[str, list[str]], handler_token: HandlerToken):
        self.conf_dir = conf_dir
        self.access_config = access_config
        self.handler_token = handler_token
        self.topics: Dict[str, Dict[str, Any]] = {}

    def load_topic_schemas(self) -> "HandlerTopic":
        """
        Load topic schemas from configuration files.
        Returns:
            HandlerTopic: The current instance with loaded topic schemas.
        """
        logger.debug("Loading topic schemas from %s", self.conf_dir)

        with open(os.path.join(self.conf_dir, "topic_runs.json"), "r", encoding="utf-8") as file:
            self.topics["public.cps.za.runs"] = json.load(file)
        with open(os.path.join(self.conf_dir, "topic_dlchange.json"), "r", encoding="utf-8") as file:
            self.topics["public.cps.za.dlchange"] = json.load(file)
        with open(os.path.join(self.conf_dir, "topic_test.json"), "r", encoding="utf-8") as file:
            self.topics["public.cps.za.test"] = json.load(file)

        logger.debug("Loaded topic schemas successfully")
        return self

    def get_topics_list(self) -> Dict[str, Any]:
        """
        Return the list of available topics.
        Returns:
            Dict[str, Any]: API Gateway response with topic list.
        """
        logger.debug("Handling GET Topics")
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(list(self.topics)),
        }

    def get_topic_schema(self, topic_name: str) -> Dict[str, Any]:
        """
        Return the JSON schema for a specific topic.
        Args:
            topic_name: The topic whose schema is requested.
        Returns:
            Dict[str, Any]: API Gateway response with topic schema or error.
        """
        logger.debug("Handling GET TopicSchema(%s)", topic_name)

        if topic_name not in self.topics:
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found")

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(self.topics[topic_name]),
        }

    def post_topic_message(self, topic_name: str, topic_message: Dict[str, Any], token_encoded: str) -> Dict[str, Any]:
        """
        Validate auth and schema; dispatch message to all writers.
        Args:
            topic_name: Target topic name.
            topic_message: JSON message payload.
            token_encoded: Encoded bearer JWT token string.
        Returns:
            Dict[str, Any]: API Gateway response indicating success or failure.
        Raises:
            jwt.PyJWTError: If token decoding fails.
            ValidationError: If message validation fails.
        """
        logger.debug("Handling POST TopicMessage(%s)", topic_name)

        try:
            token: Dict[str, Any] = self.handler_token.decode_jwt(token_encoded)
        except jwt.PyJWTError:  # type: ignore[attr-defined]
            return build_error_response(401, "auth", "Invalid or missing token")

        if topic_name not in self.topics:
            return build_error_response(404, "topic", f"Topic '{topic_name}' not found")

        user = token.get("sub")
        if topic_name not in self.access_config or user not in self.access_config[topic_name]:
            return build_error_response(403, "auth", "User not authorized for topic")

        try:
            validate(instance=topic_message, schema=self.topics[topic_name])
        except ValidationError as exc:
            return build_error_response(400, "validation", exc.message)

        kafka_ok, kafka_err = writer_kafka.write(topic_name, topic_message)
        eventbridge_ok, eventbridge_err = writer_eventbridge.write(topic_name, topic_message)
        postgres_ok, postgres_err = writer_postgres.write(topic_name, topic_message)

        errors = []
        if not kafka_ok:
            errors.append({"type": "kafka", "message": kafka_err})
        if not eventbridge_ok:
            errors.append({"type": "eventbridge", "message": eventbridge_err})
        if not postgres_ok:
            errors.append({"type": "postgres", "message": postgres_err})

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
