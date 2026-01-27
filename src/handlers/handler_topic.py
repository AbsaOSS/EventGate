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
from boto3.resources.base import ServiceResource
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from src.handlers.handler_token import HandlerToken
from src.utils.utils import build_error_response

logger = logging.getLogger(__name__)


class HandlerTopic:
    """
    HandlerTopic manages topic schemas, access control, and message posting.
    """

    def __init__(self, conf_dir: str, config: Dict[str, Any], aws_s3: ServiceResource, handler_token: HandlerToken):
        self.conf_dir = conf_dir
        self.config = config
        self.aws_s3 = aws_s3
        self.handler_token = handler_token
        self.access_config: Dict[str, list[str]] = {}
        self.topics: Dict[str, Dict[str, Any]] = {}

    def load_access_config(self) -> "HandlerTopic":
        """
        Load access control configuration from S3 or local file.
        Returns:
            HandlerTopic: The current instance with loaded access config.
        """
        access_path = self.config["access_config"]
        logger.debug("Loading access configuration from %s", access_path)

        if access_path.startswith("s3://"):
            name_parts = access_path.split("/")
            bucket_name = name_parts[2]
            bucket_object_key = "/".join(name_parts[3:])
            self.access_config = json.loads(
                self.aws_s3.Bucket(bucket_name).Object(bucket_object_key).get()["Body"].read().decode("utf-8")
            )
        else:
            with open(access_path, "r", encoding="utf-8") as file:
                self.access_config = json.load(file)

        logger.debug("Loaded access configuration")
        return self

    def load_topic_schemas(self) -> "HandlerTopic":
        """
        Load topic schemas from configuration files.
        Returns:
            HandlerTopic: The current instance with loaded topic schemas.
        """
        topic_schemas_dir = os.path.join(self.conf_dir, "topic_schemas")
        logger.debug("Loading topic schemas from %s", topic_schemas_dir)

        with open(os.path.join(topic_schemas_dir, "runs.json"), "r", encoding="utf-8") as file:
            self.topics["public.cps.za.runs"] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "dlchange.json"), "r", encoding="utf-8") as file:
            self.topics["public.cps.za.dlchange"] = json.load(file)
        with open(os.path.join(topic_schemas_dir, "test.json"), "r", encoding="utf-8") as file:
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

    def handle_request(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle GET/POST requests for /topics/{topic_name} resource.

        Args:
            event: The API Gateway event containing path parameters, method, body, and headers.
        Returns:
            Dict[str, Any]: API Gateway response.
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

    def _get_topic_schema(self, topic_name: str) -> Dict[str, Any]:
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

    def _post_topic_message(self, topic_name: str, topic_message: Dict[str, Any], token_encoded: str) -> Dict[str, Any]:
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
