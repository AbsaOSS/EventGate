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

"""Event Gate Lambda function implementation."""
import json
import logging
import os
import sys
from typing import Any, Dict

import boto3
import jwt
from botocore.exceptions import BotoCoreError, NoCredentialsError
from jsonschema import validate
from jsonschema.exceptions import ValidationError

from src.handlers.handler_token import HandlerToken
from src.utils.constants import SSL_CA_BUNDLE_KEY
from src.writers import writer_eventbridge, writer_kafka, writer_postgres
from src.utils.conf_path import CONF_DIR, INVALID_CONF_ENV

# Internal aliases used by rest of module
_CONF_DIR = CONF_DIR
_INVALID_CONF_ENV = INVALID_CONF_ENV


logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())
logger.debug("Initialized LOGGER")
logger.debug("Using CONF_DIR=%s", _CONF_DIR)
if _INVALID_CONF_ENV:
    logger.warning("CONF_DIR env var set to non-existent path: %s; fell back to %s", _INVALID_CONF_ENV, _CONF_DIR)

with open(os.path.join(_CONF_DIR, "api.yaml"), "r", encoding="utf-8") as file:
    API = file.read()
logger.debug("Loaded API definition")

TOPICS: Dict[str, Dict[str, Any]] = {}
with open(os.path.join(_CONF_DIR, "topic_runs.json"), "r", encoding="utf-8") as file:
    TOPICS["public.cps.za.runs"] = json.load(file)
with open(os.path.join(_CONF_DIR, "topic_dlchange.json"), "r", encoding="utf-8") as file:
    TOPICS["public.cps.za.dlchange"] = json.load(file)
with open(os.path.join(_CONF_DIR, "topic_test.json"), "r", encoding="utf-8") as file:
    TOPICS["public.cps.za.test"] = json.load(file)
logger.debug("Loaded TOPICS")

with open(os.path.join(_CONF_DIR, "config.json"), "r", encoding="utf-8") as file:
    config = json.load(file)
logger.debug("Loaded main CONFIG")

# Initialize S3 client with SSL verification
try:
    ssl_verify = config.get(SSL_CA_BUNDLE_KEY, True)
    aws_s3 = boto3.Session().resource("s3", verify=ssl_verify)
    logger.debug("Initialized AWS S3 Client")
except (BotoCoreError, NoCredentialsError) as exc:
    logger.exception("Failed to initialize AWS S3 client")
    raise RuntimeError("AWS S3 client initialization failed") from exc

if config["access_config"].startswith("s3://"):
    name_parts = config["access_config"].split("/")
    BUCKET_NAME = name_parts[2]
    BUCKET_OBJECT_KEY = "/".join(name_parts[3:])
    ACCESS = json.loads(aws_s3.Bucket(BUCKET_NAME).Object(BUCKET_OBJECT_KEY).get()["Body"].read().decode("utf-8"))
else:
    with open(config["access_config"], "r", encoding="utf-8") as file:
        ACCESS = json.load(file)
logger.debug("Loaded ACCESS definitions")

# Initialize token handler and load token public keys
handler_token = HandlerToken(config).load_public_keys()

# Initialize EventGate writers
writer_eventbridge.init(logger, config)
writer_kafka.init(logger, config)
writer_postgres.init(logger)


def _error_response(status: int, err_type: str, message: str) -> Dict[str, Any]:
    """Build a standardized JSON error response body.

    Args:
        status: HTTP status code.
        err_type: A short error classifier (e.g. 'auth', 'validation').
        message: Human readable error description.
    Returns:
        A dictionary compatible with API Gateway Lambda Proxy integration.
    """
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {
                "success": False,
                "statusCode": status,
                "errors": [{"type": err_type, "message": message}],
            }
        ),
    }


def get_api() -> Dict[str, Any]:
    """Return the OpenAPI specification text."""
    return {"statusCode": 200, "body": API}


def get_topics() -> Dict[str, Any]:
    """Return list of available topic names."""
    logger.debug("Handling GET Topics")
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(list(TOPICS)),
    }


def get_topic_schema(topic_name: str) -> Dict[str, Any]:
    """Return the JSON schema for a specific topic.

    Args:
        topic_name: The topic whose schema is requested.
    """
    logger.debug("Handling GET TopicSchema(%s)", topic_name)
    if topic_name not in TOPICS:
        return _error_response(404, "topic", f"Topic '{topic_name}' not found")

    return {"statusCode": 200, "headers": {"Content-Type": "application/json"}, "body": json.dumps(TOPICS[topic_name])}


def post_topic_message(topic_name: str, topic_message: Dict[str, Any], token_encoded: str) -> Dict[str, Any]:
    """Validate auth and schema; dispatch message to all writers.

    Args:
        topic_name: Target topic name.
        topic_message: JSON message payload.
        token_encoded: Encoded bearer JWT token string.
    """
    logger.debug("Handling POST %s", topic_name)
    try:
        token: Dict[str, Any] = handler_token.decode_jwt(token_encoded)
    except jwt.PyJWTError:  # type: ignore[attr-defined]
        return _error_response(401, "auth", "Invalid or missing token")

    if topic_name not in TOPICS:
        return _error_response(404, "topic", f"Topic '{topic_name}' not found")

    user = token.get("sub")
    if topic_name not in ACCESS or user not in ACCESS[topic_name]:  # type: ignore[index]
        return _error_response(403, "auth", "User not authorized for topic")

    try:
        validate(instance=topic_message, schema=TOPICS[topic_name])
    except ValidationError as exc:
        return _error_response(400, "validation", exc.message)

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


def lambda_handler(event: Dict[str, Any], context: Any):  # pylint: disable=unused-argument,too-many-return-statements
    """AWS Lambda entry point.

    Dispatches based on API Gateway proxy 'resource' and 'httpMethod'.
    """
    try:
        resource = event.get("resource", "").lower()
        if resource == "/api":
            return get_api()
        if resource == "/token":
            return handler_token.get_token_provider_info()
        if resource == "/topics":
            return get_topics()
        if resource == "/topics/{topic_name}":
            method = event.get("httpMethod")
            if method == "GET":
                return get_topic_schema(event["pathParameters"]["topic_name"].lower())
            if method == "POST":
                return post_topic_message(
                    event["pathParameters"]["topic_name"].lower(),
                    json.loads(event["body"]),
                    handler_token.extract_token(event.get("headers", {})),
                )
        if resource == "/terminate":
            sys.exit("TERMINATING")  # pragma: no cover - deliberate termination path
        return _error_response(404, "route", "Resource not found")
    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Unexpected exception: %s", exc)
        return _error_response(500, "internal", "Unexpected server error")
