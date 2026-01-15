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
from botocore.exceptions import BotoCoreError, NoCredentialsError

from src.handlers.handler_token import HandlerToken
from src.handlers.handler_topic import HandlerTopic
from src.handlers.handler_health import HandlerHealth
from src.utils.constants import SSL_CA_BUNDLE_KEY
from src.utils.utils import build_error_response
from src.writers.writer_eventbridge import WriterEventBridge
from src.writers.writer_kafka import WriterKafka
from src.writers.writer_postgres import WriterPostgres
from src.utils.conf_path import CONF_DIR, INVALID_CONF_ENV


# Initialize logger
logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)
if not logger.handlers:
    logger.addHandler(logging.StreamHandler())
logger.debug("Initialized logger with level %s", log_level)

# Load main configuration
logger.debug("Using CONF_DIR=%s", CONF_DIR)
if INVALID_CONF_ENV:
    logger.warning("CONF_DIR env var set to non-existent path: %s; fell back to %s", INVALID_CONF_ENV, CONF_DIR)
with open(os.path.join(CONF_DIR, "config.json"), "r", encoding="utf-8") as file:
    config = json.load(file)
logger.debug("Loaded main configuration")

# Load API definition
with open(os.path.join(CONF_DIR, "api.yaml"), "r", encoding="utf-8") as file:
    API = file.read()
logger.debug("Loaded API definition")

# Initialize S3 client with SSL verification
try:
    ssl_verify = config.get(SSL_CA_BUNDLE_KEY, True)
    aws_s3 = boto3.Session().resource("s3", verify=ssl_verify)
    logger.debug("Initialized AWS S3 Client")
except (BotoCoreError, NoCredentialsError) as exc:
    logger.exception("Failed to initialize AWS S3 client")
    raise RuntimeError("AWS S3 client initialization failed") from exc

# Load access configuration
ACCESS: Dict[str, list[str]] = {}
if config["access_config"].startswith("s3://"):
    name_parts = config["access_config"].split("/")
    BUCKET_NAME = name_parts[2]
    BUCKET_OBJECT_KEY = "/".join(name_parts[3:])
    ACCESS = json.loads(aws_s3.Bucket(BUCKET_NAME).Object(BUCKET_OBJECT_KEY).get()["Body"].read().decode("utf-8"))
else:
    with open(config["access_config"], "r", encoding="utf-8") as file:
        ACCESS = json.load(file)
logger.debug("Loaded access configuration")

# Initialize token handler and load token public keys
handler_token = HandlerToken(config).load_public_keys()

# Initialize EventGate writers
writers = {
    "kafka": WriterKafka(config),
    "eventbridge": WriterEventBridge(config),
    "postgres": WriterPostgres(config),
}

# Initialize topic handler and load topic schemas
handler_topic = HandlerTopic(CONF_DIR, ACCESS, handler_token, writers).load_topic_schemas()

# Initialize health handler
handler_health = HandlerHealth(writers)


def get_api() -> Dict[str, Any]:
    """Return the OpenAPI specification text."""
    return {"statusCode": 200, "body": API}


def lambda_handler(event: Dict[str, Any], _context: Any = None) -> Dict[str, Any]:
    """
    AWS Lambda entry point. Dispatches based on API Gateway proxy 'resource' and 'httpMethod'.
    Args:
        event: The event data from API Gateway.
        _context: The mandatory context argument for AWS Lambda invocation (unused).
    Returns:
        A dictionary compatible with API Gateway Lambda Proxy integration.
    Raises:
        Request exception: For unexpected errors.
    """
    try:
        resource = event.get("resource", "").lower()
        if resource == "/api":
            return get_api()
        if resource == "/token":
            return handler_token.get_token_provider_info()
        if resource == "/health":
            return handler_health.get_health()
        if resource == "/topics":
            return handler_topic.get_topics_list()
        if resource == "/topics/{topic_name}":
            method = event.get("httpMethod")
            if method == "GET":
                return handler_topic.get_topic_schema(event["pathParameters"]["topic_name"].lower())
            if method == "POST":
                return handler_topic.post_topic_message(
                    event["pathParameters"]["topic_name"].lower(),
                    json.loads(event["body"]),
                    handler_token.extract_token(event.get("headers", {})),
                )
        if resource == "/terminate":
            sys.exit("TERMINATING")
        return build_error_response(404, "route", "Resource not found")
    except (KeyError, json.JSONDecodeError, ValueError, AttributeError, TypeError, RuntimeError) as request_exc:
        logger.exception("Request processing error: %s", request_exc)
        return build_error_response(500, "internal", "Unexpected server error")
