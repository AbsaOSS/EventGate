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
This module contains the AWS Lambda entry point for the EventGate service.
"""

import json
import logging
import os
import sys
from typing import Any, Dict

import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

from src.handlers.handler_api import HandlerApi
from src.handlers.handler_token import HandlerToken
from src.handlers.handler_topic import HandlerTopic
from src.handlers.handler_health import HandlerHealth
from src.utils.constants import SSL_CA_BUNDLE_KEY
from src.utils.utils import build_error_response
from src.writers import writer_eventbridge, writer_kafka, writer_postgres
from src.utils.conf_path import CONF_DIR, INVALID_CONF_ENV


# Initialize logger
root_logger = logging.getLogger()
if not root_logger.handlers:
    root_logger.addHandler(logging.StreamHandler())

log_level = os.environ.get("LOG_LEVEL", "INFO")
root_logger.setLevel(log_level)
logger = logging.getLogger(__name__)
logger.debug("Initialized logger with level %s", log_level)

# Load main configuration
logger.debug("Using CONF_DIR=%s", CONF_DIR)
if INVALID_CONF_ENV:
    logger.warning("CONF_DIR env var set to non-existent path: %s; fell back to %s", INVALID_CONF_ENV, CONF_DIR)
with open(os.path.join(CONF_DIR, "config.json"), "r", encoding="utf-8") as file:
    config = json.load(file)
logger.debug("Loaded main configuration")

# Initialize S3 client with SSL verification
try:
    ssl_verify = config.get(SSL_CA_BUNDLE_KEY, True)
    aws_s3 = boto3.Session().resource("s3", verify=ssl_verify)
    logger.debug("Initialized AWS S3 Client")
except (BotoCoreError, NoCredentialsError) as exc:
    logger.exception("Failed to initialize AWS S3 client")
    raise RuntimeError("AWS S3 client initialization failed") from exc

# Initialize EventGate writers
writer_eventbridge.init(logger, config)
writer_kafka.init(logger, config)
writer_postgres.init(logger)

# Initialize EventGate handlers
handler_token = HandlerToken(config).load_public_keys()
handler_topic = HandlerTopic(CONF_DIR, config, aws_s3, handler_token).load_access_config().load_topic_schemas()
handler_health = HandlerHealth()
handler_api = HandlerApi().load_api_definition()


# Route to handler function mapping
ROUTE_MAP: Dict[str, Any] = {
    "/api": lambda _: handler_api.get_api(),
    "/token": lambda _: handler_token.get_token_provider_info(),
    "/health": lambda _: handler_health.get_health(),
    "/topics": lambda _: handler_topic.get_topics_list(),
    "/topics/{topic_name}": handler_topic.handle_request,
    "/terminate": lambda _: sys.exit("TERMINATING"),
}


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
        route_function = ROUTE_MAP.get(resource)

        if route_function:
            return route_function(event)

        return build_error_response(404, "route", "Resource not found")
    except (KeyError, json.JSONDecodeError, ValueError, AttributeError, TypeError, RuntimeError) as request_exc:
        logger.exception("Request processing error: %s", request_exc)
        return build_error_response(500, "internal", "Unexpected server error")
