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

"""AWS Lambda entry point for the EventStats service."""

import logging
import os
from typing import Any, Dict

import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

from src.handlers.handler_health import HandlerHealth
from src.handlers.handler_stats import HandlerStats
from src.handlers.handler_token import HandlerToken
from src.readers.reader_postgres import ReaderPostgres
from src.utils.conf_path import CONF_DIR, INVALID_CONF_ENV
from src.utils.config_loader import load_access_config, load_config, load_topic_names
from src.utils.constants import SSL_CA_BUNDLE_KEY
from src.utils.utils import dispatch_request


# Initialize logger
root_logger = logging.getLogger()
if not root_logger.handlers:
    root_logger.addHandler(logging.StreamHandler())

log_level = os.environ.get("LOG_LEVEL", "INFO")
root_logger.setLevel(log_level)
logger = logging.getLogger(__name__)
logger.debug("Initialized EventStats logger with level %s.", log_level)

# Load main configuration
logger.debug("Using CONF_DIR=%s.", CONF_DIR)
if INVALID_CONF_ENV:
    logger.warning("CONF_DIR env var set to non-existent path: %s; fell back to %s.", INVALID_CONF_ENV, CONF_DIR)

config = load_config(CONF_DIR)

# Initialize S3 client with SSL verification
try:
    ssl_verify = config.get(SSL_CA_BUNDLE_KEY, True)
    aws_s3 = boto3.Session().resource("s3", verify=ssl_verify)
    logger.debug("Initialized AWS S3 client.")
except (BotoCoreError, NoCredentialsError) as exc:
    logger.exception("Failed to initialize AWS S3 client.")
    raise RuntimeError("AWS S3 client initialization failed.") from exc

# Load topic names and access control.
topic_names = load_topic_names(CONF_DIR)
topics: Dict[str, Dict[str, Any]] = {name: {} for name in topic_names}
access_config = load_access_config(config, aws_s3)

# Initialize EventStats readers
reader_postgres = ReaderPostgres(config)

# Initialize EventStats handlers
handler_token = HandlerToken(config).with_public_keys_queried()
handler_stats = HandlerStats(config, handler_token, reader_postgres, topics, access_config)
handler_health = HandlerHealth({"postgres_reader": reader_postgres})

# Route to handler function mapping
ROUTE_MAP: Dict[str, Any] = {
    "/stats/{topic_name}": handler_stats.handle_request,
    "/health": lambda _: handler_health.get_health(),
}


def lambda_handler(event: Dict[str, Any], _context: Any = None) -> Dict[str, Any]:
    """AWS Lambda entry point for EventGate. Dispatches based on API Gateway proxy resource field.
    Args:
        event: The event data from API Gateway.
        _context: The mandatory context argument for AWS Lambda invocation (unused).
    Returns:
        A dictionary compatible with API Gateway Lambda Proxy integration.
    """
    return dispatch_request(event, ROUTE_MAP, logger)
