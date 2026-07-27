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

"""AWS Lambda entry point for the EventGate service."""

import logging
import sys
import time
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

from src.handlers.handler_api import HandlerApi
from src.handlers.handler_health import HandlerHealth
from src.handlers.handler_token import HandlerToken
from src.handlers.handler_topic import HandlerTopic
from src.utils.conf_path import CONF_DIR, log_configuration_source
from src.utils.config_loader import load_config
from src.utils.constants import SSL_CA_BUNDLE_KEY
from src.utils.observability import configured_log_level, init_lambda_logging
from src.utils.utils import dispatch_request
from src.writers.writer_eventbridge import WriterEventBridge
from src.writers.writer_kafka import WriterKafka
from src.writers.writer_postgres import WriterPostgres

logger = init_lambda_logging(__name__)

_INIT_STARTED_AT = time.perf_counter()
logger.info("Initializing EventGate lambda.", extra={"log_level": configured_log_level()})

# Load main configuration
log_configuration_source(logger)
config = load_config(CONF_DIR)
logger.debug("Loaded main configuration.")

# Initialize S3 client with SSL verification
try:
    ssl_verify = config.get(SSL_CA_BUNDLE_KEY, True)
    aws_s3 = boto3.Session().resource("s3", verify=ssl_verify)
    logger.debug("Initialized AWS S3 client.")
except (BotoCoreError, NoCredentialsError) as exc:
    logger.exception("Failed to initialize AWS S3 client.")
    raise RuntimeError("AWS S3 client initialization failed.") from exc

# Initialize EventGate writers
writers = {
    "kafka": WriterKafka(config),
    "eventbridge": WriterEventBridge(config),
    "postgres": WriterPostgres(config),
}

# Initialize EventGate handlers
handler_token = HandlerToken(config).with_public_keys_queried()
handler_topic = (
    HandlerTopic(config, aws_s3, handler_token, writers)
    .with_load_access_config()
    .with_load_topic_keys_config()
    .with_load_topic_schemas()
)
handler_health = HandlerHealth(writers)
handler_api = HandlerApi().with_api_definition_loaded()

logger.info(
    "EventGate lambda initialized.",
    extra={
        "init_duration_ms": round((time.perf_counter() - _INIT_STARTED_AT) * 1000, 2),
        "writers": sorted(writers),
        "topics": sorted(handler_topic.topics),
    },
)

# Route to handler function mapping
ROUTE_MAP: dict[str, Any] = {
    "/api": lambda _: handler_api.get_api(),
    "/docs": lambda _: handler_api.get_docs(),
    "/token": lambda _: handler_token.get_token_provider_info(),
    "/health": lambda _: handler_health.get_health(),
    "/topics": lambda _: handler_topic.get_topics_list(),
    "/topics/{topic_name}": handler_topic.handle_request,
    "/terminate": lambda _: sys.exit("TERMINATING"),
}


def lambda_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """AWS Lambda entry point for EventGate. Dispatches based on API Gateway proxy resource field.
    Args:
        event: The event data from API Gateway.
        context: The AWS Lambda context object, used to enrich logs with the execution context.
    Returns:
        A dictionary compatible with API Gateway Lambda Proxy integration.
    """
    return dispatch_request(event, ROUTE_MAP, logger, context)
