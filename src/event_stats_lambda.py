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
import time
from typing import Any

from src.handlers.handler_health import HandlerHealth
from src.handlers.handler_stats import HandlerStats
from src.readers.reader_postgres import ReaderPostgres
from src.utils.conf_path import CONF_DIR, log_configuration_source
from src.utils.config_loader import load_topic_names
from src.utils.observability import configured_log_level, init_lambda_logging
from src.utils.utils import dispatch_request

logger = init_lambda_logging(__name__)

_INIT_STARTED_AT = time.perf_counter()
logger.info("Initializing EventStats lambda.", extra={"log_level": configured_log_level()})

# Load main configuration
log_configuration_source(logger)

# Load topic names.
topic_names = load_topic_names(CONF_DIR)
topics: dict[str, dict[str, Any]] = {name: {} for name in topic_names}

# Initialize EventStats readers
reader_postgres = ReaderPostgres()

# Initialize EventStats handlers
handler_stats = HandlerStats(topics, reader_postgres)
handler_health = HandlerHealth({"postgres_reader": reader_postgres})

logger.info(
    "EventStats lambda initialized.",
    extra={
        "init_duration_ms": round((time.perf_counter() - _INIT_STARTED_AT) * 1000, 2),
        "topics": sorted(topics),
    },
)

# Route to handler function mapping
ROUTE_MAP: dict[str, Any] = {
    "/stats/{topic_name}": handler_stats.handle_request,
    "/health": lambda _: handler_health.get_health(),
}


def lambda_handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    """AWS Lambda entry point for EventStats. Dispatches based on API Gateway proxy resource field.
    Args:
        event: The event data from API Gateway.
        context: The AWS Lambda context object, used to enrich logs with the execution context.
    Returns:
        A dictionary compatible with API Gateway Lambda Proxy integration.
    """
    return dispatch_request(event, ROUTE_MAP, logger, context)
