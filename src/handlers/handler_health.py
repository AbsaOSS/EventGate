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
This module provides the HandlerHealth class for service health monitoring.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any

from src.writers import writer_eventbridge, writer_kafka, writer_postgres

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class HandlerHealth:
    """
    HandlerHealth manages service health checks and dependency status monitoring.
    """

    def __init__(self):
        self.start_time: datetime = datetime.now(timezone.utc)

    def get_health(self) -> Dict[str, Any]:
        """
        Check service health and return status.

        Returns:
            Dict[str, Any]: API Gateway response with health status.
                - 200: All dependencies healthy
                - 503: One or more dependencies not initialized
        """
        logger.debug("Handling GET Health")

        failures: Dict[str, str] = {}

        # Check Kafka writer
        if writer_kafka.STATE.get("producer") is None:
            failures["kafka"] = "producer not initialized"

        # Check EventBridge writer
        eventbus_arn = writer_eventbridge.STATE.get("event_bus_arn")
        eventbridge_client = writer_eventbridge.STATE.get("client")
        if eventbus_arn:
            if eventbridge_client is None:
                failures["eventbridge"] = "client not initialized"

        # Check PostgreSQL writer
        postgres_config = writer_postgres.POSTGRES
        if postgres_config.get("database"):
            if not postgres_config.get("host"):
                failures["postgres"] = "host not configured"
            elif not postgres_config.get("user"):
                failures["postgres"] = "user not configured"
            elif not postgres_config.get("password"):
                failures["postgres"] = "password not configured"
            elif not postgres_config.get("port"):
                failures["postgres"] = "port not configured"

        uptime_seconds = int((datetime.now(timezone.utc) - self.start_time).total_seconds())

        if not failures:
            logger.debug("Health check passed")
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"status": "ok", "uptime_seconds": uptime_seconds}),
            }

        logger.debug("Health check degraded: %s", failures)
        return {
            "statusCode": 503,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "degraded", "failures": failures}),
        }
