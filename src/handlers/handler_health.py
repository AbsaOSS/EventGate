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
from datetime import datetime, timezone
from typing import Dict, Any

from src.writers import writer_eventbridge, writer_kafka, writer_postgres


class HandlerHealth:
    """
    HandlerHealth manages service health checks and dependency status monitoring.
    """

    def __init__(self, logger_instance: logging.Logger, config: Dict[str, Any]):
        """
        Initialize the health handler.

        Args:
            logger_instance: Shared application logger.
            config: Configuration dictionary.
        """
        self.logger = logger_instance
        self.config = config
        self.start_time = datetime.now(timezone.utc)

    def get_health(self) -> Dict[str, Any]:
        """
        Check service health and return status.

        Performs lightweight dependency checks by verifying that writer STATE
        dictionaries are properly initialized with required keys.

        Returns:
            Dict[str, Any]: API Gateway response with health status.
                - 200: All dependencies healthy
                - 503: One or more dependencies not initialized
        """
        self.logger.debug("Handling GET Health")

        details: Dict[str, str] = {}
        all_healthy = True

        # Check Kafka writer STATE
        kafka_state = writer_kafka.STATE
        if not all(key in kafka_state for key in ["logger", "producer"]):
            details["kafka"] = "not_initialized"
            all_healthy = False
            self.logger.debug("Kafka writer not properly initialized")

        # Check EventBridge writer STATE
        eventbridge_state = writer_eventbridge.STATE
        if not all(key in eventbridge_state for key in ["logger", "client", "event_bus_arn"]):
            details["eventbridge"] = "not_initialized"
            all_healthy = False
            self.logger.debug("EventBridge writer not properly initialized")

        # Check PostgreSQL writer - it uses global logger variable and POSTGRES dict
        # Just verify the module is accessible (init is always called in event_gate_lambda)
        try:
            _ = writer_postgres.logger
        except AttributeError:
            details["postgres"] = "not_initialized"
            all_healthy = False
            self.logger.debug("PostgreSQL writer not accessible")

        # Calculate uptime
        uptime_seconds = int((datetime.now(timezone.utc) - self.start_time).total_seconds())

        if all_healthy:
            self.logger.debug("Health check passed - all dependencies healthy")
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"status": "ok", "uptime_seconds": uptime_seconds}),
            }

        self.logger.debug("Health check degraded - some dependencies not initialized: %s", details)
        return {
            "statusCode": 503,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"status": "degraded", "details": details}),
        }
