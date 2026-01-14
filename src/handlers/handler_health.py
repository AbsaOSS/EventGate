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

from src.writers.writer import Writer

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class HandlerHealth:
    """
    HandlerHealth manages service health checks and dependency status monitoring.
    """

    def __init__(
        self,
        writer_eventbridge: Writer,
        writer_kafka: Writer,
        writer_postgres: Writer,
    ):
        self.start_time: datetime = datetime.now(timezone.utc)
        self.writer_eventbridge = writer_eventbridge
        self.writer_kafka = writer_kafka
        self.writer_postgres = writer_postgres

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

        for name, writer in [
            ("kafka", self.writer_kafka),
            ("eventbridge", self.writer_eventbridge),
            ("postgres", self.writer_postgres),
        ]:
            healthy, msg = writer.check_health()
            if not healthy:
                failures[name] = msg

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
