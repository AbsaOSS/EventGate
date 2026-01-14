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
EventBridge writer module.
Provides initialization and write functionality for publishing events to AWS EventBridge.
"""

import json
import logging
import os
from typing import Any, Dict, Optional, Tuple, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.trace_logging import log_payload_at_trace
from src.writers.writer import Writer

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class WriterEventBridge(Writer):
    """
    EventBridge writer for publishing events to AWS EventBridge.
    The boto3 EventBridge client is created on the first write() call.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._client: Optional["boto3.client"] = None
        self._entries: List[Dict[str, Any]] = []
        self.event_bus_arn: str = config.get("event_bus_arn", "")
        logger.debug("Initialized EventBridge writer")

    def _format_failed_entries(self) -> str:
        """
        Format failed EventBridge entries for error message.

        Returns:
            JSON string of failed entries.
        """
        failed = [e for e in self._entries if "ErrorCode" in e or "ErrorMessage" in e]
        return json.dumps(failed) if failed else "[]"

    def write(self, topic_name: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Publish a message to EventBridge.

        Args:
            topic_name: Target EventBridge writer topic (destination) name.
            message: JSON-serializable payload to publish.
        Returns:
            Tuple of (success: bool, error_message: Optional[str]).
        """
        if not self.event_bus_arn:
            logger.debug("No EventBus Arn - skipping EventBridge writer")
            return True, None

        if self._client is None:
            self._client = boto3.client("events")
            logger.debug("EventBridge client initialized")

        log_payload_at_trace(logger, "EventBridge", topic_name, message)

        try:
            logger.debug("Sending to EventBridge %s", topic_name)
            response = self._client.put_events(
                Entries=[
                    {
                        "Source": topic_name,
                        "DetailType": "JSON",
                        "Detail": json.dumps(message),
                        "EventBusName": self.event_bus_arn,
                    }
                ]
            )
            failed_count = response.get("FailedEntryCount", 0)
            if failed_count > 0:
                self._entries = response.get("Entries", [])
                failed_repr = self._format_failed_entries()
                msg = f"{failed_count} EventBridge entries failed: {failed_repr}"
                logger.error(msg)
                return False, msg
        except (BotoCoreError, ClientError) as err:  # explicit AWS client-related errors
            logger.exception("EventBridge put_events call failed")
            return False, str(err)

        return True, None
