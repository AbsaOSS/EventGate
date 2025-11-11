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

"""EventBridge writer module.

Provides initialization and write functionality for publishing events to AWS EventBridge.
"""

import json
import logging
from typing import Any, Dict, Optional, Tuple, List

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from src.utils.trace_logging import log_payload_at_trace

STATE: Dict[str, Any] = {"logger": logging.getLogger(__name__), "event_bus_arn": "", "client": None}


def init(logger: logging.Logger, config: Dict[str, Any]) -> None:
    """Initialize the EventBridge writer.

    Args:
        logger: Shared application logger.
        config: Configuration dictionary (expects optional 'event_bus_arn').
    """
    STATE["logger"] = logger
    STATE["client"] = boto3.client("events")
    STATE["event_bus_arn"] = config.get("event_bus_arn", "")
    STATE["logger"].debug("Initialized EVENTBRIDGE writer")


def _format_failed_entries(entries: List[Dict[str, Any]]) -> str:
    failed = [e for e in entries if "ErrorCode" in e or "ErrorMessage" in e]
    # Keep message concise but informative
    return json.dumps(failed) if failed else "[]"


def write(topic_name: str, message: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Publish a message to EventBridge.

    Args:
        topic_name: Source topic name used as event Source.
        message: JSON-serializable payload.
    Returns:
        Tuple of success flag and optional error message.
    """
    logger = STATE["logger"]
    event_bus_arn = STATE["event_bus_arn"]
    client = STATE["client"]

    if not event_bus_arn:
        logger.debug("No EventBus Arn - skipping")
        return True, None
    if client is None:  # defensive
        logger.debug("EventBridge client not initialized - skipping")
        return True, None

    log_payload_at_trace(logger, "EventBridge", topic_name, message)

    try:
        logger.debug("Sending to eventBridge %s", topic_name)
        response = client.put_events(
            Entries=[
                {
                    "Source": topic_name,
                    "DetailType": "JSON",
                    "Detail": json.dumps(message),
                    "EventBusName": event_bus_arn,
                }
            ]
        )
        failed_count = response.get("FailedEntryCount", 0)
        if failed_count > 0:
            entries = response.get("Entries", [])
            failed_repr = _format_failed_entries(entries)
            msg = f"{failed_count} EventBridge entries failed: {failed_repr}"
            logger.error(msg)
            return False, msg
    except (BotoCoreError, ClientError) as err:  # explicit AWS client-related errors
        logger.exception("EventBridge put_events call failed")
        return False, str(err)

    # Let any unexpected exception propagate for upstream handler (avoids broad except BLE001 / TRY400)
    return True, None
