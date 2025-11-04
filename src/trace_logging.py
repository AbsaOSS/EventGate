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

"""Trace-level logging utilities.

Provides reusable TRACE-level payload logging for writer modules.
"""

import logging
from typing import Any, Dict

from .logging_levels import TRACE_LEVEL
from .safe_serialization import safe_serialize_for_log


def log_payload_at_trace(logger: logging.Logger, writer_name: str, topic_name: str, message: Dict[str, Any]) -> None:
    """Log message payload at TRACE level with safe serialization.

    Args:
        logger: Logger instance to use for logging.
        writer_name: Name of the writer (e.g., "EventBridge", "Kafka", "Postgres").
        topic_name: Topic name being written to.
        message: Message payload to log.
    """
    if not logger.isEnabledFor(TRACE_LEVEL):
        return

    try:
        safe_payload = safe_serialize_for_log(message)
        if safe_payload:
            logger.trace(  # type: ignore[attr-defined]
                "%s payload topic=%s payload=%s", writer_name, topic_name, safe_payload
            )
    except (TypeError, ValueError):  # pragma: no cover - defensive serialization guard
        logger.trace("%s payload topic=%s <unserializable>", writer_name, topic_name)  # type: ignore[attr-defined]


__all__ = ["log_payload_at_trace"]
