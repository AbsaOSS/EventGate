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

"""Custom logging levels.

Adds a TRACE level below DEBUG for very verbose payload logging.
"""

from __future__ import annotations
import logging

TRACE_LEVEL = 5

# Register level name only once (idempotent)
if not hasattr(logging, "TRACE"):
    logging.addLevelName(TRACE_LEVEL, "TRACE")

    def trace(self: logging.Logger, message: str, *args, **kws):  # type: ignore[override]
        """Log a message with TRACE level.

        Args:
            self: Logger instance.
            message: Log message format string.
            *args: Positional arguments for message formatting.
            **kws: Keyword arguments passed to _log.
        """
        if self.isEnabledFor(TRACE_LEVEL):
            self._log(TRACE_LEVEL, message, args, **kws)  # pylint: disable=protected-access

    logging.Logger.trace = trace  # type: ignore[attr-defined]

__all__ = ["TRACE_LEVEL"]
