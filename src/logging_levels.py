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
        if self.isEnabledFor(TRACE_LEVEL):
            self._log(TRACE_LEVEL, message, args, **kws)  # pylint: disable=protected-access

    logging.Logger.trace = trace  # type: ignore[attr-defined]

__all__ = ["TRACE_LEVEL"]
