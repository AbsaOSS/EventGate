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

"""Safe serialization utilities for logging.

Provides PII-safe, size-bounded JSON serialization for TRACE logging.
"""

import json
import os
from typing import Any, List, Set


def _redact_sensitive_keys(obj: Any, redact_keys: Set[str]) -> Any:
    """Recursively redact sensitive keys from nested structures.

    Args:
        obj: Object to redact (dict, list, or scalar).
        redact_keys: Set of key names to redact (case-insensitive).

    Returns:
        Copy of obj with sensitive values replaced by "***REDACTED***".
    """
    if isinstance(obj, dict):
        return {
            k: "***REDACTED***" if k.lower() in redact_keys else _redact_sensitive_keys(v, redact_keys)
            for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_redact_sensitive_keys(item, redact_keys) for item in obj]
    return obj


def safe_serialize_for_log(message: Any, redact_keys: List[str] | None = None, max_bytes: int | None = None) -> str:
    """Safely serialize a message for logging with redaction and size capping.

    Args:
        message: Object to serialize (typically a dict).
        redact_keys: List of key names to redact (case-insensitive). If None, uses env TRACE_REDACT_KEYS.
        max_bytes: Maximum serialized output size in bytes. If None, uses env TRACE_MAX_BYTES (default 10000).

    Returns:
        JSON string (redacted and truncated if needed), or empty string on serialization error.
    """
    # Apply configuration defaults
    if redact_keys is None:
        redact_keys_str = os.environ.get("TRACE_REDACT_KEYS", "password,secret,token,key,apikey,api_key")
        redact_keys = [k.strip() for k in redact_keys_str.split(",") if k.strip()]
    if max_bytes is None:
        max_bytes = int(os.environ.get("TRACE_MAX_BYTES", "10000"))

    # Normalize to case-insensitive set
    redact_set = {k.lower() for k in redact_keys}

    try:
        # Redact sensitive keys
        redacted = _redact_sensitive_keys(message, redact_set)
        # Serialize with minimal whitespace
        serialized = json.dumps(redacted, separators=(",", ":"))
        # Truncate if needed
        if len(serialized.encode("utf-8")) > max_bytes:
            # Binary truncate to max_bytes and append marker
            truncated_bytes = serialized.encode("utf-8")[:max_bytes]
            # Ensure we don't break mid-multibyte character
            try:
                return truncated_bytes.decode("utf-8", errors="ignore") + "..."
            except UnicodeDecodeError:  # pragma: no cover - defensive
                return ""
        return serialized
    except (TypeError, ValueError, OverflowError):  # pragma: no cover - catch serialization errors
        return ""


__all__ = ["safe_serialize_for_log"]
