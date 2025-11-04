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

"""Unit tests for safe_serialization module."""

import json
import os
from unittest.mock import patch

import pytest

from src.safe_serialization import safe_serialize_for_log


class TestSafeSerializeForLog:
    """Test suite for safe_serialize_for_log function."""

    def test_simple_serialization(self):
        """Test basic serialization without redaction or truncation."""
        message = {"event_id": "123", "tenant_id": "abc"}
        result = safe_serialize_for_log(message, redact_keys=[], max_bytes=10000)
        assert result == '{"event_id":"123","tenant_id":"abc"}'

    def test_redact_single_key(self):
        """Test redaction of a single sensitive key."""
        message = {"event_id": "123", "password": "secret123"}
        result = safe_serialize_for_log(message, redact_keys=["password"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["event_id"] == "123"
        assert parsed["password"] == "***REDACTED***"

    def test_redact_multiple_keys(self):
        """Test redaction of multiple sensitive keys."""
        message = {"username": "user", "password": "pass123", "api_key": "key456", "data": "visible"}
        result = safe_serialize_for_log(message, redact_keys=["password", "api_key"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["username"] == "user"
        assert parsed["password"] == "***REDACTED***"
        assert parsed["api_key"] == "***REDACTED***"
        assert parsed["data"] == "visible"

    def test_redact_case_insensitive(self):
        """Test that redaction is case-insensitive."""
        message = {"Password": "secret", "API_KEY": "key", "Token": "tok"}
        result = safe_serialize_for_log(message, redact_keys=["password", "api_key", "token"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["Password"] == "***REDACTED***"
        assert parsed["API_KEY"] == "***REDACTED***"
        assert parsed["Token"] == "***REDACTED***"

    def test_redact_nested_dict(self):
        """Test redaction in nested dictionaries."""
        message = {
            "event_id": "123",
            "credentials": {"username": "user", "password": "secret"},
            "config": {"api_key": "key123", "timeout": 30},
        }
        result = safe_serialize_for_log(message, redact_keys=["password", "api_key"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["event_id"] == "123"
        assert parsed["credentials"]["username"] == "user"
        assert parsed["credentials"]["password"] == "***REDACTED***"
        assert parsed["config"]["api_key"] == "***REDACTED***"
        assert parsed["config"]["timeout"] == 30

    def test_redact_in_list(self):
        """Test redaction in lists of objects."""
        message = {
            "users": [
                {"name": "alice", "password": "pass1"},
                {"name": "bob", "secret": "sec2"},
            ]
        }
        result = safe_serialize_for_log(message, redact_keys=["password", "secret"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["users"][0]["name"] == "alice"
        assert parsed["users"][0]["password"] == "***REDACTED***"
        assert parsed["users"][1]["name"] == "bob"
        assert parsed["users"][1]["secret"] == "***REDACTED***"

    def test_truncation_at_max_bytes(self):
        """Test that output is truncated when exceeding max_bytes."""
        message = {"data": "x" * 10000}
        result = safe_serialize_for_log(message, redact_keys=[], max_bytes=100)
        # Result should be truncated and end with "..."
        assert result.endswith("...")
        assert len(result.encode("utf-8")) <= 103  # 100 bytes + "..."

    def test_truncation_preserves_validity(self):
        """Test that truncation doesn't break in middle of multibyte characters."""
        # Create a message with multibyte unicode characters
        message = {"emoji": "🔥" * 1000}
        result = safe_serialize_for_log(message, redact_keys=[], max_bytes=50)
        assert result.endswith("...")
        # Should not raise decoding errors
        assert len(result) > 0

    def test_env_default_redact_keys(self):
        """Test that default redact keys are loaded from environment."""
        with patch.dict(os.environ, {"TRACE_REDACT_KEYS": "password,secret,token"}):
            message = {"password": "pass123", "secret": "sec456", "data": "visible"}
            result = safe_serialize_for_log(message)
            parsed = json.loads(result)
            assert parsed["password"] == "***REDACTED***"
            assert parsed["secret"] == "***REDACTED***"
            assert parsed["data"] == "visible"

    def test_env_default_max_bytes(self):
        """Test that default max_bytes is loaded from environment."""
        with patch.dict(os.environ, {"TRACE_MAX_BYTES": "50"}):
            message = {"data": "x" * 1000}
            result = safe_serialize_for_log(message)
            assert result.endswith("...")
            assert len(result.encode("utf-8")) <= 53  # 50 + "..."

    def test_env_defaults_when_not_set(self):
        """Test default behavior when env vars are not set."""
        # Clear env vars if present
        with patch.dict(os.environ, {}, clear=False):
            if "TRACE_REDACT_KEYS" in os.environ:
                del os.environ["TRACE_REDACT_KEYS"]
            if "TRACE_MAX_BYTES" in os.environ:
                del os.environ["TRACE_MAX_BYTES"]
            message = {"password": "secret", "data": "x" * 20000}
            result = safe_serialize_for_log(message)
            # Default should redact 'password' (in default list)
            parsed_or_truncated = result
            if result.endswith("..."):
                # Truncated at default 10000 bytes
                assert len(result.encode("utf-8")) <= 10003
            else:
                parsed = json.loads(result)
                assert parsed["password"] == "***REDACTED***"

    def test_unserializable_object_returns_empty(self):
        """Test that unserializable objects return empty string."""

        class Unserializable:
            def __repr__(self):
                raise RuntimeError("Cannot serialize")

        # Mock json.dumps to raise an exception
        message = {"obj": "normal"}
        with patch("src.safe_serialization.json.dumps", side_effect=TypeError("Cannot serialize")):
            result = safe_serialize_for_log(message, redact_keys=[], max_bytes=10000)
            assert result == ""

    def test_empty_message(self):
        """Test serialization of empty message."""
        result = safe_serialize_for_log({}, redact_keys=[], max_bytes=10000)
        assert result == "{}"

    def test_none_redact_keys(self):
        """Test that None redact_keys uses environment defaults."""
        with patch.dict(os.environ, {"TRACE_REDACT_KEYS": "password"}):
            message = {"password": "secret"}
            result = safe_serialize_for_log(message, redact_keys=None, max_bytes=10000)
            parsed = json.loads(result)
            assert parsed["password"] == "***REDACTED***"

    def test_none_max_bytes(self):
        """Test that None max_bytes uses environment defaults."""
        with patch.dict(os.environ, {"TRACE_MAX_BYTES": "100"}):
            message = {"data": "x" * 1000}
            result = safe_serialize_for_log(message, redact_keys=[], max_bytes=None)
            assert result.endswith("...")

    def test_empty_redact_keys_list(self):
        """Test that empty redact_keys list performs no redaction."""
        message = {"password": "secret", "api_key": "key123"}
        result = safe_serialize_for_log(message, redact_keys=[], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["password"] == "secret"
        assert parsed["api_key"] == "key123"

    def test_redact_with_whitespace_in_env(self):
        """Test that env var with whitespace is handled correctly."""
        with patch.dict(os.environ, {"TRACE_REDACT_KEYS": " password , api_key , token "}):
            message = {"password": "secret", "api_key": "key", "token": "tok"}
            result = safe_serialize_for_log(message, redact_keys=None, max_bytes=10000)
            parsed = json.loads(result)
            assert parsed["password"] == "***REDACTED***"
            assert parsed["api_key"] == "***REDACTED***"
            assert parsed["token"] == "***REDACTED***"

    def test_complex_nested_structure(self):
        """Test redaction in complex nested structures."""
        message = {
            "level1": {
                "level2": {
                    "level3": {
                        "password": "deep_secret",
                        "data": "visible",
                    }
                },
                "items": [
                    {"id": 1, "secret": "s1"},
                    {"id": 2, "secret": "s2"},
                ],
            }
        }
        result = safe_serialize_for_log(message, redact_keys=["password", "secret"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["level1"]["level2"]["level3"]["password"] == "***REDACTED***"
        assert parsed["level1"]["level2"]["level3"]["data"] == "visible"
        assert parsed["level1"]["items"][0]["secret"] == "***REDACTED***"
        assert parsed["level1"]["items"][1]["secret"] == "***REDACTED***"
        assert parsed["level1"]["items"][0]["id"] == 1

    def test_non_dict_message(self):
        """Test serialization of non-dict messages (e.g., list, string)."""
        # List
        result = safe_serialize_for_log([1, 2, 3], redact_keys=[], max_bytes=10000)
        assert result == "[1,2,3]"

        # String
        result = safe_serialize_for_log("hello", redact_keys=[], max_bytes=10000)
        assert result == '"hello"'

        # Number
        result = safe_serialize_for_log(42, redact_keys=[], max_bytes=10000)
        assert result == "42"

    def test_redaction_with_non_string_values(self):
        """Test that redaction works when sensitive keys have non-string values."""
        message = {"password": 12345, "api_key": None, "secret": True}
        result = safe_serialize_for_log(message, redact_keys=["password", "api_key", "secret"], max_bytes=10000)
        parsed = json.loads(result)
        assert parsed["password"] == "***REDACTED***"
        assert parsed["api_key"] == "***REDACTED***"
        assert parsed["secret"] == "***REDACTED***"
