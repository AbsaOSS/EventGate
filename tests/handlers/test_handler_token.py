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

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, Mock

import pytest
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from src.handlers.handler_token import HandlerToken


@pytest.fixture
def token_handler():
    """Create a HandlerToken instance for testing."""
    config = {"token_public_keys_url": "https://example.com/keys"}
    return HandlerToken(config)


def test_get_token_endpoint(event_gate_module, make_event):
    event = make_event("/token")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 303
    assert "Location" in resp["headers"]


def test_post_expired_token(event_gate_module, make_event, valid_payload):
    """Expired JWT should yield 401 auth error."""

    with patch.object(
        event_gate_module.jwt,
        "decode",
        side_effect=event_gate_module.jwt.ExpiredSignatureError("expired"),
        create=True,
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer expiredtoken"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 401
        body = json.loads(resp["body"])
        assert any(e["type"] == "auth" for e in body["errors"])


def test_decode_jwt_all_second_key_succeeds(event_gate_module):
    """First key fails signature, second key succeeds; claims returned from second key."""
    # Arrange: two dummy public keys
    first_key = object()
    second_key = object()
    event_gate_module.handler_token.public_keys = [first_key, second_key]

    def decode_side_effect(token, key, algorithms):
        if key is first_key:
            raise event_gate_module.jwt.PyJWTError("signature mismatch")
        return {"sub": "TestUser"}

    with patch.object(event_gate_module.jwt, "decode", side_effect=decode_side_effect, create=True):
        claims = event_gate_module.handler_token.decode_jwt("dummy-token")
        assert claims["sub"] == "TestUser"


def test_decode_jwt_all_all_keys_fail(event_gate_module):
    """All keys fail; final PyJWTError with aggregate message is raised."""
    bad_keys = [object(), object()]
    event_gate_module.handler_token.public_keys = bad_keys

    def always_fail(token, key, algorithms):
        raise event_gate_module.jwt.PyJWTError("bad signature")

    with patch.object(event_gate_module.jwt, "decode", side_effect=always_fail, create=True):
        with pytest.raises(event_gate_module.jwt.PyJWTError) as exc:
            event_gate_module.handler_token.decode_jwt("dummy-token")
        assert "Verification failed for all public keys" in str(exc.value)


def test_extract_token_empty():
    assert HandlerToken.extract_token({}) == ""


def test_extract_token_direct_bearer_header():
    token = HandlerToken.extract_token({"Bearer": "  tok123  "})
    assert token == "tok123"


## Checking the freshness of public keys
def test_refresh_keys_not_needed_when_keys_fresh(token_handler):
    """Keys loaded less than 28 minutes ago should not trigger refresh."""
    token_handler._last_loaded_at = datetime.now(timezone.utc) - timedelta(minutes=10)
    token_handler.public_keys = [Mock(spec=RSAPublicKey)]

    with patch.object(token_handler, "load_public_keys") as mock_load:
        token_handler._refresh_keys_if_needed()
        mock_load.assert_not_called()


def test_refresh_keys_triggered_when_keys_stale(token_handler):
    """Keys loaded more than 28 minutes ago should trigger refresh."""
    token_handler._last_loaded_at = datetime.now(timezone.utc) - timedelta(minutes=29)
    token_handler.public_keys = [Mock(spec=RSAPublicKey)]

    with patch.object(token_handler, "load_public_keys") as mock_load:
        token_handler._refresh_keys_if_needed()
        mock_load.assert_called_once()


def test_refresh_keys_handles_load_failure_gracefully(token_handler):
    """If key refresh fails, should log warning and continue with existing keys."""
    old_key = Mock(spec=RSAPublicKey)
    token_handler.public_keys = [old_key]
    token_handler._last_loaded_at = datetime.now(timezone.utc) - timedelta(minutes=29)

    with patch.object(token_handler, "load_public_keys", side_effect=RuntimeError("Network error")):
        token_handler._refresh_keys_if_needed()
        assert token_handler.public_keys == [old_key]


def test_decode_jwt_triggers_refresh_check(token_handler):
    """Decoding JWT should check if keys need refresh before decoding."""
    dummy_key = Mock(spec=RSAPublicKey)
    token_handler.public_keys = [dummy_key]
    token_handler._last_loaded_at = datetime.now(timezone.utc) - timedelta(minutes=10)

    with patch.object(token_handler, "_refresh_keys_if_needed") as mock_refresh:
        with patch("jwt.decode", return_value={"sub": "TestUser"}):
            token_handler.decode_jwt("dummy-token")
            mock_refresh.assert_called_once()


def test_handler_token_default_ssl_ca_bundle():
    """HandlerToken should default to True for ssl_ca_bundle when not specified."""
    config = {"token_public_keys_url": "https://example.com/keys"}
    handler = HandlerToken(config)
    assert handler.ssl_ca_bundle is True


def test_handler_token_custom_ssl_ca_bundle_path():
    """HandlerToken should accept custom CA bundle path."""
    config = {"token_public_keys_url": "https://example.com/keys", "ssl_ca_bundle": "/path/to/custom/ca-bundle.pem"}
    handler = HandlerToken(config)
    assert handler.ssl_ca_bundle == "/path/to/custom/ca-bundle.pem"
