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
This module provides the HandlerToken class for managing the token related operations.
"""

import base64
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, cast

import jwt
import requests
from cryptography.exceptions import UnsupportedAlgorithm
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey

from src.utils.constants import (
    TOKEN_PROVIDER_URL_KEY,
    TOKEN_PUBLIC_KEYS_URL_KEY,
    TOKEN_PUBLIC_KEY_URL_KEY,
    SSL_CA_BUNDLE_KEY,
)

logger = logging.getLogger(__name__)
log_level = os.environ.get("LOG_LEVEL", "INFO")
logger.setLevel(log_level)


class HandlerToken:
    """
    HandlerToken manages token provider URL and public keys for JWT verification.
    """

    _REFRESH_INTERVAL = timedelta(minutes=28)

    def __init__(self, config):
        self.provider_url: str = config.get(TOKEN_PROVIDER_URL_KEY, "")
        self.public_keys_url: str = config.get(TOKEN_PUBLIC_KEYS_URL_KEY) or config.get(TOKEN_PUBLIC_KEY_URL_KEY)
        self.public_keys: list[RSAPublicKey] = []
        self._last_loaded_at: datetime | None = None
        self.ssl_ca_bundle: str | bool = config.get(SSL_CA_BUNDLE_KEY, True)

    def _refresh_keys_if_needed(self) -> None:
        """
        Refresh the public keys if the refresh interval has passed.
        """
        logger.debug("Checking if the token public keys need refresh")

        if self._last_loaded_at is None:
            return
        now = datetime.now(timezone.utc)
        if now - self._last_loaded_at < self._REFRESH_INTERVAL:
            logger.debug("Token public keys are up to date, no refresh needed")
            return
        try:
            logger.debug("Token public keys are stale, refreshing now")
            self.load_public_keys()
        except RuntimeError:
            logger.warning("Token public key refresh failed, using existing keys")

    def load_public_keys(self) -> "HandlerToken":
        """
        Load token public keys from the configured URL.
        Returns:
            HandlerToken: The current instance with loaded public keys.
        Raises:
            RuntimeError: If fetching or deserializing the public keys fails.
        """
        logger.debug("Loading token public keys from %s", self.public_keys_url)

        try:
            response_json = requests.get(self.public_keys_url, verify=self.ssl_ca_bundle, timeout=5).json()
            raw_keys: list[str] = []

            if isinstance(response_json, dict):
                if "keys" in response_json and isinstance(response_json["keys"], list):
                    for item in response_json["keys"]:
                        if "key" in item:
                            raw_keys.append(item["key"].strip())
                elif "key" in response_json:
                    raw_keys.append(response_json["key"].strip())

            if not raw_keys:
                raise KeyError(f"No public keys found in {self.public_keys_url} endpoint response")

            self.public_keys = [
                cast(RSAPublicKey, serialization.load_der_public_key(base64.b64decode(raw_key))) for raw_key in raw_keys
            ]
            logger.debug("Loaded %d token public keys", len(self.public_keys))
            self._last_loaded_at = datetime.now(timezone.utc)

            return self
        except (requests.RequestException, ValueError, KeyError, UnsupportedAlgorithm) as exc:
            logger.exception("Failed to fetch or deserialize token public key from %s", self.public_keys_url)
            raise RuntimeError("Token public key initialization failed") from exc

    def decode_jwt(self, token_encoded: str) -> Dict[str, Any]:
        """
        Decode and verify a JWT using the loaded public keys.
        Args:
            token_encoded (str): The encoded JWT token.
        Returns:
            Dict[str, Any]: The decoded JWT payload.
        Raises:
            jwt.PyJWTError: If verification fails for all public keys.
        """
        self._refresh_keys_if_needed()

        logger.debug("Decoding JWT")
        for public_key in self.public_keys:
            try:
                return jwt.decode(token_encoded, public_key, algorithms=["RS256"])
            except jwt.PyJWTError:
                continue
        raise jwt.PyJWTError("Verification failed for all public keys")

    def get_token_provider_info(self) -> Dict[str, Any]:
        """
        Returns: A 303 redirect response to the token provider URL.
        """
        logger.debug("Handling GET Token")
        return {"statusCode": 303, "headers": {"Location": self.provider_url}}

    @staticmethod
    def extract_token(event_headers: Dict[str, str]) -> str:
        """
        Extracts the bearer (custom/standard) token from event headers.
        Args:
            event_headers (Dict[str, str]): The event headers.
        Returns:
            str: The extracted bearer token, or an empty string if not found.
        """
        if not event_headers:
            return ""

        # Normalize keys to lowercase for case-insensitive lookup
        lowered = {str(k).lower(): v for k, v in event_headers.items()}

        # Direct bearer header (raw token)
        if "bearer" in lowered and isinstance(lowered["bearer"], str):
            token_candidate = lowered["bearer"].strip()
            if token_candidate:
                return token_candidate

        # Authorization header with Bearer scheme
        auth_val = lowered.get("authorization", "")
        if not isinstance(auth_val, str):  # defensive
            return ""
        auth_val = auth_val.strip()
        if not auth_val:
            return ""

        # Case-insensitive match for 'Bearer ' prefix
        if not auth_val.lower().startswith("bearer "):
            return ""
        token_part = auth_val[7:].strip()  # len('Bearer ')==7
        return token_part
