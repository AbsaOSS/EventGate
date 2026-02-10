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

"""JWT token generation utilities for integration tests."""

import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

logger = logging.getLogger(__name__)


def create_test_jwt_keypair() -> Dict[str, Any]:
    """
    Generate RSA keypair for JWT signing.

    Returns:
        Dictionary with private_key_pem (bytes) and public_key_b64 (str).
    """
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public_key = private_key.public_key()

    private_key_pem = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    public_key_der = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    public_key_b64 = base64.b64encode(public_key_der).decode("utf-8")

    logger.debug("Generated RSA keypair for JWT signing.")

    return {
        "private_key_pem": private_key_pem,
        "public_key_b64": public_key_b64,
    }


def generate_token(
    private_key_pem: bytes,
    username: str,
    exp_minutes: int = 30,
) -> str:
    """
    Generate a signed JWT token.

    Args:
        private_key_pem: PEM-encoded private key bytes.
        username: Username to include in 'sub' claim.
        exp_minutes: Token expiration time in minutes.

    Returns:
        Signed JWT token string.
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub": username,
        "iat": now,
        "exp": now + timedelta(minutes=exp_minutes),
    }

    token = jwt.encode(payload, private_key_pem, algorithm="RS256")
    logger.debug("Generated JWT token for user %s.", username)

    return token
