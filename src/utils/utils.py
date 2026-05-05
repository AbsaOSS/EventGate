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

"""Utility functions for the EventGate project."""

import json
import logging
from collections.abc import Callable
from typing import Any

import boto3

logger = logging.getLogger(__name__)


def build_error_response(status: int, err_type: str, message: str) -> dict[str, Any]:
    """Build a standardized JSON error response body.
    Args:
        status: HTTP status code.
        err_type: A short error classifier (e.g. `auth`, `validation`).
        message: Human readable error description.
    Returns:
        A dictionary compatible with API Gateway Lambda Proxy integration.
    """
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(
            {
                "success": False,
                "statusCode": status,
                "errors": [{"type": err_type, "message": message}],
            }
        ),
    }


def dispatch_request(
    event: dict[str, Any],
    route_map: dict[str, Callable[..., dict[str, Any]]],
    request_logger: logging.Logger,
) -> dict[str, Any]:
    """Dispatch an API Gateway event to the matching route handler.
    Args:
        event: API Gateway proxy event.
        route_map: Mapping of resource paths to handler callables.
        request_logger: Logger instance for error reporting.
    Returns:
        API Gateway response dictionary.
    """
    try:
        resource = event.get("resource", "").lower()
        route_function = route_map.get(resource)

        if route_function:
            return route_function(event)

        return build_error_response(404, "route", "Resource not found.")
    except (
        KeyError,
        json.JSONDecodeError,
        ValueError,
        AttributeError,
        TypeError,
        RuntimeError,
    ) as request_exc:
        request_logger.exception("Request processing error: %s.", request_exc)
        return build_error_response(500, "internal", "Unexpected server error.")


def load_postgres_config(secret_name: str, secret_region: str) -> dict[str, Any]:
    """Load PostgreSQL connection config from AWS Secrets Manager.
    Args:
        secret_name: Name or ARN of the secret.
        secret_region: AWS region where the secret is stored.
    Returns:
        Parsed connection dict with keys like database, host, user, password, port.
        Returns {"database": ""} when secret_name or secret_region is empty.
    """
    if not secret_name or not secret_region:
        return {"database": ""}

    aws_secrets = boto3.Session().client(service_name="secretsmanager", region_name=secret_region)
    postgres_secret = aws_secrets.get_secret_value(SecretId=secret_name)["SecretString"]
    aws_pg_secret: dict[str, Any] = json.loads(postgres_secret)
    logger.debug("Loaded PostgreSQL config from Secrets Manager.")
    return aws_pg_secret
