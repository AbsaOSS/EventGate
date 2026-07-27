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
import time
from collections.abc import Callable
from typing import Any

import boto3

from src.utils.observability import CORRELATION_ID_RESPONSE_HEADER, bind_request_context

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


def with_correlation_header(response: dict[str, Any], correlation_id: str) -> dict[str, Any]:
    """Add the correlation id header to a response so callers can quote it when reporting issues.
    Args:
        response: API Gateway response dictionary.
        correlation_id: Correlation id of the current request. Ignored when empty.
    Returns:
        The response with the correlation header set.
    """
    if not correlation_id or not isinstance(response, dict):
        return response

    headers = dict(response.get("headers") or {})
    headers.setdefault(CORRELATION_ID_RESPONSE_HEADER, correlation_id)
    response["headers"] = headers
    return response


def dispatch_request(
    event: dict[str, Any],
    route_map: dict[str, Callable[..., dict[str, Any]]],
    request_logger: logging.Logger,
    context: Any = None,
) -> dict[str, Any]:
    """Dispatch an API Gateway event to the matching route handler.
    Binds the request context to the logger, logs the request outcome and stamps the response
    with the correlation id.
    Args:
        event: API Gateway proxy event.
        route_map: Mapping of resource paths to handler callables.
        request_logger: Logger instance for error reporting.
        context: AWS Lambda context object, when available.
    Returns:
        API Gateway response dictionary.
    """
    started_at = time.perf_counter()
    correlation_id = bind_request_context(event, context)
    resource = str(event.get("resource", "")).lower()

    try:
        route_function = route_map.get(resource)
        if route_function is None:
            request_logger.warning("No route matched the requested resource.")
            response = build_error_response(404, "route", "Resource not found.")
        else:
            request_logger.debug("Dispatching request to the route handler.")
            response = route_function(event)
    except Exception:  # pylint: disable=broad-exception-caught
        # Boundary handler: any escaping exception must become a stable 500 with a logged cause,
        # otherwise API Gateway returns an opaque 502 and the traceback is unstructured.
        request_logger.exception("Unhandled error while processing the request.")
        response = build_error_response(500, "internal", "Unexpected server error.")

    duration_ms = round((time.perf_counter() - started_at) * 1000, 2)
    status_code = response.get("statusCode") if isinstance(response, dict) else None
    request_logger.info("Request completed.", extra={"status_code": status_code, "duration_ms": duration_ms})

    return with_correlation_header(response, correlation_id)


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
