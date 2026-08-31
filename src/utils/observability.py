#
# Copyright 2026 ABSA Group Limited
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

"""Structured logging and request correlation for the EventGate lambdas.

Wraps AWS Lambda Powertools `Logger` so that every log line is emitted as JSON
enriched with the Lambda execution context and a request correlation id.
Module loggers keep using `logging.getLogger(__name__)`; the Powertools handler
is attached to the root logger, so their records are formatted and enriched too.
"""

import logging
import os
import re
from typing import Any

from aws_lambda_powertools import Logger

from src.utils.logging_levels import TRACE_LEVEL, configured_log_level, resolve_log_level

DEFAULT_SERVICE_NAME = "eventgate"

# Attribute carrying the invocation id on the AWS Lambda context object.
AWS_REQUEST_ID = "aws_request_id"

# Header accepted from callers so that an upstream request id can be carried through EventGate.
CORRELATION_ID_HEADER = "x-correlation-id"
REQUEST_ID_HEADER = "x-request-id"
# Header returned to the caller on every response, so a client can quote a single id in a ticket.
CORRELATION_ID_RESPONSE_HEADER = "X-Correlation-ID"

# Inbound correlation ids end up in the log stream, so they must not carry newlines or control
# characters (log injection) and must stay small enough not to bloat every log line.
CORRELATION_ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")

# Third-party loggers that flood the log group when the service runs at DEBUG or TRACE.
NOISY_LOGGERS = ("boto3", "botocore", "urllib3", "s3transfer", "aiosql", "confluent_kafka")

_INITIAL_LOG_LEVEL, _ = resolve_log_level()

logger: Logger = Logger(
    service=os.environ.get("POWERTOOLS_SERVICE_NAME", DEFAULT_SERVICE_NAME),
    level=_INITIAL_LOG_LEVEL,
    use_rfc3339=True,
    utc=True,
    log_uncaught_exceptions=True,
)

# Names of the log keys bound for the duration of a single invocation; cleared at the start of
# the next one so a warm container does not leak context between requests.
_request_context_key_names: set[str] = set()

# Mutable container so the flag can be flipped without a module level `global` statement.
_container_state: dict[str, bool] = {"cold_start": True}


def configure_root_logging() -> None:
    """Route every logger in the process through the Powertools JSON handler.

    Replaces handlers installed by the Lambda runtime so that records are not emitted twice,
    and caps noisy third-party loggers at WARNING so `LOG_LEVEL=DEBUG` stays readable.
    """
    log_level, _ = resolve_log_level()

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.addHandler(logger.registered_handler)
    root_logger.setLevel(log_level)

    for noisy_logger in NOISY_LOGGERS:
        logging.getLogger(noisy_logger).setLevel(max(log_level, logging.WARNING))


def init_lambda_logging(module_name: str) -> logging.Logger:
    """Configure logging for a lambda entry point.
    Args:
        module_name: Typically `__name__` of the calling entry point module.
    Returns:
        The module logger to use for the rest of the module.
    """
    configure_root_logging()
    module_logger = logging.getLogger(module_name)

    if rejected_level := resolve_log_level()[1]:
        module_logger.warning(
            "Invalid log level configured; defaulting to INFO.",
            extra={"log_level": rejected_level},
        )

    return module_logger


def resolve_correlation_id(event: dict[str, Any]) -> str:
    """Resolve the correlation id for an invocation.
    Args:
        event: API Gateway proxy event.
    Returns:
        The caller supplied correlation id when present and well-formed, otherwise the
        API Gateway request id, otherwise an empty string.
    """
    headers = event.get("headers") or {}
    lowered_headers = {str(key).lower(): value for key, value in headers.items()}

    for header in (CORRELATION_ID_HEADER, REQUEST_ID_HEADER):
        header_value = lowered_headers.get(header)
        if isinstance(header_value, str) and CORRELATION_ID_PATTERN.fullmatch(header_value.strip()):
            return header_value.strip()

    request_context = event.get("requestContext") or {}
    api_request_id = request_context.get("requestId")
    return str(api_request_id) if api_request_id else ""


def append_request_context(**context_keys: Any) -> None:
    """Append structured log context for the current invocation only.
    Args:
        **context_keys: Key/value pairs added to every subsequent log line of this invocation.
    """
    if not context_keys:
        return
    _request_context_key_names.update(context_keys)
    logger.append_keys(**context_keys)


def _clear_request_context() -> None:
    """Drop the context appended during the previous invocation of a warm container."""
    if _request_context_key_names:
        logger.remove_keys(list(_request_context_key_names))
        _request_context_key_names.clear()


def bind_request_context(event: dict[str, Any], context: Any = None) -> str:
    """Bind Lambda and request context to the logger for the current invocation.
    Args:
        event: API Gateway proxy event.
        context: AWS Lambda context object. May be `None` when invoked locally or from tests.
    Returns:
        The resolved correlation id (possibly an empty string).
    """
    _clear_request_context()

    correlation_id = resolve_correlation_id(event)
    logger.set_correlation_id(correlation_id or None)

    cold_start = _container_state["cold_start"]
    _container_state["cold_start"] = False

    append_request_context(
        cold_start=cold_start,
        resource=event.get("resource", ""),
        http_method=event.get("httpMethod", ""),
    )

    if context is not None:
        append_request_context(function_request_id=getattr(context, AWS_REQUEST_ID, None))
        if cold_start:
            # Static per-deployment facts are logged once per container instead of being bound
            # to every line; the log stream already identifies the function, so repeating them
            # would only inflate every record.
            logger.info(
                "Lambda execution context.",
                extra={
                    "function_name": getattr(context, "function_name", None),
                    "function_memory_size": getattr(context, "memory_limit_in_mb", None),
                    "function_arn": getattr(context, "invoked_function_arn", None),
                },
            )

    return correlation_id


__all__ = [
    "AWS_REQUEST_ID",
    "CORRELATION_ID_HEADER",
    "CORRELATION_ID_PATTERN",
    "CORRELATION_ID_RESPONSE_HEADER",
    "DEFAULT_SERVICE_NAME",
    "NOISY_LOGGERS",
    "REQUEST_ID_HEADER",
    "TRACE_LEVEL",
    "append_request_context",
    "bind_request_context",
    "configure_root_logging",
    "configured_log_level",
    "init_lambda_logging",
    "logger",
    "resolve_correlation_id",
]
