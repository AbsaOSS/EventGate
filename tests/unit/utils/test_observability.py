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

import json
import logging

import pytest

from src.utils import observability
from src.utils.logging_levels import TRACE_LEVEL
from src.utils.observability import (
    append_request_context,
    bind_request_context,
    configure_root_logging,
    logger,
    resolve_correlation_id,
)


class FakeLambdaContext:
    function_name = "eventgate"
    aws_request_id = "aws-request-id-1"
    memory_limit_in_mb = 512
    invoked_function_arn = "arn:aws:lambda:eu-west-1:000000000000:function:eventgate"


def formatted(record):
    """Render a captured record the way the lambda emits it, so persistent keys are visible."""
    return json.loads(logger.registered_formatter.format(record))


@pytest.fixture(autouse=True)
def clean_logger_state():
    """Keep the module level logger state from leaking between tests."""
    observability._container_state["cold_start"] = True
    yield
    bind_request_context({})
    logger.set_correlation_id(None)


## resolve_correlation_id()
def test_correlation_id_from_correlation_header():
    event = {"headers": {"X-Correlation-ID": "run-42_a.b:c"}}

    assert "run-42_a.b:c" == resolve_correlation_id(event)


def test_correlation_id_header_lookup_is_case_insensitive():
    event = {"headers": {"x-CORRELATION-id": " run-42 "}}

    assert "run-42" == resolve_correlation_id(event)


def test_correlation_id_from_request_id_header():
    event = {"headers": {"X-Request-ID": "req-7"}}

    assert "req-7" == resolve_correlation_id(event)


@pytest.mark.parametrize(
    "value",
    ["with space", "line\nbreak", "", "!@#$", "x" * 129],
)
def test_correlation_id_rejects_malformed_header(value):
    event = {"headers": {"X-Correlation-ID": value}, "requestContext": {"requestId": "apigw-1"}}

    assert "apigw-1" == resolve_correlation_id(event)


def test_correlation_id_falls_back_to_api_gateway_request_id():
    event = {"requestContext": {"requestId": "apigw-1"}}

    assert "apigw-1" == resolve_correlation_id(event)


def test_correlation_id_empty_when_unavailable():
    assert "" == resolve_correlation_id({})


## bind_request_context()
def test_bind_request_context_binds_correlation_id_and_request_keys(caplog):
    caplog.set_level(logging.INFO)
    event = {"headers": {"X-Correlation-ID": "run-42"}, "resource": "/topics/{topic_name}", "httpMethod": "POST"}

    correlation_id = bind_request_context(event, FakeLambdaContext())
    logging.getLogger("src.handlers.handler_topic").info("Message accepted.")

    payload = formatted(caplog.records[-1])
    assert "run-42" == correlation_id
    assert "run-42" == payload["correlation_id"]
    assert "/topics/{topic_name}" == payload["resource"]
    assert "POST" == payload["http_method"]
    assert "aws-request-id-1" == payload["function_request_id"]
    assert payload["cold_start"] is True


def test_bind_request_context_reports_cold_start_once(caplog):
    caplog.set_level(logging.INFO)

    bind_request_context({}, FakeLambdaContext())
    logging.getLogger("src.utils.utils").info("Request completed.")
    first = formatted(caplog.records[-1])

    bind_request_context({}, FakeLambdaContext())
    logging.getLogger("src.utils.utils").info("Request completed.")
    second = formatted(caplog.records[-1])

    assert first["cold_start"] is True
    assert second["cold_start"] is False


def test_lambda_execution_context_is_logged_once_and_not_bound_per_line(caplog):
    caplog.set_level(logging.INFO)

    bind_request_context({}, FakeLambdaContext())
    logging.getLogger("src.utils.utils").info("Request completed.")

    context_lines = [r for r in caplog.records if r.message == "Lambda execution context."]
    assert 1 == len(context_lines)
    assert "eventgate" == context_lines[0].function_name
    assert 512 == context_lines[0].function_memory_size
    assert FakeLambdaContext.invoked_function_arn == context_lines[0].function_arn

    payload = formatted(caplog.records[-1])
    assert "aws-request-id-1" == payload["function_request_id"]
    assert "function_name" not in payload
    assert "function_arn" not in payload
    assert "function_memory_size" not in payload

    bind_request_context({}, FakeLambdaContext())
    assert 1 == len([r for r in caplog.records if r.message == "Lambda execution context."])


def test_bind_request_context_tolerates_missing_lambda_context(caplog):
    caplog.set_level(logging.INFO)

    bind_request_context({"resource": "/health"}, None)
    logging.getLogger("src.utils.utils").info("Request completed.")

    payload = formatted(caplog.records[-1])
    assert "/health" == payload["resource"]
    assert "function_request_id" not in payload


def test_request_log_keys_do_not_leak_into_the_next_invocation(caplog):
    caplog.set_level(logging.INFO)

    bind_request_context({"resource": "/topics/{topic_name}"}, None)
    append_request_context(topic="test", user="someone")
    logging.getLogger("src.handlers.handler_topic").info("Message accepted.")
    assert "test" == formatted(caplog.records[-1])["topic"]

    bind_request_context({"resource": "/health"}, None)
    logging.getLogger("src.handlers.handler_health").info("Health check passed.")

    payload = formatted(caplog.records[-1])
    assert "topic" not in payload
    assert "user" not in payload


## configure_root_logging()
def test_configure_root_logging_routes_root_through_powertools_handler():
    configure_root_logging()

    root_logger = logging.getLogger()
    assert [logger.registered_handler] == root_logger.handlers


def test_configure_root_logging_caps_noisy_loggers(monkeypatch):
    monkeypatch.setenv("LOG_LEVEL", "TRACE")
    monkeypatch.delenv("POWERTOOLS_LOG_LEVEL", raising=False)

    configure_root_logging()

    assert TRACE_LEVEL == logging.getLogger().level
    for noisy_logger in observability.NOISY_LOGGERS:
        assert logging.WARNING == logging.getLogger(noisy_logger).level
