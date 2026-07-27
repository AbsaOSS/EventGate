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
import logging

import pytest

from src.utils.observability import CORRELATION_ID_RESPONSE_HEADER
from src.utils.utils import build_error_response, dispatch_request

logger = logging.getLogger(__name__)


def make_event(resource="/health", method="GET", correlation_id="run-42"):
    """Build a minimal API Gateway proxy event."""
    return {
        "resource": resource,
        "httpMethod": method,
        "headers": {"X-Correlation-ID": correlation_id},
    }


## build_error_response()
def test_build_error_response_structure():
    """Test that build_error_response returns correct response structure."""
    resp = build_error_response(404, "topic", "Topic not found")

    assert 404 == resp["statusCode"]
    assert "application/json" == resp["headers"]["Content-Type"]

    body = json.loads(resp["body"])
    assert body["success"] is False
    assert 404 == body["statusCode"]
    assert 1 == len(body["errors"])
    assert "topic" == body["errors"][0]["type"]
    assert "Topic not found" == body["errors"][0]["message"]


## dispatch_request()
def test_dispatch_request_routes_and_stamps_the_correlation_id():
    route_map = {"/health": lambda _: {"statusCode": 200, "headers": {"Content-Type": "application/json"}}}

    resp = dispatch_request(make_event(), route_map, logger)

    assert 200 == resp["statusCode"]
    assert "run-42" == resp["headers"][CORRELATION_ID_RESPONSE_HEADER]


def test_dispatch_request_logs_the_request_outcome(caplog):
    caplog.set_level(logging.INFO)
    route_map = {"/health": lambda _: {"statusCode": 503, "headers": {}}}

    dispatch_request(make_event(), route_map, logger)

    completed = [record for record in caplog.records if record.message == "Request completed."]
    assert 1 == len(completed)
    assert 503 == completed[0].status_code
    assert completed[0].duration_ms >= 0


def test_dispatch_request_warns_on_unknown_route(caplog):
    caplog.set_level(logging.WARNING)

    resp = dispatch_request(make_event(resource="/unknown"), {}, logger)

    assert 404 == resp["statusCode"]
    assert any(record.message == "No route matched the requested resource." for record in caplog.records)


@pytest.mark.parametrize("error", [RuntimeError("boom"), OSError("connection reset"), IndexError("out of range")])
def test_dispatch_request_converts_any_handler_error_into_a_logged_500(caplog, error):
    caplog.set_level(logging.ERROR)

    def failing_route(_event):
        raise error

    resp = dispatch_request(make_event(), {"/health": failing_route}, logger)

    assert 500 == resp["statusCode"]
    assert "internal" == json.loads(resp["body"])["errors"][0]["type"]
    assert "run-42" == resp["headers"][CORRELATION_ID_RESPONSE_HEADER]
    assert any(record.message == "Unhandled error while processing the request." for record in caplog.records)


def test_dispatch_request_does_not_swallow_system_exit():
    route_map = {"/terminate": lambda _: (_ for _ in ()).throw(SystemExit("TERMINATING"))}

    with pytest.raises(SystemExit):
        dispatch_request(make_event(resource="/terminate"), route_map, logger)
