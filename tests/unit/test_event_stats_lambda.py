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


import base64
import importlib
import importlib.util
import json
import sys
import types
from contextlib import ExitStack
from unittest.mock import MagicMock, Mock, patch

import pytest


@pytest.fixture(scope="module")
def event_stats_module():
    """Import ``src.event_stats_lambda`` with external deps patched/mocked."""
    started_patches: list = []
    exit_stack = ExitStack()

    def start_patch(target: str):
        p = patch(target)
        started_patches.append(p)
        return p.start()

    # Local dummy modules only if truly missing.
    if importlib.util.find_spec("confluent_kafka") is None:
        dummy_ck = types.ModuleType("confluent_kafka")

        class DummyProducer:
            def __init__(self, *_, **__):
                pass

        dummy_ck.Producer = DummyProducer

        class DummyKafkaException(Exception):
            pass

        dummy_ck.KafkaException = DummyKafkaException
        exit_stack.enter_context(patch.dict(sys.modules, {"confluent_kafka": dummy_ck}))

    if importlib.util.find_spec("psycopg2") is None:
        dummy_pg = types.ModuleType("psycopg2")
        exit_stack.enter_context(patch.dict(sys.modules, {"psycopg2": dummy_pg}))

    mock_requests_get = start_patch("requests.get")
    mock_requests_get.return_value.json.return_value = {"keys": [{"key": base64.b64encode(b"dummy_der").decode()}]}

    mock_load_key = start_patch("cryptography.hazmat.primitives.serialization.load_der_public_key")
    mock_load_key.return_value = object()

    class MockS3ObjectBody:
        def read(self):
            return json.dumps(
                {
                    "public.cps.za.runs": ["FooBarUser"],
                    "public.cps.za.dlchange": ["FooUser", "BarUser"],
                    "public.cps.za.test": ["TestUser"],
                }
            ).encode("utf-8")

    class MockS3Object:
        def get(self):
            return {"Body": MockS3ObjectBody()}

    class MockS3Bucket:
        def Object(self, _key):
            return MockS3Object()

    class MockS3Resource:
        def Bucket(self, _name):
            return MockS3Bucket()

    mock_session = start_patch("boto3.Session")
    mock_session.return_value.resource.return_value = MockS3Resource()

    module = importlib.import_module("src.event_stats_lambda")

    yield module

    for p in started_patches:
        p.stop()
    exit_stack.close()


@pytest.fixture
def make_stats_event():
    """Build a minimal API Gateway-style event dict for stats tests."""

    def _make(resource, method="POST", body=None, topic=None, headers=None):
        return {
            "resource": resource,
            "httpMethod": method,
            "headers": headers or {},
            "pathParameters": {"topic_name": topic} if topic else {},
            "body": json.dumps(body) if isinstance(body, dict) else body,
        }

    return _make


class TestEventStatsLambdaRouteMap:
    """Tests for route map and dispatch."""

    def test_route_map_contains_stats(self, event_stats_module) -> None:
        """Test that /stats/{topic_name} is in ROUTE_MAP."""
        assert "/stats/{topic_name}" in event_stats_module.ROUTE_MAP

    def test_route_map_contains_health(self, event_stats_module) -> None:
        """Test that /health is in ROUTE_MAP."""
        assert "/health" in event_stats_module.ROUTE_MAP

    def test_route_map_does_not_contain_topics(self, event_stats_module) -> None:
        """Test that /topics is NOT in the stats Lambda ROUTE_MAP."""
        assert "/topics" not in event_stats_module.ROUTE_MAP

    def test_route_map_does_not_contain_api(self, event_stats_module) -> None:
        """Test that /api is NOT in the stats Lambda ROUTE_MAP."""
        assert "/api" not in event_stats_module.ROUTE_MAP


class TestEventStatsLambdaDispatch:
    """Tests for lambda_handler dispatch."""

    def test_unknown_resource_returns_404(self, event_stats_module, make_stats_event) -> None:
        """Test that unknown route returns 404."""
        event = make_stats_event("/unknown")
        resp = event_stats_module.lambda_handler(event)

        assert 404 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "route" == body["errors"][0]["type"]

    def test_health_returns_200(self, event_stats_module, make_stats_event) -> None:
        """Test that /health route returns 200 when reader is healthy."""
        with patch.object(event_stats_module.reader_postgres, "check_health", return_value=(True, "ok")):
            event = make_stats_event("/health", method="GET")
            resp = event_stats_module.lambda_handler(event)

        assert 200 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "ok" == body["status"]

    def test_health_returns_503_when_degraded(self, event_stats_module, make_stats_event) -> None:
        """Test that /health returns 503 when reader is unhealthy."""
        with patch.object(
            event_stats_module.reader_postgres, "check_health", return_value=(False, "connection refused")
        ):
            event = make_stats_event("/health", method="GET")
            resp = event_stats_module.lambda_handler(event)

        assert 503 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "degraded" == body["status"]

    def test_stats_route_dispatches_to_handler(self, event_stats_module, make_stats_event) -> None:
        """Test that /stats/{topic_name} dispatches to HandlerStats."""
        mock_response = {"statusCode": 401, "body": json.dumps({"success": False, "statusCode": 401, "errors": []})}
        mock_fn = Mock(return_value=mock_response)
        with patch.dict(event_stats_module.ROUTE_MAP, {"/stats/{topic_name}": mock_fn}):
            event = make_stats_event("/stats/{topic_name}", topic="public.cps.za.runs")
            resp = event_stats_module.lambda_handler(event)

        assert 401 == resp["statusCode"]
        mock_fn.assert_called_once_with(event)

    def test_internal_error_returns_500(self, event_stats_module, make_stats_event) -> None:
        """Test that uncaught exception returns 500."""
        mock_fn = Mock(side_effect=RuntimeError("boom"))
        with patch.dict(event_stats_module.ROUTE_MAP, {"/stats/{topic_name}": mock_fn}):
            event = make_stats_event("/stats/{topic_name}", topic="public.cps.za.runs")
            resp = event_stats_module.lambda_handler(event)

        assert 500 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "internal" == body["errors"][0]["type"]
