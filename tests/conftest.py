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
import os, sys
import base64
import importlib.util
import json
import types
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest


# Ensure project root is on sys.path so 'src' package is importable during tests
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


@pytest.fixture(scope="module")
def event_gate_module():
    """Import `src.event_gate_lambda` with external deps patched/mocked.

    This fixture centralises the heavy environment setup shared by
    multiple test modules.
    """
    started_patches = []
    exit_stack = ExitStack()

    def start_patch(target: str):
        p = patch(target)
        started_patches.append(p)
        return p.start()

    # Local, temporary dummy modules only if truly missing
    if importlib.util.find_spec("confluent_kafka") is None:
        dummy_ck = types.ModuleType("confluent_kafka")

        class DummyProducer:
            def __init__(self, *_, **__):
                pass

            def produce(self, *_, **kwargs):
                cb = kwargs.get("callback")
                if cb:
                    cb(None, None)

            def flush(self):
                return None

        dummy_ck.Producer = DummyProducer

        class DummyKafkaException(Exception):
            pass

        dummy_ck.KafkaException = DummyKafkaException
        exit_stack.enter_context(patch.dict(sys.modules, {"confluent_kafka": dummy_ck}))

    if importlib.util.find_spec("psycopg2") is None:
        dummy_pg = types.ModuleType("psycopg2")
        exit_stack.enter_context(patch.dict(sys.modules, {"psycopg2": dummy_pg}))

    mock_requests_get = start_patch("requests.get")
    mock_requests_get.return_value.json.return_value = {"key": base64.b64encode(b"dummy_der").decode("utf-8")}

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

    mock_boto_client = start_patch("boto3.client")
    mock_events_client = MagicMock()
    mock_events_client.put_events.return_value = {"FailedEntryCount": 0}
    mock_boto_client.return_value = mock_events_client

    mock_kafka_producer = start_patch("confluent_kafka.Producer")
    mock_kafka_producer.return_value = MagicMock()

    module = importlib.import_module("src.event_gate_lambda")

    yield module

    for p in started_patches:
        p.stop()
    exit_stack.close()


@pytest.fixture
def make_event():
    """Build a minimal API Gateway-style event dict for tests."""

    def _make(resource, method="GET", body=None, topic=None, headers=None):
        return {
            "resource": resource,
            "httpMethod": method,
            "headers": headers or {},
            "pathParameters": {"topic_name": topic} if topic else {},
            "body": json.dumps(body) if isinstance(body, dict) else body,
        }

    return _make


@pytest.fixture
def valid_payload():
    """A canonical valid payload used across tests."""
    return {
        "event_id": "e1",
        "tenant_id": "t1",
        "source_app": "app",
        "environment": "dev",
        "timestamp": 123,
    }
