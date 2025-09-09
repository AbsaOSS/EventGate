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
import base64
import json
import importlib
import sys
import types
import pytest
from unittest.mock import patch, MagicMock

# Inject dummy confluent_kafka if not installed so patching works
if "confluent_kafka" not in sys.modules:
    dummy_ck = types.ModuleType("confluent_kafka")

    class DummyProducer:  # minimal interface
        def __init__(self, *a, **kw):
            pass

        def produce(self, *a, **kw):
            cb = kw.get("callback")
            if cb:
                cb(None, None)

        def flush(self):
            return None

    dummy_ck.Producer = DummyProducer
    sys.modules["confluent_kafka"] = dummy_ck

# Inject dummy psycopg2 (optional dependency)
if "psycopg2" not in sys.modules:
    sys.modules["psycopg2"] = types.ModuleType("psycopg2")


@pytest.fixture(scope="module")
def event_gate_module():
    started_patches = []

    def start_patch(target):
        p = patch(target)
        started_patches.append(p)
        return p.start()

    mock_requests_get = start_patch("requests.get")
    mock_requests_get.return_value.json.return_value = {"key": base64.b64encode(b"dummy_der").decode("utf-8")}

    mock_load_key = start_patch("cryptography.hazmat.primitives.serialization.load_der_public_key")
    mock_load_key.return_value = object()

    # Mock S3 access_config retrieval
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
        def Object(self, key):
            return MockS3Object()

    class MockS3Resource:
        def Bucket(self, name):
            return MockS3Bucket()

    mock_session = start_patch("boto3.Session")
    mock_session.return_value.resource.return_value = MockS3Resource()

    # Mock EventBridge client
    mock_boto_client = start_patch("boto3.client")
    mock_events_client = MagicMock()
    mock_events_client.put_events.return_value = {"FailedEntryCount": 0}
    mock_boto_client.return_value = mock_events_client

    # Allow kafka producer patching (already stubbed) but still patch to inspect if needed
    start_patch("confluent_kafka.Producer")

    module = importlib.import_module("src.event_gate_lambda")

    yield module

    for p in started_patches:
        p.stop()


@pytest.fixture
def make_event():
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
    return {"event_id": "e1", "tenant_id": "t1", "source_app": "app", "environment": "dev", "timestamp": 123}


# --- GET flows ---


def test_get_topics(event_gate_module, make_event):
    event = make_event("/topics")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert "public.cps.za.test" in body


def test_get_topic_schema_found(event_gate_module, make_event):
    event = make_event("/topics/{topic_name}", method="GET", topic="public.cps.za.test")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 200
    schema = json.loads(resp["body"])
    assert schema["type"] == "object"


def test_get_topic_schema_not_found(event_gate_module, make_event):
    event = make_event("/topics/{topic_name}", method="GET", topic="no.such.topic")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 404


# --- POST auth / validation failures ---


def test_post_missing_token(event_gate_module, make_event, valid_payload):
    event = make_event(
        "/topics/{topic_name}", method="POST", topic="public.cps.za.test", body=valid_payload, headers={}
    )
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 401
    body = json.loads(resp["body"])
    assert not body["success"]
    assert body["errors"][0]["type"] == "auth"


def test_post_unauthorized_user(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.jwt, "decode", return_value={"sub": "NotAllowed"}, create=True):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 403
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "auth"


def test_post_schema_validation_error(event_gate_module, make_event):
    payload = {"event_id": "e1", "tenant_id": "t1", "source_app": "app", "environment": "dev"}  # missing timestamp
    with patch.object(event_gate_module.jwt, "decode", return_value={"sub": "TestUser"}, create=True):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "validation"


# --- POST success & failure aggregation ---


def test_post_success_all_writers(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.jwt, "decode", return_value={"sub": "TestUser"}, create=True),
        patch("src.event_gate_lambda.writer_kafka.write", return_value=(True, None)),
        patch("src.event_gate_lambda.writer_eventbridge.write", return_value=(True, None)),
        patch("src.event_gate_lambda.writer_postgres.write", return_value=(True, None)),
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 202
        body = json.loads(resp["body"])
        assert body["success"]
        assert body["statusCode"] == 202


def test_post_single_writer_failure(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.jwt, "decode", return_value={"sub": "TestUser"}, create=True),
        patch("src.event_gate_lambda.writer_kafka.write", return_value=(False, "Kafka boom")),
        patch("src.event_gate_lambda.writer_eventbridge.write", return_value=(True, None)),
        patch("src.event_gate_lambda.writer_postgres.write", return_value=(True, None)),
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        body = json.loads(resp["body"])
        assert not body["success"]
        assert len(body["errors"]) == 1
        assert body["errors"][0]["type"] == "kafka"


def test_post_multiple_writer_failures(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.jwt, "decode", return_value={"sub": "TestUser"}, create=True),
        patch("src.event_gate_lambda.writer_kafka.write", return_value=(False, "Kafka A")),
        patch("src.event_gate_lambda.writer_eventbridge.write", return_value=(False, "EB B")),
        patch("src.event_gate_lambda.writer_postgres.write", return_value=(True, None)),
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        body = json.loads(resp["body"])
        assert sorted(e["type"] for e in body["errors"]) == ["eventbridge", "kafka"]


def test_token_extraction_lowercase_bearer_header(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.jwt, "decode", return_value={"sub": "TestUser"}, create=True),
        patch("src.event_gate_lambda.writer_kafka.write", return_value=(True, None)),
        patch("src.event_gate_lambda.writer_eventbridge.write", return_value=(True, None)),
        patch("src.event_gate_lambda.writer_postgres.write", return_value=(True, None)),
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"bearer": "token"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 202


def test_unknown_resource(event_gate_module, make_event):
    event = make_event("/unknown")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 404
    body = json.loads(resp["body"])
    assert body["errors"][0]["type"] == "route"


def test_get_api_endpoint(event_gate_module, make_event):
    event = make_event("/api")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 200
    assert "openapi" in resp["body"].lower()


def test_get_token_endpoint(event_gate_module, make_event):
    event = make_event("/token")
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 303
    assert "Location" in resp["headers"]


def test_internal_error_path(event_gate_module, make_event):
    with patch("src.event_gate_lambda.get_topics", side_effect=RuntimeError("boom")):
        event = make_event("/topics")
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 500
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "internal"
