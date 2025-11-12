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
import importlib.util
from contextlib import ExitStack


@pytest.fixture(scope="module")
def event_gate_module():
    started_patches = []
    exit_stack = ExitStack()

    def start_patch(target):
        p = patch(target)
        started_patches.append(p)
        return p.start()

    # Local, temporary dummy modules only if truly missing
    # confluent_kafka
    if importlib.util.find_spec("confluent_kafka") is None:  # pragma: no cover - environment dependent
        dummy_ck = types.ModuleType("confluent_kafka")

        class DummyProducer:  # minimal interface
            def __init__(self, *a, **kw):
                pass

            def produce(self, *a, **kw):
                cb = kw.get("callback")
                if cb:
                    cb(None, None)

            def flush(self):  # noqa: D401 - simple stub
                return None

        dummy_ck.Producer = DummyProducer  # type: ignore[attr-defined]

        class DummyKafkaException(Exception):
            pass

        dummy_ck.KafkaException = DummyKafkaException  # type: ignore[attr-defined]
        exit_stack.enter_context(patch.dict(sys.modules, {"confluent_kafka": dummy_ck}))

    # psycopg2 optional dependency
    if importlib.util.find_spec("psycopg2") is None:  # pragma: no cover - environment dependent
        dummy_pg = types.ModuleType("psycopg2")
        exit_stack.enter_context(patch.dict(sys.modules, {"psycopg2": dummy_pg}))

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
        def Object(self, key):  # noqa: D401 - simple proxy
            return MockS3Object()

    class MockS3Resource:
        def Bucket(self, name):  # noqa: D401 - simple proxy
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
    exit_stack.close()


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


def test_post_invalid_token_decode(event_gate_module, make_event, valid_payload):
    class DummyJwtError(Exception):
        pass

    # Patch jwt.decode to raise PyJWTError-like exception; use existing attribute if present
    with patch.object(
        event_gate_module.jwt, "decode", side_effect=event_gate_module.jwt.PyJWTError("bad"), create=True
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer abc"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 401
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "auth"


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


def test_post_invalid_json_body(event_gate_module, make_event):
    # Invalid JSON triggers json.loads exception and returns 500 internal error
    event = make_event(
        "/topics/{topic_name}",
        method="POST",
        topic="public.cps.za.test",
        body="{invalid json",
        headers={"Authorization": "Bearer token"},
    )
    resp = event_gate_module.lambda_handler(event, None)
    assert resp["statusCode"] == 500
    body = json.loads(resp["body"])
    assert any(e["type"] == "internal" for e in body["errors"])  # internal error path


def test_post_expired_token(event_gate_module, make_event, valid_payload):
    """Expired JWT should yield 401 auth error."""
    with patch.object(
        event_gate_module.jwt,
        "decode",
        side_effect=event_gate_module.jwt.ExpiredSignatureError("expired"),
        create=True,
    ):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer expiredtoken"},
        )
        resp = event_gate_module.lambda_handler(event, None)
        assert resp["statusCode"] == 401
        body = json.loads(resp["body"])
        assert any(e["type"] == "auth" for e in body["errors"])


def test_decode_jwt_all_second_key_succeeds(event_gate_module):
    """First key fails signature, second key succeeds; claims returned from second key."""
    first_key = object()
    second_key = object()
    event_gate_module.TOKEN_PUBLIC_KEYS = [first_key, second_key]

    def decode_side_effect(token, key, algorithms):
        if key is first_key:
            raise event_gate_module.jwt.PyJWTError("signature mismatch")
        return {"sub": "TestUser"}

    with patch.object(event_gate_module.jwt, "decode", side_effect=decode_side_effect, create=True):
        claims = event_gate_module.decode_jwt_all("dummy-token")
        assert claims["sub"] == "TestUser"


def test_decode_jwt_all_all_keys_fail(event_gate_module):
    """All keys fail; final PyJWTError with aggregate message is raised."""
    bad_keys = [object(), object()]
    event_gate_module.TOKEN_PUBLIC_KEYS = bad_keys

    def always_fail(token, key, algorithms):
        raise event_gate_module.jwt.PyJWTError("bad signature")

    with patch.object(event_gate_module.jwt, "decode", side_effect=always_fail, create=True):
        with pytest.raises(event_gate_module.jwt.PyJWTError) as exc:
            event_gate_module.decode_jwt_all("dummy-token")
        assert "Verification failed for all public keys" in str(exc.value)
