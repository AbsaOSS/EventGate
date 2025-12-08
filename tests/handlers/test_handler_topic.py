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
from unittest.mock import patch, mock_open, MagicMock

import jwt

from src.handlers.handler_topic import HandlerTopic


## load_topic_schemas()
def test_load_topic_schemas_success():
    """Test successful loading of all topic schemas."""
    mock_handler_token = MagicMock()
    access_config = {"public.cps.za.test": ["TestUser"]}
    handler = HandlerTopic("conf", access_config, mock_handler_token)

    mock_schemas = {
        "topic_runs.json": {"type": "object", "properties": {"run_id": {"type": "string"}}},
        "topic_dlchange.json": {"type": "object", "properties": {"change_id": {"type": "string"}}},
        "topic_test.json": {"type": "object", "properties": {"event_id": {"type": "string"}}},
    }

    def mock_open_side_effect(file_path, *args, **kwargs):
        for filename, schema in mock_schemas.items():
            if filename in file_path:
                return mock_open(read_data=json.dumps(schema)).return_value
        raise FileNotFoundError(f"File not found: {file_path}")

    with patch("builtins.open", side_effect=mock_open_side_effect):
        result = handler.load_topic_schemas()

    assert result is handler
    assert len(handler.topics) == 3
    assert "public.cps.za.runs" in handler.topics
    assert "public.cps.za.dlchange" in handler.topics
    assert "public.cps.za.test" in handler.topics


## get_topics_list()
def test_get_topics(event_gate_module, make_event):
    event = make_event("/topics")
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 200
    body = json.loads(resp["body"])
    assert "public.cps.za.test" in body


## get_topic_schema()
def test_get_topic_schema_found(event_gate_module, make_event):
    event = make_event("/topics/{topic_name}", method="GET", topic="public.cps.za.test")
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 200
    schema = json.loads(resp["body"])
    assert schema["type"] == "object"


def test_get_topic_schema_not_found(event_gate_module, make_event):
    event = make_event("/topics/{topic_name}", method="GET", topic="no.such.topic")
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 404


## post_topic_message()
# --- POST auth / validation failures ---
def test_post_missing_token(event_gate_module, make_event, valid_payload):
    event = make_event(
        "/topics/{topic_name}", method="POST", topic="public.cps.za.test", body=valid_payload, headers={}
    )
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 401
    body = json.loads(resp["body"])
    assert not body["success"]
    assert body["errors"][0]["type"] == "auth"


def test_post_unauthorized_user(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "NotAllowed"}):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 403
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "auth"


def test_post_schema_validation_error(event_gate_module, make_event):
    payload = {"event_id": "e1", "tenant_id": "t1", "source_app": "app", "environment": "dev"}  # missing timestamp
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "validation"


def test_post_invalid_token_decode(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", side_effect=jwt.PyJWTError("bad")):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer abc"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 401
        body = json.loads(resp["body"])
        assert body["errors"][0]["type"] == "auth"


# --- POST success & failure aggregation ---
def test_post_success_all_writers(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}),
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
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 202
        body = json.loads(resp["body"])
        assert body["success"]
        assert body["statusCode"] == 202


def test_post_single_writer_failure(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}),
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
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 500
        body = json.loads(resp["body"])
        assert not body["success"]
        assert len(body["errors"]) == 1
        assert body["errors"][0]["type"] == "kafka"


def test_post_multiple_writer_failures(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}),
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
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 500
        body = json.loads(resp["body"])
        assert sorted(e["type"] for e in body["errors"]) == ["eventbridge", "kafka"]


def test_token_extraction_lowercase_bearer_header(event_gate_module, make_event, valid_payload):
    with (
        patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}),
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
        resp = event_gate_module.lambda_handler(event)
        assert resp["statusCode"] == 202
