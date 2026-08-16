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
import re
from unittest.mock import patch, mock_open, MagicMock

import jwt
import pytest

from src.handlers.handler_topic import HandlerTopic
from src.utils.observability import logger as powertools_logger
from src.writers.writer import WriteError


## load_access_config()
def test_load_access_config_from_local_file():
    """Test loading access config from local file."""
    mock_handler_token = MagicMock()
    mock_aws_s3 = MagicMock()
    mock_writers = {
        "kafka": MagicMock(),
        "eventbridge": MagicMock(),
        "postgres": MagicMock(),
    }
    config = {"access_config": "conf/access.json"}
    handler = HandlerTopic(config, mock_aws_s3, mock_handler_token, mock_writers)

    access_data = {"public.cps.za.test": ["TestUser"]}
    with patch("builtins.open", mock_open(read_data=json.dumps(access_data))):
        result = handler.with_load_access_config()

    assert result is handler
    assert {"TestUser": {}} == handler.access_config["public.cps.za.test"]


def test_load_access_config_from_s3():
    """Test loading access config from S3."""
    mock_handler_token = MagicMock()
    mock_aws_s3 = MagicMock()
    mock_writers = {
        "kafka": MagicMock(),
        "eventbridge": MagicMock(),
        "postgres": MagicMock(),
    }
    config = {"access_config": "s3://my-bucket/path/to/access.json"}
    handler = HandlerTopic(config, mock_aws_s3, mock_handler_token, mock_writers)

    access_data = {"public.cps.za.test": ["TestUser"]}
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(access_data).encode("utf-8")
    mock_aws_s3.Bucket.return_value.Object.return_value.get.return_value = {"Body": mock_body}

    result = handler.with_load_access_config()

    assert result is handler
    assert {"TestUser": {}} == handler.access_config["public.cps.za.test"]
    mock_aws_s3.Bucket.assert_called_once_with("my-bucket")
    mock_aws_s3.Bucket.return_value.Object.assert_called_once_with("path/to/access.json")


## load_topic_keys_config()
def test_load_topic_keys_config_from_local_file():
    """Test loading topic key config from local file."""
    mock_handler_token = MagicMock()
    mock_aws_s3 = MagicMock()
    mock_writers = {
        "kafka": MagicMock(),
        "eventbridge": MagicMock(),
        "postgres": MagicMock(),
    }
    config = {"topic_keys_config": "conf/topic_keys.json"}
    handler = HandlerTopic(config, mock_aws_s3, mock_handler_token, mock_writers)

    topic_keys_data = {"public.cps.za.test": "event_id"}
    with patch("builtins.open", mock_open(read_data=json.dumps(topic_keys_data))):
        result = handler.with_load_topic_keys_config()

    assert result is handler
    assert "event_id" == handler.topic_keys["public.cps.za.test"]


def test_load_topic_keys_config_from_s3():
    """Test loading topic key config from S3."""
    mock_handler_token = MagicMock()
    mock_aws_s3 = MagicMock()
    mock_writers = {
        "kafka": MagicMock(),
        "eventbridge": MagicMock(),
        "postgres": MagicMock(),
    }
    config = {"topic_keys_config": "s3://my-bucket/path/to/topic_keys.json"}
    handler = HandlerTopic(config, mock_aws_s3, mock_handler_token, mock_writers)

    topic_keys_data = {"public.cps.za.status_change": "job_id"}
    mock_body = MagicMock()
    mock_body.read.return_value = json.dumps(topic_keys_data).encode("utf-8")
    mock_aws_s3.Bucket.return_value.Object.return_value.get.return_value = {"Body": mock_body}

    result = handler.with_load_topic_keys_config()

    assert result is handler
    assert "job_id" == handler.topic_keys["public.cps.za.status_change"]
    mock_aws_s3.Bucket.assert_called_once_with("my-bucket")
    mock_aws_s3.Bucket.return_value.Object.assert_called_once_with("path/to/topic_keys.json")


## load_topic_schemas()
def test_load_topic_schemas_success():
    mock_handler_token = MagicMock()
    mock_writers = {
        "kafka": MagicMock(),
        "eventbridge": MagicMock(),
        "postgres": MagicMock(),
    }
    config = {"access_config": "conf/access.json"}
    mock_aws_s3 = MagicMock()
    handler = HandlerTopic(config, mock_aws_s3, mock_handler_token, mock_writers)

    mock_schemas = {
        "runs.json": {"type": "object", "properties": {"run_id": {"type": "string"}}},
        "dlchange.json": {"type": "object", "properties": {"change_id": {"type": "string"}}},
        "test.json": {"type": "object", "properties": {"event_id": {"type": "string"}}},
        "status_change.json": {"type": "object", "properties": {"execution_id": {"type": "string"}}},
    }

    def mock_open_side_effect(file_path, *_args, **_kwargs):
        for filename, schema in mock_schemas.items():
            if filename in file_path:
                return mock_open(read_data=json.dumps(schema)).return_value
        raise FileNotFoundError(file_path)

    with patch("builtins.open", side_effect=mock_open_side_effect):
        result = handler.with_load_topic_schemas()

    assert result is handler
    assert 4 == len(handler.topics)
    assert "public.cps.za.runs" in handler.topics
    assert "public.cps.za.dlchange" in handler.topics
    assert "public.cps.za.test" in handler.topics
    assert "public.cps.za.status_change" in handler.topics


## get_topics_list()
def test_get_topics(event_gate_module, make_event):
    event = make_event("/topics")
    resp = event_gate_module.lambda_handler(event)
    assert 200 == resp["statusCode"]
    body = json.loads(resp["body"])
    assert "public.cps.za.test" in body
    assert "public.cps.za.status_change" in body


## get_topic_schema()
def test_get_topic_schema_found(event_gate_module, make_event):
    event = make_event("/topics/{topic_name}", method="GET", topic="public.cps.za.test")
    resp = event_gate_module.lambda_handler(event)
    assert 200 == resp["statusCode"]
    schema = json.loads(resp["body"])
    assert "object" == schema["type"]


def test_get_topic_schema_not_found(event_gate_module, make_event):
    event = make_event("/topics/{topic_name}", method="GET", topic="no.such.topic")
    resp = event_gate_module.lambda_handler(event)
    assert 404 == resp["statusCode"]


## post_topic_message()
# --- POST auth / validation failures ---
def test_post_missing_token(event_gate_module, make_event, valid_payload):
    event = make_event(
        "/topics/{topic_name}", method="POST", topic="public.cps.za.test", body=valid_payload, headers={}
    )
    resp = event_gate_module.lambda_handler(event)
    assert 401 == resp["statusCode"]
    body = json.loads(resp["body"])
    assert not body["success"]
    assert "auth" == body["errors"][0]["type"]


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
        assert 403 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "auth" == body["errors"][0]["type"]


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
        assert 400 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "validation" == body["errors"][0]["type"]


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
        assert 401 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "auth" == body["errors"][0]["type"]


# --- POST success & failure aggregation ---
def test_post_success_all_writers(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        for writer in event_gate_module.handler_topic.writers.values():
            writer.write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 202 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert body["success"]
        assert 202 == body["statusCode"]


def test_post_authorized_user_case_insensitive(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "testuser"}):
        for writer in event_gate_module.handler_topic.writers.values():
            writer.write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 202 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert body["success"]


def test_post_passes_topic_key_to_writers(event_gate_module, make_event, valid_payload):
    """Configured topic key field is extracted and passed to writer.write."""
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.topic_keys["public.cps.za.test"] = "event_id"
        kafka_writer = event_gate_module.handler_topic.writers["kafka"]
        kafka_writer.write = MagicMock(return_value=None)
        event_gate_module.handler_topic.writers["eventbridge"].write = MagicMock(return_value=None)
        event_gate_module.handler_topic.writers["postgres"].write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 202 == resp["statusCode"]

        kafka_writer.write.assert_called_once_with("public.cps.za.test", valid_payload, "e1")


def test_post_missing_topic_key_field_falls_back_to_empty_key(event_gate_module, make_event, valid_payload):
    """Missing configured key field falls back to empty message key."""
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.topic_keys["public.cps.za.test"] = "job_id"
        kafka_writer = event_gate_module.handler_topic.writers["kafka"]
        kafka_writer.write = MagicMock(return_value=None)
        event_gate_module.handler_topic.writers["eventbridge"].write = MagicMock(return_value=None)
        event_gate_module.handler_topic.writers["postgres"].write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 202 == resp["statusCode"]

        kafka_writer.write.assert_called_once_with("public.cps.za.test", valid_payload, "")


def test_post_single_writer_failure(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.writers["kafka"].write = MagicMock(side_effect=WriteError("Kafka boom"))
        event_gate_module.handler_topic.writers["eventbridge"].write = MagicMock(return_value=None)
        event_gate_module.handler_topic.writers["postgres"].write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 500 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert not body["success"]
        assert 1 == len(body["errors"])
        assert "kafka" == body["errors"][0]["type"]


def test_post_multiple_writer_failures(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.writers["kafka"].write = MagicMock(side_effect=WriteError("Kafka A"))
        event_gate_module.handler_topic.writers["eventbridge"].write = MagicMock(side_effect=WriteError("EB B"))
        event_gate_module.handler_topic.writers["postgres"].write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 500 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert ["eventbridge", "kafka"] == sorted(e["type"] for e in body["errors"])


def test_token_extraction_lowercase_bearer_header(event_gate_module, make_event, valid_payload):
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        for writer in event_gate_module.handler_topic.writers.values():
            writer.write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"authorization": "bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 202 == resp["statusCode"]


## _validate_user_permissions()
@pytest.mark.parametrize(
    "user_perms,payload_updates",
    [
        ({}, {}),
        ({"source_app": [re.compile("app")]}, {}),
        ({"tenant_id": [re.compile("avms|avm")]}, {"tenant_id": "avms"}),
        ({"source_app": [re.compile("app")], "environment": [re.compile("dev")]}, {}),
    ],
    ids=["no-restrictions", "exact-match", "regex-match", "multiple-fields"],
)
def test_post_permission_allowed(event_gate_module, make_event, valid_payload, user_perms, payload_updates):
    """User with matching permissions can post successfully."""
    valid_payload.update(payload_updates)
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.access_config["public.cps.za.test"] = {"TestUser": user_perms}
        for writer in event_gate_module.handler_topic.writers.values():
            writer.write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 202 == resp["statusCode"]


@pytest.mark.parametrize(
    "user_perms,payload_updates,expected_fragment",
    [
        ({"environment": [re.compile("prod")]}, {}, "environment"),
        ({"nonexistent_field": [re.compile("val")]}, {}, "nonexistent_field"),
        ({"tenant_id": [re.compile("avms|avm")]}, {"tenant_id": "xxxx"}, "tenant_id"),
        ({"source_app": [re.compile("other")], "environment": [re.compile("prod")]}, {}, "source_app"),
    ],
    ids=["value-mismatch", "missing-field", "regex-no-match", "first-constraint-fails"],
)
def test_post_permission_denied(
    event_gate_module, make_event, valid_payload, user_perms, payload_updates, expected_fragment
):
    """User with non-matching permissions gets 403."""
    valid_payload.update(payload_updates)
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.access_config["public.cps.za.test"] = {"TestUser": user_perms}
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)
        assert 403 == resp["statusCode"]
        body = json.loads(resp["body"])
        assert "permission" == body["errors"][0]["type"]
        assert expected_fragment in body["errors"][0]["message"]


## request rejection logging
def logged_messages(caplog, level):
    """Collect log messages captured at a single level."""
    return [record.message for record in caplog.records if record.levelno == level]


def test_post_missing_topic_path_parameter_is_rejected(event_gate_module, make_event, caplog):
    caplog.set_level(logging.WARNING)
    event = make_event("/topics/{topic_name}", method="POST", body={"a": 1})

    resp = event_gate_module.lambda_handler(event)

    assert 400 == resp["statusCode"]
    assert "Request rejected: path parameter 'topic_name' is missing." in logged_messages(caplog, logging.WARNING)


def test_post_non_object_body_is_rejected(event_gate_module, make_event, caplog):
    caplog.set_level(logging.WARNING)
    event = make_event("/topics/{topic_name}", method="POST", topic="public.cps.za.test", body="[1, 2]")

    resp = event_gate_module.lambda_handler(event)

    assert 400 == resp["statusCode"]
    assert "Request rejected: message body is not a JSON object." in logged_messages(caplog, logging.WARNING)


def test_unsupported_method_is_rejected(event_gate_module, make_event, caplog):
    caplog.set_level(logging.WARNING)
    event = make_event("/topics/{topic_name}", method="PUT", topic="public.cps.za.test")

    resp = event_gate_module.lambda_handler(event)

    assert 404 == resp["statusCode"]
    assert "Request rejected: unsupported HTTP method." in logged_messages(caplog, logging.WARNING)


def test_invalid_token_is_logged(event_gate_module, make_event, valid_payload, caplog):
    caplog.set_level(logging.WARNING)
    with patch.object(event_gate_module.handler_token, "decode_jwt", side_effect=jwt.PyJWTError("nope")):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)

    assert 401 == resp["statusCode"]
    assert "Request rejected: token verification failed." in logged_messages(caplog, logging.WARNING)


def test_unauthorized_user_is_logged(event_gate_module, make_event, valid_payload, caplog):
    caplog.set_level(logging.WARNING)
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "UnknownUser"}):
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)

    assert 403 == resp["statusCode"]
    assert "Request rejected: user is not authorized for the topic." in logged_messages(caplog, logging.WARNING)


def test_schema_validation_failure_is_logged(event_gate_module, make_event, caplog):
    caplog.set_level(logging.WARNING)
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.access_config["public.cps.za.test"] = {"TestUser": {}}
        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body={"event_id": "e1"},
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)

    assert 400 == resp["statusCode"]
    assert "Request rejected: message does not match the topic schema." in logged_messages(caplog, logging.WARNING)


def test_partial_writer_failure_reports_both_sides(event_gate_module, make_event, valid_payload, caplog):
    caplog.set_level(logging.ERROR)
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.access_config["public.cps.za.test"] = {"TestUser": {}}
        for name, writer in event_gate_module.handler_topic.writers.items():
            writer.write = MagicMock(side_effect=WriteError("down") if name == "postgres" else None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)

    dispatch_failures = [r for r in caplog.records if r.message == "Message dispatch failed for at least one writer."]
    assert 500 == resp["statusCode"]
    assert 1 == len(dispatch_failures)
    assert ["postgres"] == dispatch_failures[0].writers_failed
    assert ["eventbridge", "kafka"] == sorted(dispatch_failures[0].writers_ok)
    assert ["down"] == [error["message"] for error in dispatch_failures[0].writer_errors]


def test_accepted_message_is_logged(event_gate_module, make_event, valid_payload, caplog):
    caplog.set_level(logging.INFO)
    with patch.object(event_gate_module.handler_token, "decode_jwt", return_value={"sub": "TestUser"}):
        event_gate_module.handler_topic.access_config["public.cps.za.test"] = {"TestUser": {}}
        for writer in event_gate_module.handler_topic.writers.values():
            writer.write = MagicMock(return_value=None)

        event = make_event(
            "/topics/{topic_name}",
            method="POST",
            topic="public.cps.za.test",
            body=valid_payload,
            headers={"Authorization": "Bearer token"},
        )
        resp = event_gate_module.lambda_handler(event)

    completed = [r for r in caplog.records if r.message == "Request completed."]
    assert 202 == resp["statusCode"]
    assert 1 == len(completed)

    # The outcome fields live on the formatter (request scoped keys), not on the LogRecord.
    payload = json.loads(powertools_logger.registered_formatter.format(completed[0]))
    assert 202 == payload["status_code"]
    assert ["eventbridge", "kafka", "postgres"] == sorted(payload["writers_ok"])
    assert "message_key" in payload
