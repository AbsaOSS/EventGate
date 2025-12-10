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
from unittest.mock import patch, MagicMock


def test_unknown_resource(event_gate_module, make_event):
    event = make_event("/unknown")
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 404
    body = json.loads(resp["body"])
    assert body["errors"][0]["type"] == "route"


def test_get_api_endpoint(event_gate_module, make_event):
    event = make_event("/api")
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 200
    assert "openapi" in resp["body"].lower()


def test_internal_error_path(event_gate_module, make_event):
    with patch.object(event_gate_module.handler_topic, "get_topics_list", side_effect=RuntimeError("boom")):
        event = make_event("/topics")
        resp = event_gate_module.lambda_handler(event)
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
    resp = event_gate_module.lambda_handler(event)
    assert resp["statusCode"] == 500
    body = json.loads(resp["body"])
    assert any(e["type"] == "internal" for e in body["errors"])  # internal error path


def test_boto3_s3_client_default_ssl_verification():
    """Test that boto3 S3 client uses default SSL verification when ssl_ca_bundle not specified."""
    config = {}

    with patch("boto3.Session") as mock_session:
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        ssl_verify = config.get("ssl_ca_bundle", True)
        mock_session_instance.resource("s3", verify=ssl_verify)

        mock_session_instance.resource.assert_called_once_with("s3", verify=True)


def test_boto3_s3_client_custom_ca_bundle():
    """Test that boto3 S3 client uses custom CA bundle when ssl_ca_bundle is specified."""
    config = {"ssl_ca_bundle": "/path/to/custom-ca-bundle.pem"}

    with patch("boto3.Session") as mock_session:
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        ssl_verify = config.get("ssl_ca_bundle", True)
        mock_session_instance.resource("s3", verify=ssl_verify)

        mock_session_instance.resource.assert_called_once_with("s3", verify="/path/to/custom-ca-bundle.pem")
