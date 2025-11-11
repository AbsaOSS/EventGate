import base64
import json
import importlib
from unittest.mock import patch, MagicMock
import pytest


@pytest.fixture(scope="module")
def egl_mod():
    patches = []

    def start(target):
        p = patch(target)
        patches.append(p)
        return p.start()

    # Patch requests.get for public key fetch
    mock_get = start("requests.get")
    mock_get.return_value.json.return_value = {"key": base64.b64encode(b"dummy_der").decode("utf-8")}

    # Patch crypto key loader
    start_loader = start("cryptography.hazmat.primitives.serialization.load_der_public_key")
    start_loader.return_value = object()

    # Patch boto3.Session resource for S3 to avoid bucket validation/network
    class MockBody:
        def read(self):
            return json.dumps(
                {
                    "public.cps.za.runs": ["User"],
                    "public.cps.za.dlchange": ["User"],
                    "public.cps.za.test": ["User"],
                }
            ).encode("utf-8")

    class MockObject:
        def get(self):
            return {"Body": MockBody()}

    class MockBucket:
        def Object(self, key):  # noqa: D401
            return MockObject()

    class MockS3:
        def Bucket(self, name):  # noqa: D401
            return MockBucket()

    mock_session = start("boto3.Session")
    mock_session.return_value.resource.return_value = MockS3()

    # Patch boto3.client for EventBridge
    mock_client = start("boto3.client")
    mock_events = MagicMock()
    mock_events.put_events.return_value = {"FailedEntryCount": 0}
    mock_client.return_value = mock_events

    # Patch Kafka Producer
    start("confluent_kafka.Producer")

    module = importlib.import_module("src.event_gate_lambda")
    yield module

    for p in patches:
        p.stop()


def test_extract_token_empty(egl_mod):
    assert egl_mod.extract_token({}) == ""


def test_extract_token_direct_bearer_header(egl_mod):
    token = egl_mod.extract_token({"Bearer": "  tok123  "})
