import importlib
import io
import json
from unittest.mock import patch, MagicMock


def test_local_access_config_branch():
    import src.event_gate_lambda as egl  # initial import (s3 path) just to ensure module present

    original_open = open  # noqa: A001

    local_config = {
        "access_config": "conf/access.json",
        "token_provider_url": "https://example/token",
        "token_public_key_url": "https://example/key",
        "kafka_bootstrap_server": "localhost:9092",
        "event_bus_arn": "arn:aws:events:region:acct:event-bus/bus",
    }

    access_payload = {
        "public.cps.za.runs": ["User"],
        "public.cps.za.dlchange": ["User"],
        "public.cps.za.test": ["User"],
    }

    def open_side_effect(path, *args, **kwargs):  # noqa: D401
        p = str(path)
        if p.endswith("config.json"):
            return io.StringIO(json.dumps(local_config))
        if p.endswith("conf/access.json") or p == "conf/access.json":
            return io.StringIO(json.dumps(access_payload))
        return original_open(path, *args, **kwargs)

    with (
        patch("requests.get") as mock_get,
        patch("cryptography.hazmat.primitives.serialization.load_der_public_key") as mock_load_key,
        patch("boto3.Session") as mock_session,
        patch("boto3.client") as mock_boto_client,
        patch("confluent_kafka.Producer") as mock_kafka_producer,
        patch("builtins.open", side_effect=open_side_effect),
    ):
        mock_get.return_value.json.return_value = {"key": "ZHVtbXk="}  # base64 for 'dummy'
        mock_load_key.return_value = object()
        mock_kafka_producer.return_value = MagicMock()

        class MockS3:
            def Bucket(self, name):  # noqa: D401
                raise AssertionError("S3 branch should not be used for local access_config")

        mock_session.return_value.resource.return_value = MockS3()
        mock_events = MagicMock()
        mock_events.put_events.return_value = {"FailedEntryCount": 0}
        mock_boto_client.return_value = mock_events

        # Force reload so import-level logic re-executes with patched open
        egl_reloaded = importlib.reload(egl)

    assert not egl_reloaded.config["access_config"].startswith("s3://")  # type: ignore[attr-defined]
    assert egl_reloaded.ACCESS["public.cps.za.test"] == ["User"]  # type: ignore[attr-defined]
