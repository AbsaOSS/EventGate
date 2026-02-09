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

from unittest.mock import MagicMock, patch
from botocore.exceptions import BotoCoreError

from src.writers.writer_eventbridge import WriterEventBridge


# --- write() ---


def test_write_skips_when_no_event_bus():
    writer = WriterEventBridge({"event_bus_arn": ""})
    ok, err = writer.write("topic", {"k": 1})
    assert ok and err is None


def test_write_success():
    writer = WriterEventBridge({"event_bus_arn": "arn:aws:events:region:acct:event-bus/bus"})
    mock_client = MagicMock()
    mock_client.put_events.return_value = {"FailedEntryCount": 0, "Entries": []}
    writer._client = mock_client
    ok, err = writer.write("topic", {"k": 2})
    assert ok and err is None
    mock_client.put_events.assert_called_once()


def test_write_failed_entries():
    writer = WriterEventBridge({"event_bus_arn": "arn:aws:events:region:acct:event-bus/bus"})
    mock_client = MagicMock()
    mock_client.put_events.return_value = {
        "FailedEntryCount": 1,
        "Entries": [
            {"EventId": "1", "ErrorCode": "Err", "ErrorMessage": "Bad"},
            {"EventId": "2"},
        ],
    }
    writer._client = mock_client
    ok, err = writer.write("topic", {"k": 3})
    assert not ok and "EventBridge" in err


def test_write_client_error():
    from botocore.exceptions import BotoCoreError

    class DummyError(BotoCoreError):
        pass

    writer = WriterEventBridge({"event_bus_arn": "arn:aws:events:region:acct:event-bus/bus"})
    mock_client = MagicMock()
    mock_client.put_events.side_effect = DummyError()
    writer._client = mock_client
    ok, err = writer.write("topic", {"k": 4})
    assert not ok and err is not None


# --- check_health() ---


def test_check_health_not_configured():
    writer = WriterEventBridge({"event_bus_arn": ""})
    healthy, msg = writer.check_health()
    assert healthy and msg == "not configured"


def test_check_health_success():
    writer = WriterEventBridge({"event_bus_arn": "arn:aws:events:region:acct:event-bus/bus"})
    with patch("boto3.client") as mock_client:
        mock_client.return_value = MagicMock()
        healthy, msg = writer.check_health()
    assert healthy and msg == "ok"
    assert writer._client is not None


def test_check_health_client_error():
    class DummyError(BotoCoreError):
        fmt = "Dummy error"

    writer = WriterEventBridge({"event_bus_arn": "arn:aws:events:region:acct:event-bus/bus"})
    with patch("boto3.client", side_effect=DummyError()):
        healthy, msg = writer.check_health()
    assert not healthy and msg is not None
