import logging
from unittest.mock import MagicMock

import src.writers.writer_eventbridge as we


def test_write_skips_when_no_event_bus():
    we.STATE["logger"] = logging.getLogger("test")
    we.STATE["event_bus_arn"] = ""  # no bus configured
    ok, err = we.write("topic", {"k": 1})
    assert ok and err is None


def test_write_success():
    we.STATE["logger"] = logging.getLogger("test")
    we.STATE["event_bus_arn"] = "arn:aws:events:region:acct:event-bus/bus"  # fake
    mock_client = MagicMock()
    mock_client.put_events.return_value = {"FailedEntryCount": 0, "Entries": []}
    we.STATE["client"] = mock_client
    ok, err = we.write("topic", {"k": 2})
    assert ok and err is None
    mock_client.put_events.assert_called_once()


def test_write_failed_entries():
    we.STATE["logger"] = logging.getLogger("test")
    we.STATE["event_bus_arn"] = "arn:aws:events:region:acct:event-bus/bus"
    mock_client = MagicMock()
    mock_client.put_events.return_value = {
        "FailedEntryCount": 1,
        "Entries": [
            {"EventId": "1", "ErrorCode": "Err", "ErrorMessage": "Bad"},
            {"EventId": "2"},
        ],
    }
    we.STATE["client"] = mock_client
    ok, err = we.write("topic", {"k": 3})
    assert not ok and "EventBridge" in err


def test_write_client_error():
    from botocore.exceptions import BotoCoreError

    class DummyError(BotoCoreError):
        pass

    we.STATE["logger"] = logging.getLogger("test")
    we.STATE["event_bus_arn"] = "arn:aws:events:region:acct:event-bus/bus"
    mock_client = MagicMock()
    mock_client.put_events.side_effect = DummyError()
    we.STATE["client"] = mock_client
    ok, err = we.write("topic", {"k": 4})
    assert not ok and err is not None
