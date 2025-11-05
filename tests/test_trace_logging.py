import logging
from unittest.mock import MagicMock

from src.logging_levels import TRACE_LEVEL
import src.writer_eventbridge as we
import src.writer_kafka as wk
import src.writer_postgres as wp


def test_trace_eventbridge(caplog):
    logger = logging.getLogger("trace.eventbridge")
    logger.setLevel(TRACE_LEVEL)
    we.STATE["logger"] = logger
    we.STATE["event_bus_arn"] = "arn:aws:events:region:acct:event-bus/test"
    mock_client = MagicMock()
    mock_client.put_events.return_value = {"FailedEntryCount": 0, "Entries": []}
    we.STATE["client"] = mock_client
    caplog.set_level(TRACE_LEVEL)
    ok, err = we.write("topic.eb", {"k": 1})
    assert ok and err is None
    assert any("EventBridge payload" in rec.message for rec in caplog.records)


def test_trace_kafka(caplog):
    class FakeProducer:
        def produce(self, *a, **kw):
            cb = kw.get("callback")
            if cb:
                cb(None, object())

        def flush(self, *a, **kw):
            return 0

    logger = logging.getLogger("trace.kafka")
    logger.setLevel(TRACE_LEVEL)
    wk.STATE["logger"] = logger
    wk.STATE["producer"] = FakeProducer()
    caplog.set_level(TRACE_LEVEL)
    ok, err = wk.write("topic.kf", {"k": 2})
    assert ok and err is None
    assert any("Kafka payload" in rec.message for rec in caplog.records)


def test_trace_postgres(caplog, monkeypatch):
    # Prepare dummy psycopg2 connection machinery
    store = []

    class DummyCursor:
        def execute(self, sql, params):
            store.append((sql, params))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyConnection:
        def cursor(self):
            return DummyCursor()

        def commit(self):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    class DummyPsycopg2:
        def connect(self, **kwargs):
            return DummyConnection()

    monkeypatch.setattr(wp, "psycopg2", DummyPsycopg2())

    logger = logging.getLogger("trace.postgres")
    logger.setLevel(TRACE_LEVEL)
    wp.logger = logger
    wp.POSTGRES = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}

    caplog.set_level(TRACE_LEVEL)
    message = {"event_id": "e", "tenant_id": "t", "source_app": "a", "environment": "dev", "timestamp": 1}
    ok, err = wp.write("public.cps.za.test", message)
    assert ok and err is None
    assert any("Postgres payload" in rec.message for rec in caplog.records)
