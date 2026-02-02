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

from unittest.mock import MagicMock

from src.utils.logging_levels import TRACE_LEVEL
import src.writers.writer_eventbridge as writer_eventbridge
import src.writers.writer_kafka as writer_kafka
import src.writers.writer_postgres as writer_postgres


def test_trace_eventbridge(caplog):
    # Set trace level on the module's logger
    writer_eventbridge.logger.setLevel(TRACE_LEVEL)
    caplog.set_level(TRACE_LEVEL)

    writer = writer_eventbridge.WriterEventBridge({"event_bus_arn": "arn:aws:events:region:acct:event-bus/test"})
    mock_client = MagicMock()
    mock_client.put_events.return_value = {"FailedEntryCount": 0, "Entries": []}
    writer._client = mock_client

    ok, err = writer.write("topic.eb", {"k": 1})

    assert ok and err is None
    assert any("EventBridge payload" in rec.message for rec in caplog.records)


def test_trace_kafka(caplog):
    class FakeProducer:
        def produce(self, *a, **kw):
            cb = kw.get("callback")
            if cb:
                cb(None, object())

        def flush(self, timeout=None):
            return 0

    # Set trace level on the module's logger
    writer_kafka.logger.setLevel(TRACE_LEVEL)
    caplog.set_level(TRACE_LEVEL)

    writer = writer_kafka.WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = FakeProducer()

    ok, err = writer.write("topic.kf", {"k": 2})

    assert ok and err is None
    assert any("Kafka payload" in rec.message for rec in caplog.records)


def test_trace_postgres(caplog, monkeypatch):
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

    monkeypatch.setattr(writer_postgres, "psycopg2", DummyPsycopg2())

    # Set trace level on the module's logger
    writer_postgres.logger.setLevel(TRACE_LEVEL)
    caplog.set_level(TRACE_LEVEL)

    writer = writer_postgres.WriterPostgres({})
    writer._db_config = {"database": "db", "host": "h", "user": "u", "password": "p", "port": 5432}

    message = {"event_id": "e", "tenant_id": "t", "source_app": "a", "environment": "dev", "timestamp": 1}
    ok, err = writer.write("public.cps.za.test", message)

    assert ok and err is None
    assert any("Postgres payload" in rec.message for rec in caplog.records)
