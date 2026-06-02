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

import logging
from types import SimpleNamespace

import pytest

from src.writers.writer import WriteError, HealthCheckError
from src.writers.writer_kafka import WriterKafka
import src.writers.writer_kafka as wk


class FakeProducerSuccess:
    def __init__(self, *a, **kw):
        self.produced = []

    def produce(self, topic, key, value, callback):  # noqa: D401
        self.produced.append((topic, key, value))
        # simulate success
        callback(None, SimpleNamespace())

    def flush(self, timeout=None):
        return 0


class FakeProducerError(FakeProducerSuccess):
    def produce(self, topic, key, value, callback):  # noqa: D401
        # simulate async error
        callback("ERR", None)


class FakeProducerFlushSequence(FakeProducerSuccess):
    def __init__(self, sequence):  # sequence of remaining counts per flush call
        super().__init__()
        self.sequence = sequence
        self.flush_calls = 0

    def flush(self, timeout=None):
        # Simulate decreasing remaining messages
        if self.flush_calls < len(self.sequence):
            val = self.sequence[self.flush_calls]
        else:
            val = self.sequence[-1]
        self.flush_calls += 1
        return val


class FakeProducerTimeout(FakeProducerSuccess):
    def __init__(self, remaining_value):
        super().__init__()
        self.remaining_value = remaining_value
        self.flush_calls = 0

    def flush(self, timeout=None):  # always returns same remaining >0 to force timeout warning
        self.flush_calls += 1
        return self.remaining_value


class FakeProducerTypeError(FakeProducerSuccess):
    def __init__(self):
        super().__init__()
        self.flush_calls = 0

    # Intentionally omit timeout parameter causing TypeError on first attempt inside flush_with_timeout
    def flush(self):  # noqa: D401
        self.flush_calls += 1
        return 0


# --- write() ---


def test_write_skips_when_producer_none():
    writer = WriterKafka({})  # No kafka_bootstrap_server
    writer.write("topic", {"a": 1})


def test_write_success():
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = FakeProducerSuccess()
    writer.write("topic", {"b": 2})


def test_write_uses_message_key():
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    producer = FakeProducerSuccess()
    writer._producer = producer

    writer.write("topic", {"b": 2}, message_key="job-1")

    assert [("topic", "job-1", b'{"b": 2}')] == producer.produced


def test_write_async_error():
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = FakeProducerError()
    with pytest.raises(WriteError, match="ERR"):
        writer.write("topic", {"c": 3})


class DummyKafkaException(Exception):
    pass


def test_write_kafka_exception(monkeypatch):
    class RaisingProducer(FakeProducerSuccess):
        def produce(self, *a, **kw):  # noqa: D401
            raise DummyKafkaException("boom")

    # Monkeypatch KafkaException symbol used in except
    monkeypatch.setattr(wk, "KafkaException", DummyKafkaException, raising=False)
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = RaisingProducer()
    with pytest.raises(WriteError, match="boom"):
        writer.write("topic", {"d": 4})


def test_write_flush_retries_until_success(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    # Force smaller max retries for deterministic sequence length
    monkeypatch.setattr(wk, "_MAX_RETRIES", 5, raising=False)
    producer = FakeProducerFlushSequence([5, 4, 3, 1, 0])
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = producer
    writer.write("topic", {"e": 5})
    # It should break as soon as remaining == 0 (after flush call returning 0)
    assert producer.flush_calls == 5  # sequence consumed until 0
    # Warnings logged for attempts before success (flush_calls -1) because last attempt didn't warn
    warn_messages = [r.message for r in caplog.records if r.levelno == logging.WARNING]
    assert any("attempt 1" in m or "attempt 2" in m for m in warn_messages)


def test_write_timeout_warning_when_remaining_after_retries(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr(wk, "_MAX_RETRIES", 3, raising=False)
    producer = FakeProducerTimeout(2)
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = producer
    writer.write("topic", {"f": 6})
    timeout_warnings = [
        r.message for r in caplog.records if "timeout" in r.message
    ]  # final warning should mention timeout
    assert timeout_warnings, "Expected timeout warning logged"
    assert producer.flush_calls == 3  # retried 3 times


def test_flush_with_timeout_typeerror_fallback(monkeypatch):
    monkeypatch.setattr(wk, "_MAX_RETRIES", 4, raising=False)
    producer = FakeProducerTypeError()
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = producer
    writer.write("topic", {"g": 7})
    # Since flush returns 0 immediately, only one flush call should be needed
    assert producer.flush_calls == 1


# --- check_health() ---


def test_check_health_not_configured():
    writer = WriterKafka({})
    assert "not configured" == writer.check_health()


def test_check_health_success():
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    writer._producer = FakeProducerSuccess()
    writer.check_health()


def test_check_health_producer_init_failed(monkeypatch):
    writer = WriterKafka({"kafka_bootstrap_server": "localhost:9092"})
    # Force _create_producer to return None
    monkeypatch.setattr(writer, "_create_producer", lambda: None)
    with pytest.raises(HealthCheckError, match="initialization failed"):
        writer.check_health()
